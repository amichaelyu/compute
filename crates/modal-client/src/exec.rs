//! Running commands in a Sandbox over its task command router (`execViaTcr` and
//! `ContainerProcess` in the Go SDK).

use std::io;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::task::{Context as TaskContext, Poll, ready};
use std::time::Duration;

use anyhow::{Context, Result, anyhow, bail};
use bytes::{Buf, Bytes};
use futures_util::FutureExt;
use tokio::io::{AsyncRead, AsyncReadExt, ReadBuf};
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tonic::Code;

use crate::client::Client;
use crate::proto::task_command_router as tcr;
use crate::router::{RetryOpts, Router};

/// Linux ARG_MAX headroom the official SDKs enforce on the total length of exec arguments.
const ARG_MAX_BYTES: usize = 1 << 16;
/// Largest stdin chunk per `TaskExecStdinWrite` (the Python SDK buffers up to 16 MiB).
const STDIN_CHUNK_SIZE: usize = 4 * 1024 * 1024;
/// Output chunks buffered between the network and a slow reader.
const STDOUT_CHANNEL_DEPTH: usize = 16;
/// Stderr kept for [`ExecExit::stderr`]; older output is dropped.
const STDERR_LIMIT: usize = 4 * 1024 * 1024;
/// How long to wait for output streams to finish after the process has exited.
const OUTPUT_GRACE: Duration = Duration::from_secs(30);
/// Per-call timeout of `TaskExecWait`, which is retried until the process exits.
const EXEC_WAIT_CALL_TIMEOUT: Duration = Duration::from_secs(60);

/// A command running in a Sandbox, from [`Client::exec`].
///
/// Read its stdout with [`ExecHandle::stdout`] (or move it out with [`ExecHandle::take_stdout`])
/// and call [`ExecHandle::wait`] for the exit code and stderr. Stdout that is still in the
/// handle when `wait` is called is discarded, so the process can't block on a full pipe.
pub struct ExecHandle {
    exec_id: String,
    router: Arc<Router>,
    stdout: ExecStdout,
    stderr: Arc<Mutex<Vec<u8>>>,
    stderr_task: JoinHandle<()>,
    stdin_task: Option<JoinHandle<Result<()>>>,
}

/// How an exec'd command ended, from [`ExecHandle::wait`].
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ExecExit {
    /// Exit code; 128 + N if the process was killed by signal N.
    pub code: i32,
    /// Standard error, decoded lossily as UTF-8 (the last 4 MiB at most).
    pub stderr: String,
}

/// Binary-safe stdout of an exec'd command, as an [`AsyncRead`] that ends at the process's EOF.
pub struct ExecStdout {
    rx: Option<mpsc::Receiver<io::Result<Bytes>>>,
    buf: Bytes,
}

impl Client {
    /// Runs `argv` in the Sandbox and returns a handle to its output and exit status.
    ///
    /// If `stdin` is given it is streamed to the process and stdin is closed at its EOF;
    /// otherwise stdin is closed right away (like `ssh host cmd < /dev/null`), so a command that
    /// reads stdin can't hang. Treat this like an ssh session: run `sh -c` scripts, pipe a tar
    /// archive into `tar -x`, or read one out of `tar -c`.
    pub async fn exec(
        &self,
        sandbox_id: &str,
        argv: &[String],
        stdin: Option<Box<dyn AsyncRead + Send + Unpin>>,
    ) -> Result<ExecHandle> {
        self.exec_inner(sandbox_id, argv, stdin, None).await
    }

    /// Runs `argv` to completion and returns its stdout. Fails with the stderr text if the exit
    /// code is not 0, and after `timeout` (also enforced by Modal, which kills the process).
    pub async fn exec_output(
        &self,
        sandbox_id: &str,
        argv: &[String],
        stdin: Option<Vec<u8>>,
        timeout: Duration,
    ) -> Result<Vec<u8>> {
        let timeout_secs = timeout.as_secs_f64().ceil().clamp(1.0, f64::from(u32::MAX)) as u32;
        let stdin =
            stdin.map(|data| Box::new(io::Cursor::new(data)) as Box<dyn AsyncRead + Send + Unpin>);
        let mut handle = self
            .exec_inner(sandbox_id, argv, stdin, Some(timeout_secs))
            .await?;
        let run = async {
            let mut out = Vec::new();
            handle
                .stdout()
                .read_to_end(&mut out)
                .await
                .context("reading exec stdout")?;
            let exit = handle.wait().await?;
            anyhow::Ok((out, exit))
        };
        let (out, exit) = tokio::time::timeout(timeout + Duration::from_secs(10), run)
            .await
            .map_err(|_| anyhow!("command {argv:?} timed out after {timeout:?}"))??;
        if exit.code != 0 {
            bail!(
                "command {argv:?} exited with code {}: {}",
                exit.code,
                exit.stderr.trim_end()
            );
        }
        Ok(out)
    }

    async fn exec_inner(
        &self,
        sandbox_id: &str,
        argv: &[String],
        stdin: Option<Box<dyn AsyncRead + Send + Unpin>>,
        timeout_secs: Option<u32>,
    ) -> Result<ExecHandle> {
        if argv.is_empty() {
            bail!("exec needs a command");
        }
        let arg_len: usize = argv.iter().map(String::len).sum();
        if arg_len > ARG_MAX_BYTES {
            bail!(
                "total length of command arguments must be less than {ARG_MAX_BYTES} bytes, got {arg_len}"
            );
        }
        let router = self
            .router(sandbox_id)
            .await
            .with_context(|| format!("connecting to sandbox {sandbox_id}"))?;
        let exec_id = uuid::Uuid::new_v4().to_string();
        let req = tcr::TaskExecStartRequest {
            task_id: router.task_id.clone(),
            exec_id: exec_id.clone(),
            command_args: argv.to_vec(),
            stdout_config: tcr::TaskExecStdoutConfig::Pipe as i32,
            stderr_config: tcr::TaskExecStderrConfig::Pipe as i32,
            timeout_secs,
            ..Default::default()
        };
        router
            .unary(req, RetryOpts::default(), |mut c, r| async move {
                c.task_exec_start(r).await
            })
            .await
            .with_context(|| format!("TaskExecStart in sandbox {sandbox_id}"))?;
        tracing::debug!(sandbox_id, exec_id, ?argv, "modal: started exec");

        let (tx, rx) = mpsc::channel(STDOUT_CHANNEL_DEPTH);
        tokio::spawn(router.clone().pump_exec_stdio(
            exec_id.clone(),
            tcr::TaskExecStdioFileDescriptor::Stdout,
            tx,
        ));
        let stdout = ExecStdout {
            rx: Some(rx),
            buf: Bytes::new(),
        };

        let stderr = Arc::new(Mutex::new(Vec::new()));
        let stderr_task = tokio::spawn(collect_stderr(
            router.clone(),
            exec_id.clone(),
            stderr.clone(),
        ));

        let stdin = stdin.unwrap_or_else(|| Box::new(tokio::io::empty()));
        let stdin_task = Some(tokio::spawn(pump_stdin(
            router.clone(),
            exec_id.clone(),
            stdin,
        )));
        Ok(ExecHandle {
            exec_id,
            router,
            stdout,
            stderr,
            stderr_task,
            stdin_task,
        })
    }
}

impl ExecHandle {
    /// The exec ID Modal knows this command by.
    pub fn exec_id(&self) -> &str {
        &self.exec_id
    }

    /// The command's stdout. Empty once moved out with [`ExecHandle::take_stdout`].
    pub fn stdout(&mut self) -> &mut ExecStdout {
        &mut self.stdout
    }

    /// Moves stdout out of the handle, e.g. to read it in another task while waiting.
    pub fn take_stdout(&mut self) -> ExecStdout {
        std::mem::replace(
            &mut self.stdout,
            ExecStdout {
                rx: None,
                buf: Bytes::new(),
            },
        )
    }

    /// Waits for the command to exit and returns its exit code and stderr.
    pub async fn wait(mut self) -> Result<ExecExit> {
        let mut stdout = self.take_stdout();
        let mut drain = tokio::spawn(async move {
            let _ = tokio::io::copy(&mut stdout, &mut tokio::io::sink()).await;
        });

        let req = tcr::TaskExecWaitRequest {
            task_id: self.router.task_id.clone(),
            exec_id: self.exec_id.clone(),
        };
        let opts = RetryOpts {
            base_delay: Duration::from_secs(1),
            factor: 1,
            max_retries: None,
            deadline: None,
            per_call_timeout: Some(EXEC_WAIT_CALL_TIMEOUT),
        };
        let resp = self
            .router
            .unary(
                req,
                opts,
                |mut c, r| async move { c.task_exec_wait(r).await },
            )
            .await;
        let resp = match resp {
            Ok(resp) => resp,
            Err(err) => {
                drain.abort();
                self.stderr_task.abort();
                if let Some(task) = &self.stdin_task {
                    task.abort();
                }
                if err.code() == Code::DeadlineExceeded {
                    bail!("deadline exceeded while waiting for exec {}", self.exec_id);
                }
                return Err(
                    anyhow::Error::new(err).context(format!("TaskExecWait {}", self.exec_id))
                );
            }
        };
        let code = match resp.exit_status {
            Some(tcr::task_exec_wait_response::ExitStatus::Code(code)) => code,
            Some(tcr::task_exec_wait_response::ExitStatus::Signal(signal)) => 128 + signal,
            None => bail!("unexpected exit status for exec {}", self.exec_id),
        };

        // Output streams end shortly after the process; don't wait forever if one doesn't.
        let _ = tokio::time::timeout(OUTPUT_GRACE, async {
            let _ = (&mut drain).await;
            let _ = (&mut self.stderr_task).await;
        })
        .await;
        drain.abort();
        self.stderr_task.abort();
        let stderr = String::from_utf8_lossy(&std::mem::take(
            &mut *self.stderr.lock().expect("stderr lock"),
        ))
        .into_owned();

        if let Some(task) = self.stdin_task.take() {
            // A failed stdin write is not an error of its own: input that never got its EOF
            // leaves the process waiting, so if it exited, it read what it needed (e.g. `tar -x`
            // stops at the end-of-archive marker and exits before the trailing padding).
            if task.is_finished() {
                if let Ok(Err(err)) = task.await {
                    tracing::debug!(exec_id = self.exec_id, code, error = %err, "modal: stdin write failed");
                }
            } else {
                task.abort();
            }
        }
        Ok(ExecExit { code, stderr })
    }
}

impl Drop for ExecHandle {
    fn drop(&mut self) {
        self.stderr_task.abort();
        if let Some(task) = &self.stdin_task {
            task.abort();
        }
    }
}

impl AsyncRead for ExecStdout {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut TaskContext<'_>,
        out: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        loop {
            if !self.buf.is_empty() {
                let n = out.remaining().min(self.buf.len());
                out.put_slice(&self.buf[..n]);
                self.buf.advance(n);
                return Poll::Ready(Ok(()));
            }
            let Some(rx) = self.rx.as_mut() else {
                return Poll::Ready(Ok(()));
            };
            match ready!(rx.poll_recv(cx)) {
                Some(Ok(chunk)) => self.buf = chunk,
                Some(Err(err)) => {
                    self.rx = None;
                    return Poll::Ready(Err(err));
                }
                None => {
                    self.rx = None;
                    return Poll::Ready(Ok(()));
                }
            }
        }
    }
}

/// Collects stderr into `buf`, keeping at most the last [`STDERR_LIMIT`] bytes.
async fn collect_stderr(router: Arc<Router>, exec_id: String, buf: Arc<Mutex<Vec<u8>>>) {
    let (tx, mut rx) = mpsc::channel(STDOUT_CHANNEL_DEPTH);
    tokio::spawn(router.pump_exec_stdio(
        exec_id.clone(),
        tcr::TaskExecStdioFileDescriptor::Stderr,
        tx,
    ));
    while let Some(chunk) = rx.recv().await {
        match chunk {
            Ok(chunk) => {
                let mut buf = buf.lock().expect("stderr lock");
                buf.extend_from_slice(&chunk);
                if buf.len() > STDERR_LIMIT {
                    let excess = buf.len() - STDERR_LIMIT;
                    buf.drain(..excess);
                }
            }
            Err(err) => {
                tracing::debug!(exec_id, error = %err, "modal: reading exec stderr failed");
                return;
            }
        }
    }
}

/// Streams `src` into the process's stdin in offset-addressed chunks, then closes stdin.
async fn pump_stdin(
    router: Arc<Router>,
    exec_id: String,
    mut src: Box<dyn AsyncRead + Send + Unpin>,
) -> Result<()> {
    let mut offset: u64 = 0;
    let mut buf = vec![0u8; STDIN_CHUNK_SIZE];
    loop {
        // Wait for some input, then take whatever else is ready without waiting, so bulk input
        // goes out in large chunks while interactive input is not held back.
        let mut filled = src.read(&mut buf).await.context("reading stdin source")?;
        let mut eof = filled == 0;
        while !eof && filled < buf.len() {
            match src.read(&mut buf[filled..]).now_or_never() {
                Some(Ok(0)) => eof = true,
                Some(Ok(n)) => filled += n,
                Some(Err(err)) => {
                    return Err(anyhow::Error::new(err).context("reading stdin source"));
                }
                None => break,
            }
        }
        let req = tcr::TaskExecStdinWriteRequest {
            task_id: router.task_id.clone(),
            exec_id: exec_id.clone(),
            offset,
            data: buf[..filled].to_vec(),
            eof,
        };
        router
            .unary(req, RetryOpts::default(), |mut c, r| async move {
                c.task_exec_stdin_write(r).await
            })
            .await
            .context("TaskExecStdinWrite")?;
        offset += filled as u64;
        if eof {
            return Ok(());
        }
    }
}
