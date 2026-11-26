# Sets this Windows machine up to use the compute network at {{URL}}: installs the `compute`
# CLI, signs in with your SSH key and installs the compute skill for your agents. Run it again
# any time; the CLI also keeps itself and the skill up to date.
#
#   irm {{URL}}/v1/install.ps1 | iex
#
# COMPUTE_KEY picks the key (default ~\.ssh\id_ed25519.pub), COMPUTE_BIN_DIR where the CLI goes
# (default %LOCALAPPDATA%\compute\bin). Signing in needs ssh-keygen (Windows' OpenSSH client).
$ErrorActionPreference = 'Stop'
$url = '{{URL}}'

if (@('AMD64', 'ARM64') -notcontains $env:PROCESSOR_ARCHITECTURE) {
    throw "compute: there's no compute build for $env:PROCESSOR_ARCHITECTURE"
}
if (-not (Get-Command ssh-keygen -ErrorAction SilentlyContinue)) {
    throw "compute: needs ssh-keygen. Install Windows' OpenSSH client (Settings > Optional features) and run this again."
}

$binDir = if ($env:COMPUTE_BIN_DIR) { $env:COMPUTE_BIN_DIR } else { Join-Path $env:LOCALAPPDATA 'compute\bin' }
New-Item -ItemType Directory -Force -Path $binDir | Out-Null
$exe = Join-Path $binDir 'compute.exe'
$tmp = Join-Path $binDir 'compute.download.exe'
Invoke-WebRequest -UseBasicParsing -Uri "$url/v1/cli/windows-x86_64" -OutFile $tmp
# A running compute.exe can't be overwritten, but it can be moved aside.
if (Test-Path $exe) {
    $old = Join-Path $binDir 'compute.old.exe'
    Remove-Item -Force $old -ErrorAction SilentlyContinue
    Move-Item -Force $exe $old
}
Move-Item -Force $tmp $exe
Write-Host "compute: installed $(& $exe --version) at $exe"

$userPath = [Environment]::GetEnvironmentVariable('Path', 'User')
if (-not (($userPath -split ';') -contains $binDir)) {
    $newPath = if ($userPath) { "$userPath;$binDir" } else { $binDir }
    [Environment]::SetEnvironmentVariable('Path', $newPath, 'User')
    Write-Host "compute: added $binDir to your PATH (open a new terminal for it to apply)"
}
$env:Path = "$env:Path;$binDir"

& $exe setup $url
if ($LASTEXITCODE -ne 0) { throw "compute: setup failed" }
