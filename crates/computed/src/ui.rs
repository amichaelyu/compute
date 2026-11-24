//! Serves the embedded dashboard. The pages themselves are public; every bit of data they show
//! comes from the `/v1/` API, which needs a session (SSH key, or Cloudflare Access in a browser).

use axum::http::{HeaderValue, StatusCode, Uri, header};
use axum::response::{IntoResponse, Response};
use serde_json::json;

include!(concat!(env!("OUT_DIR"), "/ui_assets.rs"));

const PLACEHOLDER: &str = "<!doctype html><meta charset=utf-8><title>compute</title>\
<p style=\"font:16px system-ui;margin:3rem\">The dashboard isn't built into this binary. \
Run <code>bun install &amp;&amp; bun run build</code> in <code>web/</code>, then rebuild <code>computed</code>.</p>";

fn content_type(path: &str) -> &'static str {
    match path.rsplit('.').next().unwrap_or("") {
        "html" => "text/html; charset=utf-8",
        "js" | "mjs" => "text/javascript; charset=utf-8",
        "css" => "text/css; charset=utf-8",
        "woff2" => "font/woff2",
        "woff" => "font/woff",
        "svg" => "image/svg+xml",
        "png" => "image/png",
        "ico" => "image/x-icon",
        "json" | "map" => "application/json",
        _ => "application/octet-stream",
    }
}

fn find(path: &str) -> Option<&'static [u8]> {
    ASSETS.iter().find(|(p, _)| *p == path).map(|(_, d)| *d)
}

pub async fn serve(uri: Uri) -> Response {
    let path = uri.path().trim_start_matches('/');
    if path == "v1" || path.starts_with("v1/") {
        let body = json!({"error": {"code": "not_found", "message": format!("no API route {}", uri.path()), "hint": "see GET /v1/ and the compute skill"}});
        return (StatusCode::NOT_FOUND, axum::Json(body)).into_response();
    }
    if ASSETS.is_empty() {
        return ([(header::CONTENT_TYPE, "text/html; charset=utf-8")], PLACEHOLDER).into_response();
    }
    let path = if path.is_empty() { "index.html" } else { path };
    let (path, data) = match find(path) {
        Some(d) => (path, d),
        // Unknown non-file paths get the app; it routes with the URL hash.
        None if !path.contains('.') => ("index.html", find("index.html").unwrap_or_default()),
        None => return (StatusCode::NOT_FOUND, "not found").into_response(),
    };
    let cache = if path == "index.html" { "no-cache" } else { "public, max-age=31536000, immutable" };
    let mut resp = data.into_response();
    let h = resp.headers_mut();
    h.insert(header::CONTENT_TYPE, HeaderValue::from_static(content_type(path)));
    h.insert(header::CACHE_CONTROL, HeaderValue::from_static(cache));
    h.insert(header::X_CONTENT_TYPE_OPTIONS, HeaderValue::from_static("nosniff"));
    h.insert("referrer-policy", HeaderValue::from_static("no-referrer"));
    h.insert(
        header::CONTENT_SECURITY_POLICY,
        HeaderValue::from_static("default-src 'self'; img-src 'self' data:; style-src 'self' 'unsafe-inline'; frame-ancestors 'none'"),
    );
    resp
}
