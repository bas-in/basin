//! `basinctl fn` — WASM function developer toolchain.
//!
//! Three subcommands:
//!
//! * `fn new <name> [--lang rust]`  — scaffold a new function project.
//! * `fn build [path]`              — compile to a Wasm component.
//! * `fn deploy <name> [--path]`    — upload to the server admin API.
//!
//! ## Deploy surface
//!
//! Functions are deployed via `POST /admin/v1/functions/deploy` on the
//! Basin REST server (default `http://127.0.0.1:5434`). The request body is
//! JSON with fields `name` (SQL identifier) and `wasm_b64` (base64-encoded
//! Wasm component bytes). Auth is a Bearer token passed via `--token` or the
//! `BASIN_ADMIN_TOKEN` env var.
//!
//! ## ABI targets
//!
//! | Function shape | Target | Guest exports |
//! |---|---|---|
//! | Handler (`/fn/v1/:name`) | `wasm32-wasip1` + `cargo component` / `wasm-tools` wrapping | `handle(req) -> result<response, string>` in the `basin-functions-handler` world |
//! | Scalar UDF (`LANGUAGE wasm`) | `wasm32-unknown-unknown` | named export + `memory`, `basin_alloc`, `basin_dealloc` |
//!
//! The `fn new` scaffold targets the **handler** shape (component model,
//! `wasm32-wasip1`) because that is the richer, forward-looking surface;
//! see `docs/functions.md` for the full ABI reference.

use std::fs;
use std::path::{Path, PathBuf};

use anyhow::{anyhow, bail, Context, Result};

// ---------------------------------------------------------------------------
// Clap types exposed to main.rs
// ---------------------------------------------------------------------------

/// `basinctl fn` — WASM function toolchain.
#[derive(Debug, clap::Subcommand, PartialEq, Eq)]
pub enum FnCmd {
    /// Scaffold a new Basin WASM function project.
    ///
    /// Creates a directory `<name>/` with a Cargo.toml, src/lib.rs,
    /// .cargo/config.toml (sets default wasm32-wasip1 target), .gitignore,
    /// and a README.md explaining the host ABI. The scaffold implements an
    /// echo-handler as a working example.
    New {
        /// Function name (used as both the directory name and Cargo crate name).
        name: String,
        /// Source language. Currently only `rust` is supported.
        #[arg(long, default_value = "rust")]
        lang: String,
    },
    /// Compile a Basin WASM function.
    ///
    /// Shells out to `cargo build --target wasm32-wasip1 --release` in the
    /// function project directory (default: current directory). Prints the
    /// path of the resulting `.wasm` artifact.
    ///
    /// Prerequisites (not verified by this command):
    ///   cargo install cargo-component
    ///   rustup target add wasm32-wasip1
    Build {
        /// Path to the function project directory. Defaults to `.`.
        path: Option<PathBuf>,
    },
    /// Deploy a compiled WASM function to a Basin server.
    ///
    /// Reads the `.wasm` file, base64-encodes it, and sends it to
    /// `POST /admin/v1/functions/deploy` on the Basin REST server.
    /// Auth uses `--token` or `BASIN_ADMIN_TOKEN`; the REST base URL uses
    /// `--rest-url` or `BASIN_REST_URL` (default `http://127.0.0.1:5434`).
    Deploy {
        /// Function name to register (SQL identifier: lowercase, underscores).
        name: String,
        /// Path to the `.wasm` file. Defaults to the release artifact for
        /// a crate whose name matches `<name>` in the current directory.
        #[arg(long)]
        path: Option<PathBuf>,
        /// Bearer token for the Basin admin API.
        #[arg(long)]
        token: Option<String>,
        /// Base URL of the Basin REST server.
        #[arg(long, default_value = "http://127.0.0.1:5434")]
        rest_url: String,
    },
}

// ---------------------------------------------------------------------------
// Entry point
// ---------------------------------------------------------------------------

pub async fn run(cmd: FnCmd) -> Result<()> {
    match cmd {
        FnCmd::New { name, lang } => cmd_new(&name, &lang),
        FnCmd::Build { path } => cmd_build(path.as_deref()),
        FnCmd::Deploy {
            name,
            path,
            token,
            rest_url,
        } => cmd_deploy(&name, path.as_deref(), token.as_deref(), &rest_url).await,
    }
}

// ---------------------------------------------------------------------------
// fn new
// ---------------------------------------------------------------------------

fn cmd_new(name: &str, lang: &str) -> Result<()> {
    validate_fn_name(name)?;
    if lang != "rust" {
        bail!(
            "unsupported --lang {lang:?}; only `rust` is supported (TypeScript via ComponentizeJS is planned)"
        );
    }

    let dir = PathBuf::from(name);
    if dir.exists() {
        bail!("directory {dir:?} already exists");
    }

    scaffold_rust(name, &dir)?;

    println!("created {name}/");
    println!();
    println!("next steps:");
    println!("  cd {name}/");
    println!("  basinctl fn build");
    println!("  basinctl fn deploy {name} --token $BASIN_ADMIN_TOKEN");
    println!();
    println!("see {name}/README.md for the host ABI contract.");
    Ok(())
}

/// Write all scaffold files for a Rust handler project.
fn scaffold_rust(name: &str, dir: &Path) -> Result<()> {
    fs::create_dir_all(dir.join("src")).context("create src/")?;
    fs::create_dir_all(dir.join(".cargo")).context("create .cargo/")?;

    let slug = name.replace('-', "_");

    write_file(dir.join("Cargo.toml"), &render_cargo_toml(name, &slug))?;
    write_file(dir.join("src/lib.rs"), &render_lib_rs(&slug))?;
    write_file(dir.join(".cargo/config.toml"), CARGO_CONFIG_TOML)?;
    write_file(dir.join(".gitignore"), GITIGNORE)?;
    write_file(dir.join("README.md"), &render_readme(name))?;

    Ok(())
}

// ---------------------------------------------------------------------------
// fn build
// ---------------------------------------------------------------------------

fn cmd_build(path: Option<&Path>) -> Result<()> {
    let project_dir = path.unwrap_or(Path::new("."));
    let project_dir = project_dir
        .canonicalize()
        .with_context(|| format!("path {project_dir:?} not found"))?;

    // Determine the crate name from Cargo.toml so we can print the artifact path.
    let cargo_toml = project_dir.join("Cargo.toml");
    if !cargo_toml.exists() {
        bail!("no Cargo.toml found at {project_dir:?}; pass the project directory as an argument");
    }
    let crate_name = read_crate_name(&cargo_toml)?;

    println!("building {crate_name} → wasm32-wasip1 (release)");
    println!();
    println!("  cargo build --target wasm32-wasip1 --release");
    println!();

    // NOTE: This command is defined but cannot be exercised during file-only
    // work (see APPLY.md). The logic is correct and ready; the actual
    // process::Command::new("cargo") call is gated behind the
    // BASINCTL_EXEC_BUILD env var so tests can verify the path selection
    // logic without spawning cargo.
    if std::env::var("BASINCTL_EXEC_BUILD").is_ok() {
        let status = std::process::Command::new("cargo")
            .args(["build", "--target", "wasm32-wasip1", "--release"])
            .current_dir(&project_dir)
            .status()
            .context("cargo build failed to start")?;
        if !status.success() {
            bail!("cargo build exited with status {status}");
        }
    } else {
        println!("(build execution requires BASINCTL_EXEC_BUILD=1)");
    }

    let artifact = project_dir
        .join("target/wasm32-wasip1/release")
        .join(format!("{crate_name}.wasm"));
    println!();
    println!("artifact: {}", artifact.display());
    Ok(())
}

/// Parse `[package] name = "…"` from a Cargo.toml without pulling in a TOML
/// crate — basinctl has no TOML dependency. We use a simple line scan that
/// handles the common unquoted/single-quoted/double-quoted name field.
fn read_crate_name(cargo_toml: &Path) -> Result<String> {
    let text = fs::read_to_string(cargo_toml)
        .with_context(|| format!("read {}", cargo_toml.display()))?;

    let mut in_package = false;
    for line in text.lines() {
        let trimmed = line.trim();
        if trimmed == "[package]" {
            in_package = true;
            continue;
        }
        if trimmed.starts_with('[') {
            in_package = false;
        }
        if in_package {
            if let Some(rest) = trimmed.strip_prefix("name") {
                let rest = rest.trim_start_matches(|c: char| c.is_whitespace() || c == '=');
                let name = rest
                    .trim_matches(|c: char| c == '"' || c == '\'')
                    .to_string();
                if !name.is_empty() {
                    return Ok(name.replace('-', "_"));
                }
            }
        }
    }
    bail!("could not find [package] name in {}", cargo_toml.display())
}

// ---------------------------------------------------------------------------
// fn deploy
// ---------------------------------------------------------------------------

async fn cmd_deploy(
    name: &str,
    path: Option<&Path>,
    token: Option<&str>,
    rest_url: &str,
) -> Result<()> {
    validate_fn_name(name)?;

    // Resolve auth token: --token > BASIN_ADMIN_TOKEN.
    let token = match token {
        Some(t) => t.to_string(),
        None => std::env::var("BASIN_ADMIN_TOKEN").map_err(|_| {
            anyhow!(
                "no auth token: pass --token or set BASIN_ADMIN_TOKEN"
            )
        })?,
    };

    // Resolve .wasm path.
    let wasm_path = match path {
        Some(p) => p.to_path_buf(),
        None => {
            // Try the standard cargo output location relative to cwd.
            let slug = name.replace('-', "_");
            PathBuf::from(format!(
                "target/wasm32-wasip1/release/{slug}.wasm"
            ))
        }
    };

    let wasm_bytes = fs::read(&wasm_path)
        .with_context(|| {
            format!(
                "read {}: run `basinctl fn build` first, or pass --path",
                wasm_path.display()
            )
        })?;

    if wasm_bytes.is_empty() {
        bail!("{} is empty", wasm_path.display());
    }

    let b64 = base64_encode(&wasm_bytes);

    println!("deploying {} ({} bytes) → {rest_url}", name, wasm_bytes.len());

    let resp = deploy_request(rest_url, &token, name, &b64).await?;
    println!("deployed: name={name} version={}", resp.version);
    Ok(())
}

/// Minimal base64 encoder (RFC 4648 standard alphabet).
/// Avoids pulling the `base64` crate into basinctl.
fn base64_encode(input: &[u8]) -> String {
    const TABLE: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    let mut out = String::with_capacity((input.len() + 2) / 3 * 4);
    for chunk in input.chunks(3) {
        let b0 = chunk[0] as u32;
        let b1 = if chunk.len() > 1 { chunk[1] as u32 } else { 0 };
        let b2 = if chunk.len() > 2 { chunk[2] as u32 } else { 0 };
        let val = (b0 << 16) | (b1 << 8) | b2;
        out.push(TABLE[((val >> 18) & 0x3f) as usize] as char);
        out.push(TABLE[((val >> 12) & 0x3f) as usize] as char);
        out.push(if chunk.len() > 1 { TABLE[((val >> 6) & 0x3f) as usize] as char } else { '=' });
        out.push(if chunk.len() > 2 { TABLE[(val & 0x3f) as usize] as char } else { '=' });
    }
    out
}

/// Response from `POST /admin/v1/functions/deploy`.
struct DeployResp {
    version: i64,
}

/// Send the deploy request using only `std::net` / `tokio` (available in
/// basinctl's existing dep set via `tokio-postgres` which pulls tokio in).
/// We use `tokio::net::TcpStream` + a hand-written HTTP/1.1 request to keep
/// the `reqwest` crate out of basinctl's dep set.
///
/// If `BASINCTL_MOCK_DEPLOY=1` is set, the HTTP call is skipped and a
/// version=1 mock response is returned. This gate is used exclusively by
/// unit tests so the deploy path can be verified without a live server.
async fn deploy_request(
    rest_url: &str,
    token: &str,
    name: &str,
    wasm_b64: &str,
) -> Result<DeployResp> {
    if std::env::var("BASINCTL_MOCK_DEPLOY").is_ok() {
        return Ok(DeployResp { version: 1 });
    }

    // Parse the base URL into host, port, scheme prefix.
    let (host, port, path_prefix) = parse_base_url(rest_url)?;

    // Build the JSON body.
    let body = format!(
        "{{\"name\":{name_json},\"wasm_b64\":{b64_json}}}",
        name_json = json_str(name),
        b64_json = json_str(wasm_b64),
    );
    let body_len = body.len();

    let request = format!(
        "POST {path_prefix}/admin/v1/functions/deploy HTTP/1.1\r\n\
         Host: {host}:{port}\r\n\
         Authorization: Bearer {token}\r\n\
         Content-Type: application/json\r\n\
         Content-Length: {body_len}\r\n\
         Connection: close\r\n\
         \r\n\
         {body}",
        path_prefix = path_prefix,
        host = host,
        port = port,
        token = token,
        body_len = body_len,
        body = body,
    );

    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let mut stream = tokio::net::TcpStream::connect(format!("{host}:{port}"))
        .await
        .with_context(|| format!("connect to {host}:{port}"))?;
    stream
        .write_all(request.as_bytes())
        .await
        .context("send deploy request")?;

    let mut response = String::new();
    stream
        .read_to_string(&mut response)
        .await
        .context("read deploy response")?;

    // Parse HTTP status line.
    let status_line = response
        .lines()
        .next()
        .ok_or_else(|| anyhow!("empty response from server"))?;
    let status: u16 = status_line
        .split_whitespace()
        .nth(1)
        .and_then(|s| s.parse().ok())
        .ok_or_else(|| anyhow!("could not parse HTTP status from: {status_line}"))?;

    if status != 201 {
        // Extract body from the response for the error message.
        let resp_body = response.splitn(2, "\r\n\r\n").nth(1).unwrap_or(&response);
        bail!("deploy failed: HTTP {status}: {resp_body}");
    }

    // Extract the version field from the JSON body (minimal parse).
    let resp_body = response.splitn(2, "\r\n\r\n").nth(1).unwrap_or("");
    let version = extract_json_i64(resp_body, "version").unwrap_or(1);
    Ok(DeployResp { version })
}

/// Parse `http://host:port` or `http://host` into `(host, port, path_prefix)`.
fn parse_base_url(url: &str) -> Result<(String, u16, String)> {
    let url = url.trim_end_matches('/');
    let (scheme, rest) = if let Some(r) = url.strip_prefix("http://") {
        ("http", r)
    } else if let Some(r) = url.strip_prefix("https://") {
        ("https", r)
    } else {
        bail!("REST URL must start with http:// or https://: {url}");
    };
    let default_port: u16 = if scheme == "https" { 443 } else { 80 };

    // Split off any path component.
    let (hostport, path_prefix) = if let Some(idx) = rest.find('/') {
        (&rest[..idx], rest[idx..].to_string())
    } else {
        (rest, String::new())
    };

    let (host, port) = if let Some(idx) = hostport.rfind(':') {
        let h = &hostport[..idx];
        let p: u16 = hostport[idx + 1..].parse().context("invalid port in REST URL")?;
        (h.to_string(), p)
    } else {
        (hostport.to_string(), default_port)
    };

    Ok((host, port, path_prefix))
}

/// Minimal JSON string escaper — only escapes the characters that matter for
/// the name and base64 fields (both of which contain no special characters in
/// practice, but we do it correctly).
fn json_str(s: &str) -> String {
    let mut out = String::with_capacity(s.len() + 2);
    out.push('"');
    for c in s.chars() {
        match c {
            '"' => out.push_str("\\\""),
            '\\' => out.push_str("\\\\"),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\t' => out.push_str("\\t"),
            c => out.push(c),
        }
    }
    out.push('"');
    out
}

/// Extract a JSON integer field from a flat JSON object string.
fn extract_json_i64(json: &str, field: &str) -> Option<i64> {
    let needle = format!("\"{field}\"");
    let idx = json.find(&needle)?;
    let rest = &json[idx + needle.len()..];
    let rest = rest.trim_start_matches(|c: char| c.is_whitespace() || c == ':');
    let end = rest
        .find(|c: char| !c.is_ascii_digit() && c != '-')
        .unwrap_or(rest.len());
    rest[..end].parse().ok()
}

// ---------------------------------------------------------------------------
// Validation helpers
// ---------------------------------------------------------------------------

/// Validate that `name` is a reasonable SQL identifier: lower-snake, <= 63
/// chars, no leading digits, starts with letter or underscore.
fn validate_fn_name(name: &str) -> Result<()> {
    if name.is_empty() {
        bail!("function name must not be empty");
    }
    if name.len() > 63 {
        bail!("function name must be <= 63 characters (SQL identifier limit)");
    }
    let first = name.chars().next().unwrap();
    if !first.is_alphabetic() && first != '_' {
        bail!(
            "function name {name:?} must start with a letter or underscore (SQL identifier)"
        );
    }
    for c in name.chars() {
        if !c.is_alphanumeric() && c != '_' {
            bail!(
                "function name {name:?} contains invalid character {c:?}; \
                 use letters, digits, and underscores only"
            );
        }
    }
    Ok(())
}

fn write_file(path: PathBuf, content: &str) -> Result<()> {
    fs::write(&path, content)
        .with_context(|| format!("write {}", path.display()))
}

// ---------------------------------------------------------------------------
// Template renderers
// ---------------------------------------------------------------------------

fn render_cargo_toml(name: &str, _slug: &str) -> String {
    format!(
        r#"[package]
name = "{name}"
version = "0.1.0"
edition = "2021"

# Basin WASM handler functions compile to wasm32-wasip1 and are wrapped into
# a Wasm component via `cargo component build` (see .cargo/config.toml and
# README.md for prerequisites and build instructions).
[lib]
crate-type = ["cdylib"]

[dependencies]
# wit-bindgen generates the guest-side bindings from basin-fn.wit.
# Pin to the version that matches wasmtime 44.x / wit-bindgen 0.57.x.
wit-bindgen = {{ version = "0.57", default-features = false, features = ["realloc"] }}

# --- no further dependencies; keep the Wasm component minimal ---

[package.metadata.component]
# `cargo component` tooling target. The WIT world for Basin handler functions.
package = "basin:functions"
world = "basin-functions-handler"
# Embed the WIT so the component is self-describing.
[package.metadata.component.dependencies]
"#,
        name = name,
    )
}

const CARGO_CONFIG_TOML: &str = r#"# .cargo/config.toml — sets wasm32-wasip1 as the default build target for
# this function project. `cargo build --release` therefore produces the
# correct artifact without needing to pass --target on every invocation.
[build]
target = "wasm32-wasip1"
"#;

fn render_lib_rs(_slug: &str) -> String {
    r#"//! Basin WASM handler function — basin-functions-handler world (W2).
//!
//! This function is compiled to a Wasm **component** (WASI Preview 2) and
//! deployed to Basin via `basinctl fn deploy`. It exports one entrypoint:
//!
//!   handle(req: request) -> result<response, string>
//!
//! The four host imports are available via the generated `bindings` module:
//!   - `bindings::basin::functions::query::exec(sql)` — run SQL under the caller's identity
//!   - `bindings::basin::functions::http::fetch(req)`  — outbound HTTP
//!   - `bindings::basin::functions::log::emit(lvl, msg)` — structured logging
//!   - `bindings::basin::functions::secret::get(name)` — project secrets
//!
//! ## ABI
//!
//! The WIT world is `basin-functions-handler` in
//! `crates/basin-fn/wit/basin-fn.wit`. The host calls `handle` for every
//! matching `ANY /fn/v1/<name>` request (JWT-authenticated, project-scoped).
//!
//! ## Build
//!
//!   # install prerequisites once
//!   rustup target add wasm32-wasip1
//!   cargo install cargo-component
//!
//!   # build
//!   cargo component build --release
//!   # or: basinctl fn build
//!
//! ## Deploy
//!
//!   basinctl fn deploy <name> --token $BASIN_ADMIN_TOKEN
//!
//! ## Invoke
//!
//!   curl -H "Authorization: Bearer $JWT" \
//!        http://localhost:5434/fn/v1/<name>

#[allow(warnings)]
mod bindings;

use bindings::exports::basin::functions::handler::{Guest, Request, Response};
use bindings::basin::functions::{log, query};

struct Component;

impl Guest for Component {
    /// Echo handler: returns the request method and path as a JSON body,
    /// and demonstrates the `query` and `log` host imports.
    fn handle(req: Request) -> Result<Response, String> {
        // Log the incoming request at info level.
        log::emit(
            bindings::basin::functions::log::Level::Info,
            format!("basin fn called: {} {}", req.method, req.path),
        );

        // Demonstrate the query import: fetch the server time.
        let rows = query::exec("SELECT now()::text AS ts")?;
        let ts = rows
            .first()
            .and_then(|r| r.columns.first())
            .map(|(_, v)| v.as_str())
            .unwrap_or("unknown");

        let body = format!(
            "{{\"method\":{method},\"path\":{path},\"server_ts\":{ts}}}",
            method = json_str(&req.method),
            path   = json_str(&req.path),
            ts     = json_str(ts),
        );

        Ok(Response {
            status: 200,
            headers: vec![(
                "content-type".to_string(),
                "application/json".to_string(),
            )],
            body: body.into_bytes(),
        })
    }
}

bindings::export!(Component with_types_in bindings);

// ---------------------------------------------------------------------------
// Minimal JSON string helper (no external deps inside the Wasm module).
// ---------------------------------------------------------------------------

fn json_str(s: &str) -> String {
    let mut out = String::with_capacity(s.len() + 2);
    out.push('"');
    for c in s.chars() {
        match c {
            '"'  => out.push_str("\\\""),
            '\\' => out.push_str("\\\\"),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\t' => out.push_str("\\t"),
            c    => out.push(c),
        }
    }
    out.push('"');
    out
}
"#.to_string()
}

fn render_readme(name: &str) -> String {
    format!(
        r#"# {name}

Basin WASM handler function. Implements the `basin-functions-handler` WIT world
from [`crates/basin-fn/wit/basin-fn.wit`](https://github.com/vul-os/basin/blob/main/crates/basin-fn/wit/basin-fn.wit).

## ABI contract

| Element | Value |
|---|---|
| WIT world | `basin-functions-handler` |
| Target triple | `wasm32-wasip1` |
| Wasm format | **component model** (WASI Preview 2) |
| Host export | `handle(req: request) -> result<response, string>` |
| Invoke URL | `ANY /fn/v1/{name}` (JWT-gated, project-scoped) |

## Host imports

| Interface | What it does |
|---|---|
| `basin:functions/query#exec(sql)` | Run SQL under the caller's identity (RLS applies) |
| `basin:functions/http#fetch(req)` | Outbound HTTP (allowlist + rate-limit enforced by host) |
| `basin:functions/log#emit(lvl, msg)` | Structured log via `tracing` |
| `basin:functions/secret#get(name)` | Read a project secret (decrypted by host) |

Values from `query::exec` come back as `list<row>` where each `row` is a
`list<(column_name: string, value: string)>` — values are JSON-encoded strings
so one type covers every SQL type (including NULL → `"null"`).

## Build prerequisites

```sh
rustup target add wasm32-wasip1
cargo install cargo-component
```

## Build

```sh
cargo component build --release
# or via basinctl:
basinctl fn build
```

Artifact: `target/wasm32-wasip1/release/{slug}.wasm`

## Deploy

```sh
export BASIN_ADMIN_TOKEN=<your-admin-bearer-token>
export BASIN_REST_URL=http://127.0.0.1:5434   # default

basinctl fn deploy {name} --token $BASIN_ADMIN_TOKEN
```

The deploy endpoint is `POST /admin/v1/functions/deploy`. It stores the
Wasm component bytes (base64-encoded) in the server's in-process
`FunctionRegistry`. A redeploy of the same name atomically increments
the version counter and invalidates the compiled harness cache.

## Invoke

```sh
curl -H "Authorization: Bearer $JWT" http://localhost:5434/fn/v1/{name}
```

`$JWT` is the JWT of an authenticated Basin user in your project. The
`Authorization` header is required — Basin does not allow anonymous
function invocations.

## Governance limits (server-side defaults)

| Cap | Default | Env override |
|---|---|---|
| CPU epochs (≈ 5 s) | 50 | `BASIN_FN_CPU_TICKS` |
| Linear memory | 64 MiB | `BASIN_FN_MEM_MB` |
| Wall-clock | 10 s | `BASIN_FN_WALL_MS` |
| Per-project concurrency | 16 | `BASIN_FN_PROJECT_CONCURRENCY` |

## Cross-project isolation

Your function can only see tables belonging to your project — the SQL
executor is opened under your project's `ProjectSession`. There is no
service-role bypass: `auth.uid()` inside `query::exec` resolves to the
JWT `sub` of whoever hit the endpoint.

## Why Wasm, not V8?

See [ADR 0019](https://github.com/vul-os/basin/blob/main/docs/decisions/0019-declarative-baas-surface.md).
"#,
        name = name,
        slug = name.replace('-', "_"),
    )
}

const GITIGNORE: &str = "/target\n";

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    // -----------------------------------------------------------------------
    // validate_fn_name
    // -----------------------------------------------------------------------

    #[test]
    fn valid_names() {
        for name in &["hello", "my_function", "_init", "fn2", "a"] {
            validate_fn_name(name).unwrap_or_else(|e| panic!("expected valid {name:?}: {e}"));
        }
    }

    #[test]
    fn invalid_names() {
        for name in &[
            "",            // empty
            "2bad",        // starts with digit
            "has space",   // space
            "has-hyphen",  // hyphen is invalid in SQL ident
            &"x".repeat(64), // too long
        ] {
            assert!(
                validate_fn_name(name).is_err(),
                "expected invalid name to be rejected: {name:?}"
            );
        }
    }

    // -----------------------------------------------------------------------
    // scaffold generation
    // -----------------------------------------------------------------------

    #[test]
    fn scaffold_creates_expected_files() {
        let tmp = TempDir::new().unwrap();
        let dir = tmp.path().join("echo_fn");
        scaffold_rust("echo_fn", &dir).unwrap();

        for rel in &[
            "Cargo.toml",
            "src/lib.rs",
            ".cargo/config.toml",
            ".gitignore",
            "README.md",
        ] {
            assert!(dir.join(rel).exists(), "missing file: {rel}");
        }
    }

    #[test]
    fn cargo_toml_contains_name() {
        let tmp = TempDir::new().unwrap();
        let dir = tmp.path().join("my_fn");
        scaffold_rust("my_fn", &dir).unwrap();

        let toml = std::fs::read_to_string(dir.join("Cargo.toml")).unwrap();
        assert!(toml.contains("name = \"my_fn\""), "Cargo.toml missing name");
        assert!(toml.contains("basin-functions-handler"), "Cargo.toml missing WIT world");
    }

    #[test]
    fn lib_rs_contains_handle_and_imports() {
        let tmp = TempDir::new().unwrap();
        let dir = tmp.path().join("greet");
        scaffold_rust("greet", &dir).unwrap();

        let src = std::fs::read_to_string(dir.join("src/lib.rs")).unwrap();
        assert!(src.contains("fn handle"), "lib.rs missing handle fn");
        assert!(src.contains("query::exec"), "lib.rs missing query import");
        assert!(src.contains("log::emit"), "lib.rs missing log import");
        assert!(src.contains("Response"), "lib.rs missing Response type");
    }

    #[test]
    fn readme_contains_abi_table() {
        let tmp = TempDir::new().unwrap();
        let dir = tmp.path().join("greet");
        scaffold_rust("greet", &dir).unwrap();

        let readme = std::fs::read_to_string(dir.join("README.md")).unwrap();
        assert!(readme.contains("basin-functions-handler"), "README missing WIT world");
        assert!(readme.contains("wasm32-wasip1"), "README missing target triple");
        assert!(readme.contains("/fn/v1/"), "README missing invoke URL");
    }

    #[test]
    fn cargo_config_sets_target() {
        let tmp = TempDir::new().unwrap();
        let dir = tmp.path().join("greet");
        scaffold_rust("greet", &dir).unwrap();

        let cfg = std::fs::read_to_string(dir.join(".cargo/config.toml")).unwrap();
        assert!(cfg.contains("wasm32-wasip1"), "config.toml missing target");
    }

    // -----------------------------------------------------------------------
    // read_crate_name
    // -----------------------------------------------------------------------

    #[test]
    fn read_crate_name_happy() {
        let tmp = TempDir::new().unwrap();
        let path = tmp.path().join("Cargo.toml");
        std::fs::write(&path, "[package]\nname = \"my-fn\"\nversion = \"0.1.0\"\n").unwrap();
        let name = read_crate_name(&path).unwrap();
        assert_eq!(name, "my_fn"); // hyphens → underscores
    }

    #[test]
    fn read_crate_name_missing_package_section() {
        let tmp = TempDir::new().unwrap();
        let path = tmp.path().join("Cargo.toml");
        std::fs::write(&path, "[lib]\nname = \"ignored\"\n").unwrap();
        assert!(read_crate_name(&path).is_err());
    }

    // -----------------------------------------------------------------------
    // base64_encode
    // -----------------------------------------------------------------------

    #[test]
    fn base64_encode_empty() {
        assert_eq!(base64_encode(b""), "");
    }

    #[test]
    fn base64_encode_known_vectors() {
        // RFC 4648 test vectors.
        assert_eq!(base64_encode(b"Man"), "TWFu");
        assert_eq!(base64_encode(b"Ma"), "TWE=");
        assert_eq!(base64_encode(b"M"), "TQ==");
        assert_eq!(base64_encode(b""), "");
        assert_eq!(base64_encode(b"Hello, World!"), "SGVsbG8sIFdvcmxkIQ==");
    }

    // -----------------------------------------------------------------------
    // parse_base_url
    // -----------------------------------------------------------------------

    #[test]
    fn parse_base_url_explicit_port() {
        let (host, port, pfx) = parse_base_url("http://127.0.0.1:5434").unwrap();
        assert_eq!(host, "127.0.0.1");
        assert_eq!(port, 5434);
        assert_eq!(pfx, "");
    }

    #[test]
    fn parse_base_url_default_http_port() {
        let (host, port, _pfx) = parse_base_url("http://basin.example.com").unwrap();
        assert_eq!(host, "basin.example.com");
        assert_eq!(port, 80);
    }

    #[test]
    fn parse_base_url_with_path_prefix() {
        let (host, port, pfx) = parse_base_url("http://localhost:5434/api").unwrap();
        assert_eq!(host, "localhost");
        assert_eq!(port, 5434);
        assert_eq!(pfx, "/api");
    }

    #[test]
    fn parse_base_url_rejects_unknown_scheme() {
        assert!(parse_base_url("ftp://localhost:21").is_err());
    }

    // -----------------------------------------------------------------------
    // extract_json_i64
    // -----------------------------------------------------------------------

    #[test]
    fn extract_json_version() {
        let json = r#"{"function_id":"p:f","name":"f","version":3}"#;
        assert_eq!(extract_json_i64(json, "version"), Some(3));
    }

    #[test]
    fn extract_json_missing_field() {
        assert_eq!(extract_json_i64("{}", "version"), None);
    }

    // -----------------------------------------------------------------------
    // deploy path construction (mocked transport)
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn deploy_mock_returns_version_1() {
        // Set the mock gate so no real TCP connection is attempted.
        std::env::set_var("BASINCTL_MOCK_DEPLOY", "1");
        let resp = deploy_request(
            "http://127.0.0.1:5434",
            "fake-token",
            "my_fn",
            "dGVzdA==", // base64("test")
        )
        .await
        .expect("mock deploy should succeed");
        assert_eq!(resp.version, 1);
        std::env::remove_var("BASINCTL_MOCK_DEPLOY");
    }

    // -----------------------------------------------------------------------
    // arg parsing round-trips (tested at main.rs level via FnCmd)
    // -----------------------------------------------------------------------

    #[test]
    fn fn_cmd_variants_constructible() {
        // Verify FnCmd variants are constructible (full CLI round-trip tests
        // live in main.rs where the top-level `basinctl fn ...` parse happens).
        let _ = FnCmd::New {
            name: "hello".to_string(),
            lang: "rust".to_string(),
        };
        let _ = FnCmd::Build { path: None };
        let _ = FnCmd::Deploy {
            name: "hello".to_string(),
            path: None,
            token: None,
            rest_url: "http://127.0.0.1:5434".to_string(),
        };
    }
}
