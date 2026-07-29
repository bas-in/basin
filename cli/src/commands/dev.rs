//! dev — local basin-server stack for development.
//!
//! `basin dev` starts a local engine instance so developers get the same DX
//! as `supabase start` without needing the cloud.  It works in two modes:
//!
//! ## Binary mode (default, no Docker required)
//!
//!   Looks for `basin-server` on PATH or the path supplied by --server-bin /
//!   BASIN_SERVER_BIN.  Spawns it with ephemeral local-filesystem storage,
//!   waits for the pgwire port to accept connections, then prints the DSN and
//!   blocks until Ctrl-C.
//!
//!   Prerequisites:
//!   - `basin-server` binary built from the engine repo
//!     (`cargo build --release -p basin-server` in `bas-in/basin`)
//!     OR the path set via BASIN_SERVER_BIN / --server-bin.
//!
//! ## Docker mode (--docker)
//!
//!   Runs `docker compose up` against the `dev/docker-compose.yml` in the
//!   engine repo (or a path set via BASIN_COMPOSE_FILE / --compose-file).
//!   Useful when you want catalog-pg + MinIO + basin-server in one command.
//!   Requires Docker ≥ 24 with Compose v2 bundled.
//!
//! ## Connection string after start
//!
//!   postgres://dev@127.0.0.1:<port>/dev
//!
//! ## Flags
//!
//!   --port=5432          pgwire listen port (default 5432)
//!   --data-dir=<path>    data directory (default: a temp dir under /tmp/basin-dev)
//!   --server-bin=<path>  path to basin-server binary (default: search PATH)
//!   --docker             use Docker Compose instead of a local binary
//!   --compose-file=<p>   path to docker-compose.yml (default: BASIN_COMPOSE_FILE
//!                        or ../basin/dev/docker-compose.yml)
//!   --apply-migrations   apply ./basin/migrations/*.sql after start
//!   --seed=<path>        SQL seed file to run after migrations (default: ./basin/seed.sql)
//!   --timeout=60         seconds to wait for the engine to become ready (default 60)
//!
//! ## `basin stop`
//!
//!   In Docker mode `basin stop` calls `docker compose down`.
//!   In binary mode `basin dev` keeps the process in the foreground; Ctrl-C
//!   terminates the child process.

use std::io::Write;
use std::net::TcpStream;
use std::path::{Path, PathBuf};
use std::process::{Child, Command as SysCommand, Stdio};
use std::time::{Duration, Instant};

use clap::{Arg, ArgAction, Command};
use serde_json::json;

use crate::error::{msg, CliResult};
use crate::global::GlobalFlags;
use crate::output::print_json;

use super::help::help_for_command;
use super::parse_or_silent;

// ── Public entry points ───────────────────────────────────────────────────────

pub fn cmd_dev(g: &GlobalFlags, args: &[String]) -> CliResult<()> {
    let cmd = Command::new("dev")
        .arg(Arg::new("port").long("port").default_value("5432"))
        .arg(Arg::new("data-dir").long("data-dir"))
        .arg(Arg::new("server-bin").long("server-bin"))
        .arg(Arg::new("docker").long("docker").action(ArgAction::SetTrue))
        .arg(Arg::new("compose-file").long("compose-file"))
        .arg(
            Arg::new("apply-migrations")
                .long("apply-migrations")
                .action(ArgAction::SetTrue),
        )
        .arg(Arg::new("seed").long("seed"))
        .arg(Arg::new("timeout").long("timeout").default_value("60"))
        .arg(Arg::new("help").long("help").action(ArgAction::SetTrue));

    let m = parse_or_silent(cmd, args)?;
    if m.get_flag("help") {
        dev_usage();
        return Ok(());
    }

    let port: u16 = m
        .get_one::<String>("port")
        .cloned()
        .unwrap_or_else(|| "5432".into())
        .parse()
        .map_err(|_| msg("--port must be a valid port number (1–65535)"))?;

    let timeout_secs: u64 = m
        .get_one::<String>("timeout")
        .cloned()
        .unwrap_or_else(|| "60".into())
        .parse()
        .map_err(|_| msg("--timeout must be a positive integer (seconds)"))?;

    let use_docker = m.get_flag("docker");
    let apply_migrations = m.get_flag("apply-migrations");

    let seed_path: Option<PathBuf> = m
        .get_one::<String>("seed")
        .map(PathBuf::from)
        .or_else(|| {
            let p = PathBuf::from("./basin/seed.sql");
            if p.exists() { Some(p) } else { None }
        });

    if use_docker {
        let compose_file = resolve_compose_file(m.get_one::<String>("compose-file"))?;
        run_docker_mode(g, &compose_file, port, timeout_secs, apply_migrations, seed_path)
    } else {
        let server_bin = resolve_server_bin(m.get_one::<String>("server-bin"))?;
        let data_dir = resolve_data_dir(m.get_one::<String>("data-dir"))?;
        run_binary_mode(g, &server_bin, &data_dir, port, timeout_secs, apply_migrations, seed_path)
    }
}

pub fn cmd_stop(g: &GlobalFlags, args: &[String]) -> CliResult<()> {
    let cmd = Command::new("stop")
        .arg(Arg::new("compose-file").long("compose-file"))
        .arg(Arg::new("help").long("help").action(ArgAction::SetTrue));

    let m = parse_or_silent(cmd, args)?;
    if m.get_flag("help") {
        help_for_command(
            "stop",
            "Stop the local basin-server stack (Docker Compose mode only).",
            &[
                "--compose-file=<p>  Path to docker-compose.yml.",
                "",
                "In binary mode, stop the process by pressing Ctrl-C in the `basin dev` terminal.",
            ],
        );
        return Ok(());
    }

    let compose_file = resolve_compose_file(m.get_one::<String>("compose-file"))?;
    stop_docker(&compose_file, g)
}

// ── Path resolution ───────────────────────────────────────────────────────────

/// resolve_server_bin returns the path to `basin-server`.
/// Priority: --server-bin flag > BASIN_SERVER_BIN env > which basin-server.
fn resolve_server_bin(flag: Option<&String>) -> CliResult<PathBuf> {
    if let Some(p) = flag {
        let path = PathBuf::from(p);
        if path.is_file() {
            return Ok(path);
        }
        return Err(msg(format!(
            "basin-server binary not found at {:?} (from --server-bin)",
            path
        )));
    }
    if let Ok(env_path) = std::env::var("BASIN_SERVER_BIN") {
        if !env_path.is_empty() {
            let path = PathBuf::from(&env_path);
            if path.is_file() {
                return Ok(path);
            }
            return Err(msg(format!(
                "basin-server binary not found at {:?} (from BASIN_SERVER_BIN)",
                path
            )));
        }
    }
    // Search PATH.
    which_bin("basin-server").ok_or_else(|| msg(
        "basin-server not found on PATH.\n\
         \n\
         To install:\n\
           • Build from source:  cargo build --release -p basin-server  (in the engine repo)\n\
             then add target/release/ to your PATH, or set BASIN_SERVER_BIN=/path/to/basin-server\n\
           • Or use --docker to run via Docker Compose (requires Docker ≥ 24).\n\
         \n\
         Engine repo: https://github.com/bas-in/basin"
    ))
}

/// resolve_compose_file returns the docker-compose.yml path.
/// Priority: --compose-file flag > BASIN_COMPOSE_FILE env > ../basin/dev/docker-compose.yml.
fn resolve_compose_file(flag: Option<&String>) -> CliResult<PathBuf> {
    if let Some(p) = flag {
        let path = PathBuf::from(p);
        if path.is_file() {
            return Ok(path);
        }
        return Err(msg(format!(
            "docker-compose.yml not found at {:?} (from --compose-file)",
            path
        )));
    }
    if let Ok(env_path) = std::env::var("BASIN_COMPOSE_FILE") {
        if !env_path.is_empty() {
            let path = PathBuf::from(&env_path);
            if path.is_file() {
                return Ok(path);
            }
            return Err(msg(format!(
                "docker-compose.yml not found at {:?} (from BASIN_COMPOSE_FILE)",
                path
            )));
        }
    }
    // Convention: the engine repo is a sibling directory.
    let candidates = [
        PathBuf::from("../basin/dev/docker-compose.yml"),
        PathBuf::from("./dev/docker-compose.yml"),
    ];
    for p in &candidates {
        if p.is_file() {
            return Ok(p.clone());
        }
    }
    Err(msg(
        "docker-compose.yml not found.\n\
         \n\
         Provide one of:\n\
           --compose-file=<path>         explicit path\n\
           BASIN_COMPOSE_FILE=<path>     environment variable\n\
           ../basin/dev/docker-compose.yml  (engine repo sibling checkout)\n\
         \n\
         Engine repo: https://github.com/bas-in/basin"
    ))
}

/// resolve_data_dir returns the data directory for binary mode.
/// Priority: --data-dir flag > a stable temp dir at /tmp/basin-dev.
fn resolve_data_dir(flag: Option<&String>) -> CliResult<PathBuf> {
    if let Some(p) = flag {
        return Ok(PathBuf::from(p));
    }
    Ok(std::env::temp_dir().join("basin-dev"))
}

/// which_bin searches PATH for a binary name, returning None if not found.
fn which_bin(name: &str) -> Option<PathBuf> {
    let path_var = std::env::var("PATH").unwrap_or_default();
    for dir in std::env::split_paths(&path_var) {
        let candidate = dir.join(name);
        if candidate.is_file() {
            return Some(candidate);
        }
    }
    None
}

// ── Binary mode ───────────────────────────────────────────────────────────────

fn run_binary_mode(
    g: &GlobalFlags,
    server_bin: &Path,
    data_dir: &Path,
    port: u16,
    timeout_secs: u64,
    apply_migrations: bool,
    seed_path: Option<PathBuf>,
) -> CliResult<()> {
    // Prepare data + WAL directories.
    let wal_dir = data_dir.join("wal");
    std::fs::create_dir_all(data_dir)
        .map_err(|e| msg(format!("create data dir {:?}: {e}", data_dir)))?;
    std::fs::create_dir_all(&wal_dir)
        .map_err(|e| msg(format!("create wal dir {:?}: {e}", wal_dir)))?;

    if !g.quiet {
        eprintln!("basin dev: starting {} (port {})", server_bin.display(), port);
    }

    // Spawn basin-server with local-filesystem storage and a single dev project.
    let mut child: Child = SysCommand::new(server_bin)
        .env("BASIN_STORAGE_BACKEND", "local")
        .env("BASIN_DATA_DIR", data_dir.as_os_str())
        .env("BASIN_WAL_DIR", wal_dir.as_os_str())
        .env("BASIN_PROJECTS", "dev=*")
        .env("BASIN_PGWIRE_PORT", port.to_string())
        .env("BASIN_LOG_LEVEL", "warn")
        .stdout(Stdio::inherit())
        .stderr(Stdio::inherit())
        .spawn()
        .map_err(|e| msg(format!("failed to spawn {}: {e}", server_bin.display())))?;

    // Wait for readiness.
    let dsn = format!("postgres://dev@127.0.0.1:{}/dev", port);
    match wait_for_pgwire(port, timeout_secs) {
        Ok(()) => {}
        Err(e) => {
            let _ = child.kill();
            return Err(e);
        }
    }

    if !g.quiet {
        eprintln!("basin dev: ready");
    }

    // Apply migrations if requested.
    if apply_migrations {
        if let Err(e) = apply_local_migrations(&dsn, g) {
            if !g.quiet {
                eprintln!("basin dev: migrations warning: {e}");
            }
        }
    }

    // Run seed if available.
    if let Some(ref seed) = seed_path {
        if seed.is_file() {
            if let Err(e) = run_seed_file(seed, &dsn, g) {
                if !g.quiet {
                    eprintln!("basin dev: seed warning: {e}");
                }
            }
        }
    }

    // Print the connection string.
    if g.json {
        let mut out = Vec::<u8>::new();
        let _ = print_json(
            &mut out,
            &json!({
                "dsn": dsn,
                "port": port,
                "data_dir": data_dir.display().to_string(),
                "mode": "binary",
            }),
        );
        let _ = std::io::stdout().write_all(&out);
    } else {
        println!("\nConnection string:\n  {dsn}\n");
        println!("Press Ctrl-C to stop the local engine.\n");
    }

    // Block until child exits or Ctrl-C.
    match child.wait() {
        Ok(status) => {
            if !status.success() {
                return Err(msg(format!(
                    "basin-server exited with status: {status}"
                )));
            }
        }
        Err(e) => {
            return Err(msg(format!("basin-server wait failed: {e}")));
        }
    }
    Ok(())
}

// ── Docker mode ───────────────────────────────────────────────────────────────

fn run_docker_mode(
    g: &GlobalFlags,
    compose_file: &Path,
    port: u16,
    timeout_secs: u64,
    apply_migrations: bool,
    seed_path: Option<PathBuf>,
) -> CliResult<()> {
    // Verify docker is present.
    ensure_docker()?;

    if !g.quiet {
        eprintln!("basin dev: docker compose up -d ({})", compose_file.display());
    }

    let status = SysCommand::new("docker")
        .args([
            "compose",
            "-f",
            compose_file.to_str().unwrap_or("docker-compose.yml"),
            "up",
            "-d",
            "--remove-orphans",
        ])
        .status()
        .map_err(|e| msg(format!("docker compose up: {e}")))?;

    if !status.success() {
        return Err(msg(format!("docker compose up failed: {status}")));
    }

    // Wait for pgwire readiness.
    wait_for_pgwire(port, timeout_secs)?;

    let dsn = format!("postgres://dev@127.0.0.1:{}/dev", port);

    if apply_migrations {
        if let Err(e) = apply_local_migrations(&dsn, g) {
            if !g.quiet {
                eprintln!("basin dev: migrations warning: {e}");
            }
        }
    }

    if let Some(ref seed) = seed_path {
        if seed.is_file() {
            if let Err(e) = run_seed_file(seed, &dsn, g) {
                if !g.quiet {
                    eprintln!("basin dev: seed warning: {e}");
                }
            }
        }
    }

    if g.json {
        let mut out = Vec::<u8>::new();
        let _ = print_json(
            &mut out,
            &json!({
                "dsn": dsn,
                "port": port,
                "compose_file": compose_file.display().to_string(),
                "mode": "docker",
            }),
        );
        let _ = std::io::stdout().write_all(&out);
    } else {
        println!("\nConnection string:\n  {dsn}\n");
        println!("Run `basin stop` to shut down the stack.\n");
    }

    Ok(())
}

fn stop_docker(compose_file: &Path, g: &GlobalFlags) -> CliResult<()> {
    ensure_docker()?;

    if !g.quiet {
        eprintln!("basin stop: docker compose down ({})", compose_file.display());
    }

    let status = SysCommand::new("docker")
        .args([
            "compose",
            "-f",
            compose_file.to_str().unwrap_or("docker-compose.yml"),
            "down",
        ])
        .status()
        .map_err(|e| msg(format!("docker compose down: {e}")))?;

    if !status.success() {
        return Err(msg(format!("docker compose down failed: {status}")));
    }

    if !g.quiet {
        println!("basin stop: stack stopped.");
    }
    Ok(())
}

fn ensure_docker() -> CliResult<()> {
    let out = SysCommand::new("docker")
        .arg("info")
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status();
    match out {
        Ok(s) if s.success() => Ok(()),
        _ => Err(msg(
            "Docker is not running or not installed.\n\
             \n\
             Install Docker ≥ 24 from https://docs.docker.com/get-docker/ and start Docker Desktop,\n\
             or omit --docker to use the local binary mode (requires basin-server on PATH)."
        )),
    }
}

// ── Readiness polling ─────────────────────────────────────────────────────────

/// wait_for_pgwire polls 127.0.0.1:<port> via TCP until it accepts a connection
/// or the timeout elapses. Returns Ok on success, Err on timeout.
pub fn wait_for_pgwire(port: u16, timeout_secs: u64) -> CliResult<()> {
    let addr = format!("127.0.0.1:{port}");
    let deadline = Instant::now() + Duration::from_secs(timeout_secs);
    while Instant::now() < deadline {
        if TcpStream::connect_timeout(
            &addr.parse().unwrap(),
            Duration::from_millis(200),
        )
        .is_ok()
        {
            return Ok(());
        }
        std::thread::sleep(Duration::from_millis(300));
    }
    Err(msg(format!(
        "basin-server did not become ready on port {port} within {timeout_secs}s.\n\
         Check the server logs for startup errors."
    )))
}

// ── Post-start operations ─────────────────────────────────────────────────────

/// apply_local_migrations applies every .sql file under ./basin/migrations/
/// in lexicographic order via `psql` (or the basin sql command).
/// We use psql here because it avoids importing the engine's SQL parser
/// directly; psql is expected to be available when developing locally.
fn apply_local_migrations(dsn: &str, g: &GlobalFlags) -> CliResult<()> {
    let mig_dir = PathBuf::from("./basin/migrations");
    if !mig_dir.is_dir() {
        if !g.quiet {
            eprintln!("basin dev: no ./basin/migrations/ directory found; skipping migrations");
        }
        return Ok(());
    }

    let mut files: Vec<PathBuf> = std::fs::read_dir(&mig_dir)
        .map_err(|e| msg(format!("read migrations dir: {e}")))?
        .flatten()
        .map(|e| e.path())
        .filter(|p| p.extension().and_then(|x| x.to_str()) == Some("sql"))
        .collect();
    files.sort();

    if files.is_empty() {
        if !g.quiet {
            eprintln!("basin dev: no .sql files in ./basin/migrations/");
        }
        return Ok(());
    }

    // Use psql if available; otherwise fall through gracefully.
    if which_bin("psql").is_none() {
        if !g.quiet {
            eprintln!("basin dev: psql not found on PATH; skipping --apply-migrations.\n\
                       Install postgresql-client or connect manually with:\n  psql {dsn}");
        }
        return Ok(());
    }

    for file in &files {
        if !g.quiet {
            eprintln!("basin dev: applying {}", file.display());
        }
        let status = SysCommand::new("psql")
            .args([
                dsn,
                "-f",
                file.to_str().unwrap_or(""),
                "--single-transaction",
                "-v",
                "ON_ERROR_STOP=1",
            ])
            .stdout(Stdio::inherit())
            .stderr(Stdio::inherit())
            .status()
            .map_err(|e| msg(format!("psql {}: {e}", file.display())))?;
        if !status.success() {
            return Err(msg(format!(
                "migration {} failed (exit {}); the stack is still running.",
                file.display(),
                status
            )));
        }
    }
    Ok(())
}

/// run_seed_file applies a seed SQL file via psql.
fn run_seed_file(seed: &Path, dsn: &str, g: &GlobalFlags) -> CliResult<()> {
    if which_bin("psql").is_none() {
        if !g.quiet {
            eprintln!("basin dev: psql not found on PATH; skipping seed file {}", seed.display());
        }
        return Ok(());
    }
    if !g.quiet {
        eprintln!("basin dev: applying seed {}", seed.display());
    }
    let status = SysCommand::new("psql")
        .args([
            dsn,
            "-f",
            seed.to_str().unwrap_or(""),
        ])
        .stdout(Stdio::inherit())
        .stderr(Stdio::inherit())
        .status()
        .map_err(|e| msg(format!("psql seed {}: {e}", seed.display())))?;
    if !status.success() {
        return Err(msg(format!(
            "seed {} failed (exit {})",
            seed.display(),
            status
        )));
    }
    Ok(())
}

// ── Help ──────────────────────────────────────────────────────────────────────

fn dev_usage() {
    help_for_command(
        "dev",
        "Start a local basin-server instance for development.",
        &[
            "--port=5432           pgwire listen port (default 5432).",
            "--data-dir=<path>     data directory (default: /tmp/basin-dev).",
            "--server-bin=<path>   path to basin-server binary (default: search PATH).",
            "                      Also: BASIN_SERVER_BIN env var.",
            "--docker              use Docker Compose instead of a local binary.",
            "--compose-file=<p>    path to docker-compose.yml.",
            "                      Also: BASIN_COMPOSE_FILE env var.",
            "--apply-migrations    apply ./basin/migrations/*.sql after start.",
            "--seed=<path>         SQL seed file (default: ./basin/seed.sql if present).",
            "--timeout=60          seconds to wait for readiness (default 60).",
            "",
            "Prerequisites (binary mode):",
            "  basin-server binary on PATH or set via --server-bin / BASIN_SERVER_BIN.",
            "  Build it with: cargo build --release -p basin-server",
            "  Engine repo: https://github.com/bas-in/basin",
            "",
            "Prerequisites (Docker mode, --docker):",
            "  Docker ≥ 24 with Compose v2 bundled, and the engine repo checked out",
            "  as a sibling directory (../basin/dev/docker-compose.yml).",
        ],
    );
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use crate::testutil::with_temp_config_dir;

    #[test]
    fn wait_for_pgwire_times_out_immediately_on_closed_port() {
        // Port 1 is almost certainly closed on any dev machine.
        // Use a 1-second timeout so the test doesn't drag.
        let err = wait_for_pgwire(1, 1).unwrap_err();
        assert!(
            err.to_string().contains("did not become ready"),
            "expected timeout error, got: {err}"
        );
    }

    #[test]
    fn which_bin_finds_existing() {
        // sh is always present on any Unix-like system.
        assert!(
            which_bin("sh").is_some(),
            "expected to find 'sh' on PATH"
        );
    }

    #[test]
    fn which_bin_returns_none_for_bogus() {
        assert!(which_bin("this-binary-definitely-does-not-exist-basin-dev").is_none());
    }

    #[test]
    fn resolve_server_bin_missing_env() {
        let _cfg = with_temp_config_dir();
        // BASIN_SERVER_BIN pointing at a non-existent path should error.
        std::env::set_var("BASIN_SERVER_BIN", "/no/such/basin-server");
        let result = resolve_server_bin(None);
        std::env::remove_var("BASIN_SERVER_BIN");
        assert!(result.is_err(), "expected error for missing binary");
    }

    #[test]
    fn resolve_data_dir_default() {
        let dir = resolve_data_dir(None).unwrap();
        assert!(
            dir.ends_with("basin-dev"),
            "default data dir should end in basin-dev, got {:?}",
            dir
        );
    }

    #[test]
    fn resolve_data_dir_flag() {
        let dir = resolve_data_dir(Some(&"/tmp/my-basin".to_string())).unwrap();
        assert_eq!(dir, PathBuf::from("/tmp/my-basin"));
    }

    #[test]
    fn cmd_dev_help_flag() {
        let _cfg = with_temp_config_dir();
        let g = GlobalFlags::default();
        // `basin dev --help` must not error.
        cmd_dev(&g, &["--help".into()]).unwrap();
    }

    #[test]
    fn cmd_stop_help_flag() {
        let _cfg = with_temp_config_dir();
        let g = GlobalFlags::default();
        cmd_stop(&g, &["--help".into()]).unwrap();
    }

    #[test]
    fn cmd_dev_bad_port() {
        let _cfg = with_temp_config_dir();
        let g = GlobalFlags::default();
        let err = cmd_dev(&g, &["--port=notanumber".into()]).unwrap_err();
        assert!(err.to_string().contains("port"), "err={err}");
    }

    #[test]
    fn resolve_compose_file_env_missing() {
        std::env::set_var("BASIN_COMPOSE_FILE", "/no/such/docker-compose.yml");
        let result = resolve_compose_file(None);
        std::env::remove_var("BASIN_COMPOSE_FILE");
        assert!(result.is_err(), "expected error for missing compose file");
    }
}
