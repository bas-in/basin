//! output — JSON + table formatters and small stdin/file helpers shared
//! across every command. The table writer mirrors Go's text/tabwriter:
//! a header row, a dim separator, and space-padded columns.

use std::io::{self, BufRead, Read, Write};

use serde::Serialize;

use crate::error::CliResult;
use crate::global::GlobalFlags;

const ANSI_DIM: &str = "\x1b[2m";
const ANSI_RESET: &str = "\x1b[0m";

/// print_json writes `v` as pretty-printed (2-space) JSON to `w`,
/// matching Go's `printJSON` (no HTML escaping, trailing newline).
pub fn print_json<W: Write, T: Serialize>(w: &mut W, v: &T) -> CliResult<()> {
    let s = serde_json::to_string_pretty(v)?;
    w.write_all(s.as_bytes())?;
    w.write_all(b"\n")?;
    Ok(())
}

/// Table is a tiny column-aligned writer for the non-JSON output path.
/// Rows are buffered so we can compute column widths before flushing
/// (the stdlib has no tabwriter equivalent).
pub struct Table {
    headers: Vec<String>,
    rows: Vec<Vec<String>>,
    dim: bool,
}

impl Table {
    /// new_table returns a table. Separator dimming is enabled only when
    /// colour is OK and stdout is a tty.
    pub fn new(g: &GlobalFlags, headers: &[&str]) -> Table {
        Table {
            headers: headers.iter().map(|s| s.to_string()).collect(),
            rows: Vec::new(),
            dim: !g.no_color
                && std::env::var_os("NO_COLOR").is_none()
                && is_terminal_stdout(),
        }
    }

    /// row appends one data row; cells are stringified by the caller.
    pub fn row(&mut self, cells: &[&str]) {
        self.rows.push(cells.iter().map(|s| s.to_string()).collect());
    }

    /// row_strings appends an owned-string row (ergonomic for formatted cells).
    pub fn row_strings(&mut self, cells: Vec<String>) {
        self.rows.push(cells);
    }

    /// flush writes the buffered table to stdout with aligned columns.
    pub fn flush(&self) -> CliResult<()> {
        let cols = self.headers.len();
        if cols == 0 {
            return Ok(());
        }
        let mut widths: Vec<usize> = self.headers.iter().map(|h| h.chars().count()).collect();
        for r in &self.rows {
            for (i, c) in r.iter().enumerate().take(cols) {
                widths[i] = widths[i].max(c.chars().count());
            }
        }
        let out = io::stdout();
        let mut w = out.lock();
        write_row(&mut w, &self.headers, &widths)?;
        let sep: Vec<String> = self.headers.iter().map(|h| "─".repeat(h.chars().count())).collect();
        let sep_line = pad_join(&sep, &widths);
        if self.dim {
            writeln!(w, "{ANSI_DIM}{sep_line}{ANSI_RESET}")?;
        } else {
            writeln!(w, "{sep_line}")?;
        }
        for r in &self.rows {
            write_row(&mut w, r, &widths)?;
        }
        Ok(())
    }
}

fn write_row<W: Write>(w: &mut W, cells: &[String], widths: &[usize]) -> CliResult<()> {
    writeln!(w, "{}", pad_join(cells, widths))?;
    Ok(())
}

fn pad_join(cells: &[String], widths: &[usize]) -> String {
    let mut parts = Vec::with_capacity(cells.len());
    for (i, c) in cells.iter().enumerate() {
        if i + 1 == cells.len() {
            parts.push(c.clone());
        } else {
            let pad = widths.get(i).copied().unwrap_or(0);
            let cur = c.chars().count();
            let mut s = c.clone();
            if cur < pad + 2 {
                s.push_str(&" ".repeat(pad + 2 - cur));
            }
            parts.push(s);
        }
    }
    parts.join("")
}

/// is_terminal_stdout is a best-effort tty check via the unix isatty
/// syscall; non-unix conservatively returns false.
pub fn is_terminal_stdout() -> bool {
    #[cfg(unix)]
    {
        use std::os::unix::io::AsRawFd;
        // SAFETY: isatty takes a raw fd and only reads kernel state.
        unsafe { libc_isatty(io::stdout().as_raw_fd()) }
    }
    #[cfg(not(unix))]
    {
        false
    }
}

#[cfg(unix)]
unsafe fn libc_isatty(fd: i32) -> bool {
    extern "C" {
        fn isatty(fd: i32) -> i32;
    }
    isatty(fd) == 1
}

/// print_err writes a `basin: …` line to stderr (errors always print —
/// --quiet only suppresses prose).
pub fn print_err(_g: &GlobalFlags, args: std::fmt::Arguments<'_>) {
    eprintln!("basin: {args}");
}

/// print_info writes prose to stderr, suppressed under --quiet.
pub fn print_info(g: &GlobalFlags, args: std::fmt::Arguments<'_>) {
    if g.quiet {
        return;
    }
    eprintln!("{args}");
}

/// printerr! / printinfo! mirror Go's printErr / printInfo call sites.
#[macro_export]
macro_rules! printerr {
    ($g:expr, $($a:tt)*) => { $crate::output::print_err($g, format_args!($($a)*)) };
}
#[macro_export]
macro_rules! printinfo {
    ($g:expr, $($a:tt)*) => { $crate::output::print_info($g, format_args!($($a)*)) };
}

/// read_all_stdin slurps stdin until EOF, returning "" when nothing is
/// piped (interactive tty).
pub fn read_all_stdin() -> CliResult<String> {
    #[cfg(unix)]
    {
        use std::os::unix::io::AsRawFd;
        if unsafe { libc_isatty(io::stdin().as_raw_fd()) } {
            return Ok(String::new());
        }
    }
    let mut s = String::new();
    io::stdin().read_to_string(&mut s)?;
    Ok(s)
}

/// read_maybe_file loads a file into a string when `path` is non-empty.
pub fn read_maybe_file(path: &str) -> CliResult<String> {
    if path.is_empty() {
        return Ok(String::new());
    }
    Ok(std::fs::read_to_string(path)?)
}

/// read_line pulls a single line from stdin, trimming the trailing
/// newline. Used by confirm-by-name prompts and `basin login`.
pub fn read_line() -> CliResult<String> {
    let mut s = String::new();
    let n = io::stdin().lock().read_line(&mut s)?;
    if n == 0 {
        return Ok(String::new());
    }
    Ok(s.trim_end_matches(['\r', '\n']).to_string())
}

/// stringify_cell is the lossy "json-ish" cell renderer for the table
/// view. Strings stay strings; everything else gets compact JSON.
pub fn stringify_cell(v: &serde_json::Value) -> String {
    match v {
        serde_json::Value::Null => String::new(),
        serde_json::Value::String(s) => s.clone(),
        serde_json::Value::Bool(b) => b.to_string(),
        serde_json::Value::Number(n) => n.to_string(),
        other => other.to_string(),
    }
}
