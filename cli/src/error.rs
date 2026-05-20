//! error — the app-wide error alias plus the two sentinels ported from
//! the Go CLI: `ApiError` (the cloud's typed `{error:{code,message}}`
//! envelope) and `Silent` (a handler already printed its own message,
//! so `main` exits non-zero without re-printing).

use std::error::Error as StdError;
use std::fmt;

/// CliResult mirrors Go's `error` return: every handler returns
/// `Result<(), Box<dyn Error>>`. The boxed error keeps the typed
/// `ApiError` recoverable via [`as_api_error`].
pub type CliResult<T> = std::result::Result<T, Box<dyn StdError + Send + Sync>>;

/// ApiError is the cloud's typed error envelope. Mirrors
/// `httpserver.WriteError` so commands can branch on `code` when they
/// care (e.g. `pat_scope_read_only`, `engine_unconfigured`).
#[derive(Debug, Clone)]
pub struct ApiError {
    pub http_status: u16,
    pub code: String,
    pub message: String,
}

impl fmt::Display for ApiError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match (self.message.is_empty(), self.code.is_empty()) {
            (true, _) => write!(f, "HTTP {} ({})", self.http_status, self.code),
            (false, true) => write!(f, "HTTP {}: {}", self.http_status, self.message),
            (false, false) => write!(
                f,
                "HTTP {} ({}): {}",
                self.http_status, self.code, self.message
            ),
        }
    }
}

impl StdError for ApiError {}

/// as_api_error unwraps a boxed error into `&ApiError` when possible.
/// Convenient for `if let Some(e) = as_api_error(&err) { if e.code == … }`.
pub fn as_api_error<'a>(err: &'a (dyn StdError + 'static)) -> Option<&'a ApiError> {
    err.downcast_ref::<ApiError>()
}

/// Silent is returned by handlers that already wrote a tailored error
/// message to stderr; `main` exits non-zero without re-printing.
#[derive(Debug)]
pub struct Silent;

impl fmt::Display for Silent {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "silent")
    }
}

impl StdError for Silent {}

/// silent builds the boxed [`Silent`] sentinel.
pub fn silent() -> Box<dyn StdError + Send + Sync> {
    Box::new(Silent)
}

/// is_silent reports whether a boxed error is the [`Silent`] sentinel.
pub fn is_silent(err: &(dyn StdError + 'static)) -> bool {
    err.is::<Silent>()
}

/// msg builds a boxed error from a string — the ergonomic equivalent of
/// Go's `fmt.Errorf` / `errors.New` for the common no-format case.
pub fn msg(s: impl Into<String>) -> Box<dyn StdError + Send + Sync> {
    Box::<dyn StdError + Send + Sync>::from(s.into())
}
