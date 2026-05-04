//! Outbound email via SMTP (production) or a stub (tests).
//!
//! `Mailer` is a thin trait so the flow modules don't depend on `lettre`
//! directly. `SmtpMailer` is the production transport (StartTLS or implicit
//! TLS via `lettre::AsyncSmtpTransport::<Tokio1Executor>`). `StubMailer`
//! captures sent messages for assertions in tests.

use std::sync::Arc;

use async_trait::async_trait;
use basin_common::{BasinError, Result};
use lettre::{
    message::header::ContentType,
    transport::{
        smtp::{authentication::Credentials, AsyncSmtpTransport},
        stub::AsyncStubTransport,
    },
    AsyncTransport, Message, Tokio1Executor,
};

use crate::config::{SmtpConfig, SmtpTls};

/// What we tell the recipient. Plain text only in v1; HTML is post-v1.
#[derive(Debug, Clone)]
pub struct Outbound {
    pub to: String,
    pub subject: String,
    pub body: String,
}

#[async_trait]
pub trait Mailer: Send + Sync {
    async fn send(&self, msg: Outbound) -> Result<()>;
}

/// Production transport. Cheap to clone (`Arc` internals).
#[derive(Clone)]
pub struct SmtpMailer {
    transport: Arc<AsyncSmtpTransport<Tokio1Executor>>,
    from: String,
}

impl SmtpMailer {
    pub fn from_config(cfg: &SmtpConfig) -> Result<Self> {
        let credentials = Credentials::new(cfg.username.clone(), cfg.password.clone());
        let builder = match cfg.tls {
            SmtpTls::StartTls => AsyncSmtpTransport::<Tokio1Executor>::starttls_relay(&cfg.host)
                .map_err(|e| BasinError::internal(format!("smtp starttls: {e}")))?,
            SmtpTls::Implicit => AsyncSmtpTransport::<Tokio1Executor>::relay(&cfg.host)
                .map_err(|e| BasinError::internal(format!("smtp relay: {e}")))?,
            SmtpTls::None => AsyncSmtpTransport::<Tokio1Executor>::builder_dangerous(&cfg.host),
        };
        let transport = builder
            .credentials(credentials)
            .port(cfg.port)
            .build();
        let from = match cfg.from_name.as_deref() {
            Some(name) => format!("{name} <{addr}>", addr = cfg.from_email),
            None => cfg.from_email.clone(),
        };
        Ok(Self {
            transport: Arc::new(transport),
            from,
        })
    }
}

#[async_trait]
impl Mailer for SmtpMailer {
    async fn send(&self, msg: Outbound) -> Result<()> {
        let from = self
            .from
            .parse()
            .map_err(|e| BasinError::internal(format!("smtp parse from: {e}")))?;
        let to = msg
            .to
            .parse()
            .map_err(|e| BasinError::internal(format!("smtp parse to: {e}")))?;
        let email = Message::builder()
            .from(from)
            .to(to)
            .subject(msg.subject)
            .header(ContentType::TEXT_PLAIN)
            .body(msg.body)
            .map_err(|e| BasinError::internal(format!("smtp build: {e}")))?;
        self.transport
            .send(email)
            .await
            .map_err(|e| BasinError::internal(format!("smtp send: {e}")))?;
        Ok(())
    }
}

/// Test mailer. Wraps `lettre::transport::stub::AsyncStubTransport` and a
/// shared message log so assertions can pull out everything that "would have
/// been sent."
#[derive(Clone)]
pub struct StubMailer {
    inner: Arc<AsyncStubTransport>,
    sent: Arc<std::sync::Mutex<Vec<Outbound>>>,
    from: String,
}

impl StubMailer {
    pub fn new(from: impl Into<String>) -> Self {
        Self {
            inner: Arc::new(AsyncStubTransport::new_ok()),
            sent: Arc::new(std::sync::Mutex::new(Vec::new())),
            from: from.into(),
        }
    }

    pub fn sent(&self) -> Vec<Outbound> {
        self.sent.lock().unwrap().clone()
    }
}

#[async_trait]
impl Mailer for StubMailer {
    async fn send(&self, msg: Outbound) -> Result<()> {
        // Drive the lettre stub so its message log also has the envelope; this
        // keeps tests honest about the email format actually being parseable.
        let from = self
            .from
            .parse()
            .map_err(|e| BasinError::internal(format!("stub parse from: {e}")))?;
        let to = msg
            .to
            .parse()
            .map_err(|e| BasinError::internal(format!("stub parse to: {e}")))?;
        let email = Message::builder()
            .from(from)
            .to(to)
            .subject(msg.subject.clone())
            .header(ContentType::TEXT_PLAIN)
            .body(msg.body.clone())
            .map_err(|e| BasinError::internal(format!("stub build: {e}")))?;
        self.inner
            .send(email)
            .await
            .map_err(|e| BasinError::internal(format!("stub send: {e}")))?;
        self.sent.lock().unwrap().push(msg);
        Ok(())
    }
}

// --- Templates --------------------------------------------------------------
//
// Three plain-text templates. HTML is post-v1 per ADR; this keeps the surface
// trivial to audit. Tokens are interpolated as raw URLs the recipient can
// click through; the actual HTTP endpoints live in `basin-rest` (ADR 0006).

pub fn verify_template(token: &str) -> Outbound {
    Outbound {
        to: String::new(),
        subject: "Verify your email".to_owned(),
        body: format!(
            "Welcome.\n\n\
             Confirm your email by visiting:\n\n  \
             https://example.invalid/auth/verify?token={token}\n\n\
             This link expires in 24 hours.\n"
        ),
    }
}

pub fn reset_template(token: &str) -> Outbound {
    Outbound {
        to: String::new(),
        subject: "Reset your password".to_owned(),
        body: format!(
            "We received a password-reset request.\n\n\
             If this was you, visit:\n\n  \
             https://example.invalid/auth/reset?token={token}\n\n\
             This link expires in 1 hour. If you didn't request this, ignore this email.\n"
        ),
    }
}

pub fn magic_link_template(token: &str) -> Outbound {
    Outbound {
        to: String::new(),
        subject: "Your sign-in link".to_owned(),
        body: format!(
            "Click to sign in:\n\n  \
             https://example.invalid/auth/magic?token={token}\n\n\
             This link expires in 15 minutes.\n"
        ),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn stub_mailer_records_sends() {
        let m = StubMailer::new("noreply@example.com");
        let mut out = verify_template("abc");
        out.to = "alice@example.com".into();
        m.send(out.clone()).await.unwrap();
        let log = m.sent();
        assert_eq!(log.len(), 1);
        assert_eq!(log[0].to, "alice@example.com");
        assert!(log[0].body.contains("token=abc"));
    }

    #[test]
    fn templates_include_token() {
        assert!(verify_template("xyz").body.contains("xyz"));
        assert!(reset_template("xyz").body.contains("xyz"));
        assert!(magic_link_template("xyz").body.contains("xyz"));
    }
}
