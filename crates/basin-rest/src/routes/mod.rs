//! Endpoint handlers, split by surface area.
//!
//! - [`data`] handles `/rest/v1/<table>` (GET / POST / PATCH / DELETE).
//! - [`auth`] handles `/auth/v1/{signup,signin,refresh,verify-email,
//!   reset-password,magic-link}`.
//!
//! Routes are wired together in [`crate::server`].

pub(crate) mod auth;
pub(crate) mod data;
