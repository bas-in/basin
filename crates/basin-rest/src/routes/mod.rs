//! Endpoint handlers, split by surface area.
//!
//! - [`data`] handles `/rest/v1/<table>` (GET / POST / PATCH / DELETE).
//! - [`auth`] handles `/auth/v1/{signup,signin,refresh,verify-email,
//!   reset-password,magic-link}`.
//! - [`storage`] handles `/storage/v1/{object,bucket}/*` (Phase 5.17.B).
//! - [`storage_sign`] handles signed-URL mint/verify (Phase 5.17.D).
//!
//! Routes are wired together in [`crate::server`].

pub(crate) mod admin;
pub(crate) mod admin_functions;
pub(crate) mod auth;
pub(crate) mod data;
pub(crate) mod fn_handler;
pub(crate) mod inbound;
pub(crate) mod openapi;
pub(crate) mod rpc;
pub(crate) mod storage;
pub(crate) mod storage_sign;
