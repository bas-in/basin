//! Serialisation seam between openraft messages and the proto's opaque
//! `bytes payload` field.
//!
//! Codec = `serde_json`. This is deliberate and matches `raft_storage.rs`,
//! which JSON-frames the openraft `Entry<C>` body on disk: the wire and the
//! log agree on how an entry serialises, so there is a single serde contract
//! to keep working, exercised by both the storage unit tests and the network
//! round-trip tests.
//!
//! See `proto/raft.proto` for the compatibility implications of pinning the
//! wire format to openraft's serde representation.

use openraft::error::RaftError;
use serde::de::DeserializeOwned;
use serde::Serialize;

use crate::raft_wal::BasinRaftTypeConfig;

type C = BasinRaftTypeConfig;
type NodeId = u64;

/// A request/response payload failed to encode/decode. This is a *transport*
/// fault (bad bytes on the wire), distinct from a raft protocol error — the
/// client maps it to `NetworkError`, never `RemoteError`.
#[derive(Debug, thiserror::Error)]
pub enum CodecError {
    #[error("raft payload encode failed: {0}")]
    Encode(serde_json::Error),
    #[error("raft payload decode failed: {0}")]
    Decode(serde_json::Error),
}

/// Encode an openraft message into the proto `payload` bytes.
pub fn encode<T: Serialize>(value: &T) -> Result<Vec<u8>, CodecError> {
    serde_json::to_vec(value).map_err(CodecError::Encode)
}

/// Decode an openraft message from the proto `payload` bytes.
pub fn decode<T: DeserializeOwned>(bytes: &[u8]) -> Result<T, CodecError> {
    serde_json::from_slice(bytes).map_err(CodecError::Decode)
}

/// Encode the `RaftError` an inbound handler produced into the response
/// envelope's `raft_error` field. `E` is `()`/`Infallible`-shaped for
/// append/vote and `InstallSnapshotError` for install_snapshot.
pub fn encode_raft_error<E>(err: &RaftError<NodeId, E>) -> Result<Vec<u8>, CodecError>
where
    E: Serialize,
{
    serde_json::to_vec(err).map_err(CodecError::Encode)
}

/// Decode a `RaftError` from the response envelope's `raft_error` field.
pub fn decode_raft_error<E>(bytes: &[u8]) -> Result<RaftError<NodeId, E>, CodecError>
where
    E: DeserializeOwned,
{
    serde_json::from_slice(bytes).map_err(CodecError::Decode)
}

// Phantom anchor so a stray `C` change surfaces here at compile time rather
// than only in the call sites.
#[allow(dead_code)]
fn _type_anchor(_: std::marker::PhantomData<C>) {}
