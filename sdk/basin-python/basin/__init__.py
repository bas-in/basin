"""basin — Python client for Basin's HTTP surfaces."""

from .client import BasinClient, AsyncBasinClient, create_client, create_async_client
from .auth import AuthClient
from .errors import (
    BasinError,
    BasinApiError,
    BasinNetworkError,
    ERROR_CODES,
)
from .types import (
    Session,
    SignUpResult,
    ApiKeyIssued,
    ApiKeyDescriptor,
    ExecTag,
    Page,
    Bucket,
    StorageObject,
    SignedUrl,
    FunctionInvokeResult,
    RealtimeEvent,
    RealtimeErrorFrame,
    RealtimeGapFrame,
    ChangeOp,
)
from .query import QueryBuilder, QueryResult, Row
# Realtime types — lazy extra; import only when websockets is available.
from .realtime import (
    RealtimeClient,
    ChannelHandle,
    PresenceEntry,
    PresenceStateFrame,
    PresenceDiffFrame,
    PresenceErrorFrame,
    SubscribedFrame,
    UnsubscribedFrame,
)

__all__ = [
    "BasinClient",
    "AsyncBasinClient",
    "create_client",
    "create_async_client",
    "AuthClient",
    "BasinError",
    "BasinApiError",
    "BasinNetworkError",
    "ERROR_CODES",
    "Session",
    "SignUpResult",
    "ApiKeyIssued",
    "ApiKeyDescriptor",
    "ExecTag",
    "Page",
    "Bucket",
    "StorageObject",
    "SignedUrl",
    "FunctionInvokeResult",
    "RealtimeEvent",
    "RealtimeErrorFrame",
    "RealtimeGapFrame",
    "ChangeOp",
    "QueryBuilder",
    "QueryResult",
    "Row",
    "RealtimeClient",
    "ChannelHandle",
    "PresenceEntry",
    "PresenceStateFrame",
    "PresenceDiffFrame",
    "PresenceErrorFrame",
    "SubscribedFrame",
    "UnsubscribedFrame",
]
