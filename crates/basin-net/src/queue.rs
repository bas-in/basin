//! Async request queue + per-shard runner. Mirrors `pg_net`'s
//! `net.http_post` fire-and-forget shape:
//!
//! 1. Caller submits an [`HttpRequest`] via [`RequestQueue::submit`] and gets
//!    a [`RequestId`] back synchronously.
//! 2. A background tokio task pulls each pending request off the in-memory
//!    channel, dispatches it through [`HttpClient`], and persists the
//!    terminal response (or terminal error) to the per-tenant
//!    `_net_http_response` table via [`ResponseStore`].
//! 3. The caller polls `_net_http_response` (or [`ResponseStore::get`]) by id.
//!
//! ## Why tokio mpsc and not a SQL queue table
//!
//! pg_net keeps the queue in `net._http_request_queue` and walks it with a
//! background worker. Basin's flat-table-only v0.1 model would let us do the
//! same shape, but it would mean every submit incurs a SQL INSERT and every
//! tick incurs a SQL DELETE. For the v0.1 viability bar we just want to
//! prove the round-trip works; an in-memory mpsc gets us there with a
//! tenth of the moving parts. When the engine grows true UPDATE-by-id and
//! the parser swallows the `net.` schema prefix, swapping the channel for a
//! catalog-backed queue is a focused refactor.
//!
//! The [`RequestQueueHandle`] returned by [`RequestQueue::spawn`] is the
//! shutdown / join handle production code holds onto; tests can use the
//! sync [`RequestQueue::run_one`] entry point to step the runner
//! deterministically.

use std::sync::Arc;

use basin_common::{Result, TenantId};
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use uuid::Uuid;

use crate::client::HttpClient;
use crate::store::ResponseStore;
use crate::types::{HttpRequest, HttpResponse, RequestId};

/// In-memory queue of pending async requests. Cheap to clone (`Arc` inside);
/// every clone shares the same underlying channel and store.
#[derive(Clone)]
pub struct RequestQueue {
    inner: Arc<RequestQueueInner>,
}

struct RequestQueueInner {
    client: HttpClient,
    store: ResponseStore,
    tx: mpsc::UnboundedSender<QueuedRequest>,
    /// Receiver is wrapped in a tokio mutex so [`run_one`] can hold it
    /// across an await without blocking the runtime. [`spawn`] takes the
    /// receiver out once and consumes it inside the spawned task.
    rx: tokio::sync::Mutex<Option<mpsc::UnboundedReceiver<QueuedRequest>>>,
}

#[derive(Debug)]
struct QueuedRequest {
    tenant: TenantId,
    id: RequestId,
    req: HttpRequest,
}

impl RequestQueue {
    pub fn new(client: HttpClient, store: ResponseStore) -> Self {
        let (tx, rx) = mpsc::unbounded_channel();
        Self {
            inner: Arc::new(RequestQueueInner {
                client,
                store,
                tx,
                rx: tokio::sync::Mutex::new(Some(rx)),
            }),
        }
    }

    /// Underlying response store. Tests use this to assert rows landed.
    pub fn store(&self) -> &ResponseStore {
        &self.inner.store
    }

    /// Underlying client. Lets callers seed the allowlist before submitting.
    pub fn client(&self) -> &HttpClient {
        &self.inner.client
    }

    /// Enqueue a request. Returns the [`RequestId`] the response will land
    /// under in `_net_http_response`. The request is **not** dispatched
    /// here; the runner picks it up off the channel.
    ///
    /// This is the API behind `SELECT net.http_post(url := ..., body := ...)`.
    pub fn submit(&self, tenant: &TenantId, req: HttpRequest) -> RequestId {
        let id = Uuid::new_v4();
        let qr = QueuedRequest {
            tenant: *tenant,
            id,
            req,
        };
        // unbounded_send only fails when the receiver is dropped, which would
        // mean the runner has exited. Production code panics here would
        // hide a shutdown race; instead we silently drop, and the caller
        // will time out polling for the row. Tests never see this.
        let _ = self.inner.tx.send(qr);
        id
    }

    /// Run exactly one queued request through the client and persist the
    /// response. Returns `None` when the channel is empty. Tests use this
    /// to step the runner deterministically without spinning a background
    /// task.
    pub async fn run_one(&self) -> Option<RequestId> {
        let mut guard = self.inner.rx.lock().await;
        let rx = guard.as_mut()?;
        let qr = rx.try_recv().ok()?;
        drop(guard);
        let id = qr.id;
        self.dispatch(qr).await;
        Some(id)
    }

    /// Drain every pending request synchronously. Returns the ids of every
    /// request that was processed, in submission order.
    pub async fn drain(&self) -> Vec<RequestId> {
        let mut ids = Vec::new();
        while let Some(id) = self.run_one().await {
            ids.push(id);
        }
        ids
    }

    /// Spawn the production background runner. Returns a handle that aborts
    /// the runner on drop (caller can also `await` it to join). Calling
    /// `spawn` more than once on the same queue takes the receiver away and
    /// subsequent calls produce a degenerate handle that exits immediately.
    pub fn spawn(self) -> RequestQueueHandle {
        let queue = self.clone();
        let join = tokio::spawn(async move {
            let mut rx = match queue.inner.rx.lock().await.take() {
                Some(rx) => rx,
                None => return,
            };
            while let Some(qr) = rx.recv().await {
                queue.dispatch(qr).await;
            }
        });
        RequestQueueHandle { join }
    }

    async fn dispatch(&self, qr: QueuedRequest) {
        let resp = match self.inner.client.send(&qr.tenant, &qr.req).await {
            Ok(r) => r,
            Err(e) => HttpResponse {
                status: 0,
                headers: Default::default(),
                body: Vec::new(),
                error: Some(format!("{e}")),
            },
        };
        if let Err(e) = self.persist(&qr.tenant, qr.id, &resp).await {
            tracing::warn!(
                tenant = %qr.tenant,
                request_id = %qr.id,
                error = %e,
                "basin-net: failed to persist response row"
            );
        }
    }

    async fn persist(
        &self,
        tenant: &TenantId,
        id: RequestId,
        resp: &HttpResponse,
    ) -> Result<()> {
        self.inner.store.record(tenant, id, id, resp).await
    }
}

/// Join handle returned by [`RequestQueue::spawn`]. Drop aborts the runner;
/// `await` joins it.
pub struct RequestQueueHandle {
    join: JoinHandle<()>,
}

impl RequestQueueHandle {
    /// Abort the runner. Pending in-flight dispatches finish; new pulls
    /// stop.
    pub fn abort(&self) {
        self.join.abort();
    }
}

impl Drop for RequestQueueHandle {
    fn drop(&mut self) {
        self.join.abort();
    }
}
