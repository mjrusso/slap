//! Whether a database or reader is closed.

use std::future::Future;

use tokio::sync::{RwLock, RwLockReadGuard};

use crate::reply::{NifError, NifOutcome};
use crate::runtime;

/// Holds `true` once close has started. Every call holds the read lock while
/// it runs. Close takes the write lock, so it waits for calls already in
/// flight, and calls that come later see `true` and fail with a `:closed`
/// error.
pub(crate) struct Gate(RwLock<bool>);

impl Gate {
    pub(crate) fn new() -> Self {
        Self(RwLock::new(false))
    }

    /// Waits until no close is in progress and fails if closed. Hold the
    /// returned guard for the whole call.
    pub(crate) async fn enter(&self) -> NifOutcome<RwLockReadGuard<'_, bool>> {
        let guard = self.0.read().await;
        if *guard {
            return Err(NifError::closed());
        }
        Ok(guard)
    }

    /// Like `enter`, for calls that run on the scheduler and must not wait.
    pub(crate) fn try_enter(&self) -> NifOutcome<RwLockReadGuard<'_, bool>> {
        match self.0.try_read() {
            Ok(guard) if !*guard => Ok(guard),
            _ => Err(NifError::closed()),
        }
    }

    /// Waits for calls in flight and marks it closed. Returns `false` if it
    /// was already closed. Marks it closed even if the close that follows
    /// fails (for example because the database was fenced): it is not usable
    /// either way.
    pub(crate) async fn close(&self) -> bool {
        let mut closed = self.0.write().await;
        !std::mem::replace(&mut *closed, true)
    }

    /// For a resource's destructor: unless the handle was closed, logs that
    /// `what` was garbage collected without `close_fun` and runs `close` in
    /// the background.
    pub(crate) fn close_dropped<F>(self, what: &str, close_fun: &str, close: F)
    where
        F: Future<Output = Result<(), slatedb::Error>> + Send + 'static,
    {
        if self.0.into_inner() {
            return;
        }
        log::warn!(
            target: "slap_slatedb",
            "a SlateDB {what} was garbage collected without {close_fun}; \
             closing it in the background"
        );
        runtime().spawn(async move {
            if let Err(err) = close.await {
                log::warn!(target: "slap_slatedb", "background close failed: {err}");
            }
        });
    }
}
