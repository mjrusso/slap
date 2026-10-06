//! Durability subscriptions.

use rustler::types::atom::nil;
use rustler::{Atom, Encoder, Env, LocalPid, Monitor, OwnedEnv, Resource, ResourceArc, Term};
use tokio::task::AbortHandle;

use crate::db::DbResource;
use crate::reply::{close_reason_atom, Encode};
use crate::{atoms, runtime};

/// A durability subscription. Dropping the handle does not stop the
/// subscription; see `db_subscribe`.
pub(crate) struct SubscriptionResource {
    /// The task sends while holding this lock, and `cancel` takes it, so no
    /// message is sent once `cancel` returns. `unsubscribe(sub, flush: true)`
    /// depends on that.
    state: std::sync::Mutex<SubscriptionState>,
}

struct SubscriptionState {
    cancelled: bool,
    /// Set once the task is spawned. The task holds a reference to this
    /// resource, so the monitor on the subscriber lasts as long as the task.
    task: Option<AbortHandle>,
}

impl SubscriptionResource {
    fn cancel(&self) {
        let mut state = self.state.lock().unwrap();
        state.cancelled = true;
        if let Some(task) = state.task.take() {
            task.abort();
        }
    }
}

#[rustler::resource_impl]
impl Resource for SubscriptionResource {
    /// The subscriber exited: end the subscription now, rather than at the
    /// next send, which on an idle database may be when it closes.
    fn down<'a>(&'a self, _env: Env<'a>, _pid: LocalPid, _monitor: Monitor) {
        self.cancel();
    }
}

/// Sends `{:slap_slatedb_durable, ref, tag, durable_seq}` to `pid` now and each
/// time the durable sequence number goes up, and
/// `{:slap_slatedb_closed, ref, tag, reason}` once when the database closes.
/// `ref` is the reference the caller made for this subscription.
///
/// Updates are coalesced: a slow subscriber gets the latest value, not every
/// step. The subscription ends when the database closes, when `pid` exits
/// (it is monitored), or on `subscription_cancel`, not when the returned
/// handle is garbage collected.
#[rustler::nif]
fn db_subscribe<'a>(
    env: Env<'a>,
    db: ResourceArc<DbResource>,
    pid: LocalPid,
    sub_ref: Term<'a>,
    tag: Term<'a>,
) -> ResourceArc<SubscriptionResource> {
    // The reference and tag are stored in external term format, because each
    // send clears the message environment.
    let sub_ref = sub_ref.to_binary().as_slice().to_vec();
    let tag = tag.to_binary().as_slice().to_vec();
    let mut rx = db.db.subscribe();
    // Do not keep the database alive from the subscription task.
    drop(db);

    let sub = ResourceArc::new(SubscriptionResource {
        state: std::sync::Mutex::new(SubscriptionState {
            cancelled: false,
            task: None,
        }),
    });
    // The task keeps the resource, and so the monitor, alive while it runs.
    let held = sub.clone();

    let task = runtime().spawn(async move {
        let mut msg_env = OwnedEnv::new();
        let mut send = |event: Atom, payload: Encode| {
            let state = held.state.lock().unwrap();
            if state.cancelled {
                return false;
            }
            msg_env
                .send_and_clear(&pid, |env| {
                    let decode = |bytes: &[u8]| {
                        env.binary_to_term(bytes)
                            .map(|(term, _)| term)
                            .unwrap_or_else(|| nil().encode(env))
                    };
                    (event, decode(&sub_ref), decode(&tag), payload(env)).encode(env)
                })
                .is_ok()
        };

        let mut last_sent: Option<u64> = None;
        loop {
            // Copy the values out and release the borrow at once. Holding it
            // across an await blocks SlateDB's status updates.
            let (durable_seq, close_reason) = {
                let status = rx.borrow_and_update();
                (status.durable_seq, status.close_reason)
            };
            if last_sent.is_none_or(|last| durable_seq > last) {
                if !send(
                    atoms::slap_slatedb_durable(),
                    Box::new(move |env| durable_seq.encode(env)),
                ) {
                    return;
                }
                last_sent = Some(durable_seq);
            }
            if let Some(reason) = close_reason {
                let reason = close_reason_atom(reason);
                send(
                    atoms::slap_slatedb_closed(),
                    Box::new(move |env| reason.encode(env)),
                );
                return;
            }
            if rx.changed().await.is_err() {
                // The database was dropped without reporting a close reason.
                send(
                    atoms::slap_slatedb_closed(),
                    Box::new(|env| atoms::unknown().encode(env)),
                );
                return;
            }
        }
    });

    sub.state.lock().unwrap().task = Some(task.abort_handle());
    // Monitor only once the abort handle is stored, so a `down` cannot miss
    // it. `None` means `pid` is already dead.
    if sub.monitor(Some(env), &pid).is_none() {
        sub.cancel();
    }
    sub
}

#[rustler::nif]
fn subscription_cancel(sub: ResourceArc<SubscriptionResource>) -> Atom {
    sub.cancel();
    atoms::ok()
}
