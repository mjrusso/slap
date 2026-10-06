//! Transactions. Reads go through `read`.

use rustler::types::atom::nil;
use rustler::{Binary, Encoder, Env, NifUnitEnum, Resource, ResourceArc, Term};
use slatedb::IsolationLevel;
use tokio::sync::RwLock;

use crate::atoms;
use crate::db::DbResource;
use crate::read::validate_key;
use crate::reply::{ok_atom, ok_with, spawn_reply, NifError};
use crate::write::{finish_write, merge_options, put_options, validate_value};

pub(crate) struct TransactionResource {
    pub(crate) db: ResourceArc<DbResource>,
    // `None` after commit or rollback. Reads hold the read lock across their
    // await. Commit and rollback take the write lock only to move the
    // transaction out.
    pub(crate) tx: RwLock<Option<slatedb::DbTransaction>>,
}

#[rustler::resource_impl]
impl Resource for TransactionResource {}

pub(crate) fn tx_completed() -> NifError {
    NifError::invalid("transaction already committed or rolled back")
}

#[derive(NifUnitEnum)]
enum Isolation {
    Snapshot,
    Serializable,
}

#[rustler::nif]
fn db_begin<'a>(
    env: Env<'a>,
    db: ResourceArc<DbResource>,
    isolation: Isolation,
    reply_ref: Term<'a>,
) -> Term<'a> {
    let isolation = match isolation {
        Isolation::Snapshot => IsolationLevel::Snapshot,
        Isolation::Serializable => IsolationLevel::SerializableSnapshot,
    };
    spawn_reply(env, reply_ref, async move {
        let tx = {
            let _open = db.enter().await?;
            db.db.begin(isolation).await?
        };
        Ok(ok_with(ResourceArc::new(TransactionResource {
            db,
            tx: RwLock::new(Some(tx)),
        })))
    })
}

/// Runs `f` against the open transaction. Transaction writes only touch
/// memory, so this runs on the scheduler. It does not wait for any lock: the
/// locks are only held for writing while `close/1`, commit or rollback runs.
fn with_open_tx<'a>(
    env: Env<'a>,
    tx: &TransactionResource,
    f: impl FnOnce(&slatedb::DbTransaction) -> Result<(), slatedb::Error>,
) -> Term<'a> {
    let _open = match tx.db.gate.try_enter() {
        Ok(guard) => guard,
        Err(err) => return err.encode(env),
    };
    let guard = match tx.tx.try_read() {
        Ok(guard) => guard,
        Err(_) => return NifError::invalid("transaction is being committed").encode(env),
    };
    match guard.as_ref() {
        None => tx_completed().encode(env),
        Some(tx) => match f(tx) {
            Ok(()) => atoms::ok().encode(env),
            Err(err) => NifError::from(err).encode(env),
        },
    }
}

#[rustler::nif]
fn tx_put<'a>(
    env: Env<'a>,
    tx: ResourceArc<TransactionResource>,
    key: Binary<'a>,
    value: Binary<'a>,
    ttl_ms: Option<u64>,
) -> Term<'a> {
    let (key, value) = (key.as_slice(), value.as_slice());
    if let Err(err) = validate_key(key).and_then(|_| validate_value(value)) {
        return err.encode(env);
    }
    with_open_tx(env, &tx, |tx| {
        tx.put_with_options(key, value, &put_options(ttl_ms))
    })
}

#[rustler::nif]
fn tx_delete<'a>(env: Env<'a>, tx: ResourceArc<TransactionResource>, key: Binary<'a>) -> Term<'a> {
    let key = key.as_slice();
    if let Err(err) = validate_key(key) {
        return err.encode(env);
    }
    with_open_tx(env, &tx, |tx| tx.delete(key))
}

#[rustler::nif]
fn tx_merge<'a>(
    env: Env<'a>,
    tx: ResourceArc<TransactionResource>,
    key: Binary<'a>,
    operand: Binary<'a>,
    ttl_ms: Option<u64>,
) -> Term<'a> {
    let (key, operand) = (key.as_slice(), operand.as_slice());
    if let Err(err) = validate_key(key).and_then(|_| tx.db.validate_operand(operand)) {
        return err.encode(env);
    }
    with_open_tx(env, &tx, |tx| {
        tx.merge_with_options(key, operand, &merge_options(ttl_ms))
    })
}

/// Replies `{:ok, seq}`, or `{:ok, nil}` when the transaction had no writes.
#[rustler::nif]
fn tx_commit<'a>(
    env: Env<'a>,
    tx: ResourceArc<TransactionResource>,
    await_durable: bool,
    reply_ref: Term<'a>,
) -> Term<'a> {
    spawn_reply(env, reply_ref, async move {
        let _open = tx.db.enter().await?;
        let open_tx = tx.tx.write().await.take().ok_or_else(tx_completed)?;
        match open_tx.commit().await? {
            Some(handle) => finish_write(&tx.db, handle, await_durable).await,
            None => Ok(ok_with(nil())),
        }
    })
}

#[rustler::nif]
fn tx_rollback<'a>(
    env: Env<'a>,
    tx: ResourceArc<TransactionResource>,
    reply_ref: Term<'a>,
) -> Term<'a> {
    spawn_reply(env, reply_ref, async move {
        // Rolling back twice is not an error. Rollback works on a closed
        // database too: it only drops the transaction's writes.
        if let Some(open_tx) = tx.tx.write().await.take() {
            open_tx.rollback();
        }
        Ok(ok_atom())
    })
}
