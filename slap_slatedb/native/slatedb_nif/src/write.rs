//! Writes: puts, deletes, merges and batches of them.

use rustler::{Atom, Binary, Env, ResourceArc, Term};
use slatedb::config::{MergeOptions, PutOptions, Ttl, WriteOptions};
use slatedb::WriteBatch;

use crate::atoms;
use crate::binaries::to_bytes;
use crate::db::DbResource;
use crate::read::validate_key;
use crate::reply::{ok_with, spawn_reply_with, Encode, NifError, NifOutcome};

pub(crate) fn validate_value(value: &[u8]) -> NifOutcome<()> {
    if u32::try_from(value.len()).is_err() {
        return Err(NifError::invalid("value is too large"));
    }
    Ok(())
}

fn ttl(ttl_ms: Option<u64>) -> Ttl {
    match ttl_ms {
        Some(ms) => Ttl::ExpireAfterMillis(ms),
        None => Ttl::Default,
    }
}

pub(crate) fn merge_options(ttl_ms: Option<u64>) -> MergeOptions {
    MergeOptions { ttl: ttl(ttl_ms) }
}

pub(crate) fn put_options(ttl_ms: Option<u64>) -> PutOptions {
    PutOptions { ttl: ttl(ttl_ms) }
}

/// Finishes a write and replies `{:ok, seq}`. When `await_durable` is set,
/// waits until the write is durable in object storage before replying.
pub(crate) async fn finish_write(
    db: &DbResource,
    handle: slatedb::WriteHandle,
    await_durable: bool,
) -> NifOutcome<Encode> {
    let seq = handle.seqnum();
    db.record_write(seq);
    if await_durable {
        handle.await_durable().await?;
    }
    Ok(ok_with(seq))
}

/// Writes `batch`, or replies with its error, and replies `{:ok, seq}`.
fn spawn_write<'a>(
    env: Env<'a>,
    reply_ref: Term<'a>,
    db: ResourceArc<DbResource>,
    batch: NifOutcome<WriteBatch>,
    await_durable: bool,
) -> Term<'a> {
    spawn_reply_with(env, reply_ref, batch, |batch| async move {
        let _open = db.enter().await?;
        let handle = db
            .db
            .write_with_options(batch, &WriteOptions::default())
            .await?;
        finish_write(&db, handle, await_durable).await
    })
}

fn put(
    batch: &mut WriteBatch,
    key: Binary<'_>,
    value: Binary<'_>,
    ttl_ms: Option<u64>,
) -> NifOutcome<()> {
    validate_key(key.as_slice())?;
    validate_value(value.as_slice())?;
    // `put_bytes_with_options` takes the shared `Bytes` without copying.
    batch.put_bytes_with_options(to_bytes(key), to_bytes(value), &put_options(ttl_ms));
    Ok(())
}

fn merge(
    batch: &mut WriteBatch,
    db: &DbResource,
    key: Binary<'_>,
    operand: Binary<'_>,
    ttl_ms: Option<u64>,
) -> NifOutcome<()> {
    validate_key(key.as_slice())?;
    db.validate_operand(operand.as_slice())?;
    batch.merge_with_options(key.as_slice(), operand.as_slice(), &merge_options(ttl_ms));
    Ok(())
}

fn delete(batch: &mut WriteBatch, key: Binary<'_>) -> NifOutcome<()> {
    validate_key(key.as_slice())?;
    batch.delete(to_bytes(key));
    Ok(())
}

#[rustler::nif]
fn db_put<'a>(
    env: Env<'a>,
    db: ResourceArc<DbResource>,
    key: Binary<'a>,
    value: Binary<'a>,
    ttl_ms: Option<u64>,
    await_durable: bool,
    reply_ref: Term<'a>,
) -> Term<'a> {
    // A single put is a one-entry batch, which is what SlateDB's own `put`
    // does.
    let mut batch = WriteBatch::new();
    let batch = put(&mut batch, key, value, ttl_ms).map(|()| batch);
    spawn_write(env, reply_ref, db, batch, await_durable)
}

#[rustler::nif]
fn db_delete<'a>(
    env: Env<'a>,
    db: ResourceArc<DbResource>,
    key: Binary<'a>,
    await_durable: bool,
    reply_ref: Term<'a>,
) -> Term<'a> {
    let mut batch = WriteBatch::new();
    let batch = delete(&mut batch, key).map(|()| batch);
    spawn_write(env, reply_ref, db, batch, await_durable)
}

/// Writes a merge operand for `key`. The database's merge operator combines it
/// with the key's current value when the key is read or compacted.
#[rustler::nif]
fn db_merge<'a>(
    env: Env<'a>,
    db: ResourceArc<DbResource>,
    key: Binary<'a>,
    operand: Binary<'a>,
    ttl_ms: Option<u64>,
    await_durable: bool,
    reply_ref: Term<'a>,
) -> Term<'a> {
    let mut batch = WriteBatch::new();
    let batch = merge(&mut batch, &db, key, operand, ttl_ms).map(|()| batch);
    spawn_write(env, reply_ref, db, batch, await_durable)
}

/// Applies a list of `{:put, key, value}`, `{:put, key, value, ttl_ms}`,
/// `{:merge, key, operand}`, `{:merge, key, operand, ttl_ms}` and
/// `{:delete, key}` operations atomically.
#[rustler::nif]
fn db_write<'a>(
    env: Env<'a>,
    db: ResourceArc<DbResource>,
    ops: Vec<Term<'a>>,
    await_durable: bool,
    reply_ref: Term<'a>,
) -> Term<'a> {
    let batch = build_batch(ops, &db);
    spawn_write(env, reply_ref, db, batch, await_durable)
}

/// `db_write` on a dirty CPU scheduler. The Elixir side uses it for large
/// batches, because turning many terms into a `WriteBatch` can take longer
/// than a normal NIF should.
#[rustler::nif(schedule = "DirtyCpu")]
fn db_write_dirty<'a>(
    env: Env<'a>,
    db: ResourceArc<DbResource>,
    ops: Vec<Term<'a>>,
    await_durable: bool,
    reply_ref: Term<'a>,
) -> Term<'a> {
    let batch = build_batch(ops, &db);
    spawn_write(env, reply_ref, db, batch, await_durable)
}

fn build_batch(ops: Vec<Term<'_>>, db: &DbResource) -> NifOutcome<WriteBatch> {
    let mut batch = WriteBatch::new();
    for op in ops {
        let bad_op = || NifError::invalid(format!("invalid write batch operation: {op:?}"));
        let items = rustler::types::tuple::get_tuple(op).map_err(|_| bad_op())?;
        let tag: Atom = items
            .first()
            .and_then(|t| t.decode().ok())
            .ok_or_else(bad_op)?;
        let binary_at = |i: usize| -> NifOutcome<Binary<'_>> {
            items
                .get(i)
                .and_then(|t| t.decode().ok())
                .ok_or_else(bad_op)
        };
        let ttl_ms = match items.get(3) {
            Some(t) => Some(t.decode::<u64>().map_err(|_| bad_op())?),
            None => None,
        };
        match items.len() {
            3 | 4 if tag == atoms::put() => put(&mut batch, binary_at(1)?, binary_at(2)?, ttl_ms)?,
            3 | 4 if tag == atoms::merge() => {
                merge(&mut batch, db, binary_at(1)?, binary_at(2)?, ttl_ms)?
            }
            2 if tag == atoms::delete() => delete(&mut batch, binary_at(1)?)?,
            _ => return Err(bad_op()),
        }
    }
    Ok(batch)
}
