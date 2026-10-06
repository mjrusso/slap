//! Reads and scans, on any handle that can read: a database, a snapshot, a
//! transaction or a reader.

use std::ops::Bound;

use rustler::{Binary, Decoder, Encoder, NifResult, Resource, ResourceArc, Term};
use slatedb::bytes::Bytes;
use tokio::sync::Mutex;

use crate::atoms;
use crate::binaries::{bin, to_bytes};
use crate::db::{DbResource, SnapshotResource};
use crate::gate::Gate;
use crate::options::{ReadOpts, ScanOpts};
use crate::reader::ReaderResource;
use crate::reply::{ok_atom, ok_with, spawn_reply, spawn_reply_with, Encode, NifError, NifOutcome};
use crate::transaction::{tx_completed, TransactionResource};

/// A handle that can read. Decodes from any of the four resources.
pub(crate) enum ReadTarget {
    Db(ResourceArc<DbResource>),
    Snapshot(ResourceArc<SnapshotResource>),
    Transaction(ResourceArc<TransactionResource>),
    Reader(ResourceArc<ReaderResource>),
}

impl<'a> Decoder<'a> for ReadTarget {
    fn decode(term: Term<'a>) -> NifResult<Self> {
        if let Ok(db) = term.decode() {
            Ok(Self::Db(db))
        } else if let Ok(snapshot) = term.decode() {
            Ok(Self::Snapshot(snapshot))
        } else if let Ok(tx) = term.decode() {
            Ok(Self::Transaction(tx))
        } else {
            term.decode().map(Self::Reader)
        }
    }
}

impl ReadTarget {
    /// The gate of the database or reader the target reads from.
    fn gate(&self) -> &Gate {
        match self {
            Self::Db(db) => &db.gate,
            Self::Snapshot(snapshot) => &snapshot.db.gate,
            Self::Transaction(tx) => &tx.db.gate,
            Self::Reader(reader) => &reader.gate,
        }
    }
}

/// Evaluates `$body` with `$handle` bound to the target's SlateDB handle
/// (`Db`, `DbSnapshot`, `DbTransaction` or `DbReader`), while the target's
/// database or reader is open. The four types share their read methods
/// (`slatedb::DbReadOps`), but those are generic, so each needs its own arm.
macro_rules! with_handle {
    ($target:expr, |$handle:ident| $body:expr) => {{
        let target: &ReadTarget = &$target;
        let _open = target.gate().enter().await?;
        match target {
            ReadTarget::Db(db) => {
                let $handle = &db.db;
                $body
            }
            ReadTarget::Snapshot(snapshot) => {
                let $handle = snapshot.snapshot.as_ref();
                $body
            }
            ReadTarget::Transaction(tx) => {
                let guard = tx.tx.read().await;
                let $handle = guard.as_ref().ok_or_else(tx_completed)?;
                $body
            }
            ReadTarget::Reader(reader) => {
                let $handle = &reader.reader;
                $body
            }
        }
    }};
}

pub(crate) fn validate_key(key: &[u8]) -> NifOutcome<()> {
    // SlateDB panics on an empty key, so reject it before it gets there.
    if key.is_empty() {
        return Err(NifError::invalid("key cannot be empty"));
    }
    // SlateDB also panics on keys longer than u16::MAX bytes.
    if key.len() > usize::from(u16::MAX) {
        return Err(NifError::invalid("key is longer than 65535 bytes"));
    }
    Ok(())
}

fn key_arg(key: Binary<'_>) -> NifOutcome<Bytes> {
    validate_key(key.as_slice())?;
    Ok(to_bytes(key))
}

/// Replies `{:ok, value | nil}`.
#[rustler::nif]
fn read_get<'a>(
    env: rustler::Env<'a>,
    target: ReadTarget,
    key: Binary<'a>,
    opts: ReadOpts,
    reply_ref: Term<'a>,
) -> Term<'a> {
    let opts = opts.build();
    spawn_reply_with(env, reply_ref, key_arg(key), |key| async move {
        let value = with_handle!(target, |handle| handle.get_with_options(key, &opts).await?);
        let encode: Encode =
            Box::new(move |env| (atoms::ok(), value.map(|v| bin(env, &v))).encode(env));
        Ok(encode)
    })
}

/// Replies `{:ok, {key, value, seq, create_ts, expire_ts | nil}}` or
/// `{:ok, nil}`.
#[rustler::nif]
fn read_get_key_value<'a>(
    env: rustler::Env<'a>,
    target: ReadTarget,
    key: Binary<'a>,
    opts: ReadOpts,
    reply_ref: Term<'a>,
) -> Term<'a> {
    let opts = opts.build();
    spawn_reply_with(env, reply_ref, key_arg(key), |key| async move {
        let transaction = matches!(&target, ReadTarget::Transaction(_));
        let row = with_handle!(target, |handle| {
            handle.get_key_value_with_options(key, &opts).await?
        });
        let encode: Encode = Box::new(move |env| {
            let row = row.map(|kv| {
                (
                    bin(env, &kv.key),
                    bin(env, &kv.value),
                    row_version(kv.seq, transaction),
                    kv.create_ts,
                    kv.expire_ts,
                )
            });
            (atoms::ok(), row).encode(env)
        });
        Ok(encode)
    })
}

type RangeArg<'a> = (Option<Binary<'a>>, bool, Option<Binary<'a>>, bool);

fn bound(key: Option<Binary<'_>>, inclusive: bool) -> Bound<Bytes> {
    match key {
        None => Bound::Unbounded,
        Some(key) if inclusive => Bound::Included(to_bytes(key)),
        Some(key) => Bound::Excluded(to_bytes(key)),
    }
}

pub(crate) struct IteratorResource {
    /// Holding the target keeps what it reads from open.
    target: ReadTarget,
    iter: Mutex<slatedb::DbIterator>,
}

fn row_version(seq: u64, transaction: bool) -> Option<u64> {
    // SlateDB marks a transaction's uncommitted writes with this sentinel.
    (seq != u64::MAX || !transaction).then_some(seq)
}

#[rustler::resource_impl]
impl Resource for IteratorResource {}

/// Opens an iterator over `range`, limited to keys starting with `prefix`
/// if it is given. Replies `{:ok, iterator}`.
#[rustler::nif]
fn read_scan<'a>(
    env: rustler::Env<'a>,
    target: ReadTarget,
    range: RangeArg<'a>,
    prefix: Option<Binary<'a>>,
    opts: ScanOpts,
    reply_ref: Term<'a>,
) -> Term<'a> {
    let range = (bound(range.0, range.1), bound(range.2, range.3));
    let prefix = prefix.map(to_bytes);
    let opts = opts.build();
    spawn_reply(env, reply_ref, async move {
        let iter = with_handle!(target, |handle| match prefix {
            Some(prefix) =>
                handle
                    .scan_prefix_with_options(prefix, range, &opts)
                    .await?,
            None => handle.scan_with_options(range, &opts).await?,
        });
        Ok(ok_with(ResourceArc::new(IteratorResource {
            target,
            iter: Mutex::new(iter),
        })))
    })
}

/// Returns up to `max` rows, optionally including each row's sequence number.
#[rustler::nif]
fn iterator_next_batch<'a>(
    env: rustler::Env<'a>,
    iter: ResourceArc<IteratorResource>,
    max: u32,
    with_versions: bool,
    reply_ref: Term<'a>,
) -> Term<'a> {
    spawn_reply(env, reply_ref, async move {
        let transaction = matches!(&iter.target, ReadTarget::Transaction(_));
        let _open = iter.target.gate().enter().await?;
        let mut guard = iter.iter.lock().await;
        let mut rows = Vec::with_capacity(max.min(1024) as usize);
        for _ in 0..max {
            match guard.next().await? {
                Some(kv) => rows.push((kv.key, kv.value, kv.seq)),
                None => break,
            }
        }
        let encode: Encode = Box::new(move |env| {
            let rows: Vec<Term<'_>> = rows
                .iter()
                .map(|(k, v, seq)| {
                    if with_versions {
                        (bin(env, k), bin(env, v), row_version(*seq, transaction)).encode(env)
                    } else {
                        (bin(env, k), bin(env, v)).encode(env)
                    }
                })
                .collect();
            (atoms::ok(), rows).encode(env)
        });
        Ok(encode)
    })
}

#[rustler::nif]
fn iterator_seek<'a>(
    env: rustler::Env<'a>,
    iter: ResourceArc<IteratorResource>,
    key: Binary<'a>,
    reply_ref: Term<'a>,
) -> Term<'a> {
    spawn_reply_with(env, reply_ref, key_arg(key), |key| async move {
        let _open = iter.target.gate().enter().await?;
        iter.iter.lock().await.seek(key).await?;
        Ok(ok_atom())
    })
}
