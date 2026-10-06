//! Read-only access to a database with `DbReader`. Reads go through `read`.
//!
//! A reader does not take part in fencing, so it can run alongside the
//! writer, in the same VM or elsewhere. It follows the database's manifest
//! (and WAL) as the writer changes it.

use rustler::{Env, NifUnitEnum, Resource, ResourceArc, Term};
use slatedb::config::DbReaderOptions;
use slatedb::object_store::path::Path as StorePath;
use slatedb::DbReaderMode;

use crate::gate::Gate;
use crate::merge::Builtin;
use crate::options::{checkpoint_id, with_overrides, CacheChoice};
use crate::reply::{ok_atom, ok_with, spawn_reply, spawn_reply_with, NifOutcome};
use crate::store::{open_store, StoreSpec};

pub(crate) struct ReaderResource {
    pub(crate) reader: slatedb::DbReader,
    pub(crate) gate: Gate,
}

#[rustler::resource_impl]
impl Resource for ReaderResource {
    fn destructor(self, _env: Env<'_>) {
        let Self { reader, gate } = self;
        gate.close_dropped("reader", "Slap.SlateDB.Reader.close/1", async move {
            reader.close().await
        });
    }
}

#[derive(NifUnitEnum)]
enum Mode {
    Managed,
    Latest,
}

fn reader_mode(mode: Mode, checkpoint: Option<String>) -> NifOutcome<DbReaderMode> {
    match (checkpoint, mode) {
        (Some(id), _) => Ok(DbReaderMode::Checkpoint(checkpoint_id(&id)?)),
        (None, Mode::Managed) => Ok(DbReaderMode::ManagedCheckpoint),
        (None, Mode::Latest) => Ok(DbReaderMode::FollowLatest),
    }
}

/// Opens a reader. `mode` is `:managed` (the reader keeps its own checkpoint
/// so garbage collection cannot remove what it reads) or `:latest` (no
/// checkpoint, no writes to the store). A `checkpoint` id pins the reader to
/// that checkpoint instead.
#[rustler::nif]
#[allow(clippy::too_many_arguments)]
fn reader_open<'a>(
    env: Env<'a>,
    path: String,
    store: StoreSpec,
    options_json: Option<String>,
    mode: Mode,
    checkpoint: Option<String>,
    cache: CacheChoice,
    merge_operator: Option<Builtin>,
    reply_ref: Term<'a>,
) -> Term<'a> {
    let args = with_overrides::<DbReaderOptions>(options_json, "reader options")
        .and_then(|options| Ok((options, reader_mode(mode, checkpoint)?)));
    spawn_reply_with(env, reply_ref, args, |(options, mode)| async move {
        let (object_store, path) = open_store(store, path).await?;
        let mut builder = slatedb::DbReader::builder(StorePath::from(path), object_store)
            .with_options(options)
            .with_reader_mode(mode);
        builder = match cache {
            CacheChoice::Default => builder,
            CacheChoice::Disabled => builder.with_db_cache_disabled(),
            CacheChoice::Shared(cache) => builder.with_db_cache(cache.cache.clone()),
        };
        if let Some(op) = merge_operator {
            builder = builder.with_merge_operator(op.operator());
        }
        let reader = builder.build().await?;
        Ok(ok_with(ResourceArc::new(ReaderResource {
            reader,
            gate: Gate::new(),
        })))
    })
}

#[rustler::nif]
fn reader_close<'a>(
    env: Env<'a>,
    reader: ResourceArc<ReaderResource>,
    reply_ref: Term<'a>,
) -> Term<'a> {
    spawn_reply(env, reply_ref, async move {
        if reader.gate.close().await {
            reader.reader.close().await?;
        }
        Ok(ok_atom())
    })
}

/// Returns the highest sequence number the reader has seen as durable.
#[rustler::nif]
fn reader_durable_seq(reader: ResourceArc<ReaderResource>) -> u64 {
    reader.reader.subscribe().borrow().durable_seq
}
