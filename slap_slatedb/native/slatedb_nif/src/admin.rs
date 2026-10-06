//! Checkpoints, clones and administration.
//!
//! An `Admin` works on a database's files in object storage. It does not open
//! the database, so it can run while a writer has it open, in the same VM or
//! elsewhere.

use std::time::Duration;

use rustler::types::atom::nil;
use rustler::{Encoder, Env, NifUnitEnum, Resource, ResourceArc, Term};
use slatedb::admin::Admin;
use slatedb::compactor::{CompactionSpec, SourceId};
use slatedb::config::{CheckpointOptions, CheckpointScope, GarbageCollectorOptions};
use slatedb::object_store::path::Path as StorePath;
use slatedb::{CheckpointCreateResult, CloneSourceSpec};

use crate::atoms;
use crate::db::DbResource;
use crate::options::{checkpoint_id, with_overrides};
use crate::reply::{ok_atom, ok_with, spawn_reply, spawn_reply_with, Encode, NifError, NifOutcome};
use crate::store::{join_path, open_store_with_prefix, StoreSpec};

pub(crate) struct AdminResource {
    admin: Admin,
    /// The database path, including any prefix from the store URL.
    path: String,
    /// The store URL's path prefix, for resolving clone paths.
    prefix: String,
}

#[rustler::resource_impl]
impl Resource for AdminResource {}

#[rustler::nif]
fn admin_open<'a>(env: Env<'a>, path: String, store: StoreSpec, reply_ref: Term<'a>) -> Term<'a> {
    spawn_reply(env, reply_ref, async move {
        let (object_store, prefix) = open_store_with_prefix(store).await?;
        let path = join_path(&prefix, &path);
        let admin = Admin::builder(StorePath::from(path.as_str()), object_store).build();
        Ok(ok_with(ResourceArc::new(AdminResource {
            admin,
            path,
            prefix,
        })))
    })
}

fn checkpoint_options(
    lifetime_ms: Option<u64>,
    source: Option<String>,
    name: Option<String>,
) -> NifOutcome<CheckpointOptions> {
    Ok(CheckpointOptions {
        lifetime: lifetime_ms.map(Duration::from_millis),
        source: source.as_deref().map(checkpoint_id).transpose()?,
        name,
    })
}

/// Replies `{:ok, {id, manifest_id}}`.
fn encode_created(created: CheckpointCreateResult) -> Encode {
    Box::new(move |env| (atoms::ok(), (created.id.to_string(), created.manifest_id)).encode(env))
}

#[derive(NifUnitEnum)]
enum Scope {
    All,
    Durable,
}

/// Creates a checkpoint of an open database. With scope `:all`, writes still
/// in memory are flushed first and included. With `:durable`, the checkpoint
/// covers only what is already durable.
#[rustler::nif]
fn db_create_checkpoint<'a>(
    env: Env<'a>,
    db: ResourceArc<DbResource>,
    scope: Scope,
    lifetime_ms: Option<u64>,
    source: Option<String>,
    name: Option<String>,
    reply_ref: Term<'a>,
) -> Term<'a> {
    let scope = match scope {
        Scope::All => CheckpointScope::All,
        Scope::Durable => CheckpointScope::Durable,
    };
    let options = checkpoint_options(lifetime_ms, source, name);
    spawn_reply_with(env, reply_ref, options, |options| async move {
        let _open = db.enter().await?;
        Ok(encode_created(
            db.db.create_checkpoint(scope, &options).await?,
        ))
    })
}

/// Creates a checkpoint of the latest manifest, without an open database.
#[rustler::nif]
fn admin_create_checkpoint<'a>(
    env: Env<'a>,
    admin: ResourceArc<AdminResource>,
    lifetime_ms: Option<u64>,
    source: Option<String>,
    name: Option<String>,
    reply_ref: Term<'a>,
) -> Term<'a> {
    let options = checkpoint_options(lifetime_ms, source, name);
    spawn_reply_with(env, reply_ref, options, |options| async move {
        Ok(encode_created(
            admin.admin.create_detached_checkpoint(&options).await?,
        ))
    })
}

/// Replies `{:ok, [{id, manifest_id, create_ms, expire_ms | nil, name | nil}]}`.
#[rustler::nif]
fn admin_list_checkpoints<'a>(
    env: Env<'a>,
    admin: ResourceArc<AdminResource>,
    name: Option<String>,
    reply_ref: Term<'a>,
) -> Term<'a> {
    spawn_reply(env, reply_ref, async move {
        let checkpoints = admin.admin.list_checkpoints(name.as_deref()).await?;
        let encode: Encode = Box::new(move |env| {
            let rows: Vec<Term<'_>> = checkpoints
                .iter()
                .map(|c| {
                    (
                        c.id.to_string(),
                        c.manifest_id,
                        c.create_time.timestamp_millis(),
                        c.expire_time.map(|t| t.timestamp_millis()),
                        c.name.as_deref(),
                    )
                        .encode(env)
                })
                .collect();
            (atoms::ok(), rows).encode(env)
        });
        Ok(encode)
    })
}

/// Sets a checkpoint to expire `lifetime_ms` from now, or never.
#[rustler::nif]
fn admin_refresh_checkpoint<'a>(
    env: Env<'a>,
    admin: ResourceArc<AdminResource>,
    id: String,
    lifetime_ms: Option<u64>,
    reply_ref: Term<'a>,
) -> Term<'a> {
    spawn_reply_with(env, reply_ref, checkpoint_id(&id), |id| async move {
        admin
            .admin
            .refresh_checkpoint(id, lifetime_ms.map(Duration::from_millis))
            .await?;
        Ok(ok_atom())
    })
}

#[rustler::nif]
fn admin_delete_checkpoint<'a>(
    env: Env<'a>,
    admin: ResourceArc<AdminResource>,
    id: String,
    reply_ref: Term<'a>,
) -> Term<'a> {
    spawn_reply_with(env, reply_ref, checkpoint_id(&id), |id| async move {
        admin.admin.delete_checkpoint(id).await?;
        Ok(ok_atom())
    })
}

/// Runs the garbage collector once. `options_json` is merged over SlateDB's
/// default `GarbageCollectorOptions`.
#[rustler::nif]
fn admin_run_gc<'a>(
    env: Env<'a>,
    admin: ResourceArc<AdminResource>,
    options_json: Option<String>,
    reply_ref: Term<'a>,
) -> Term<'a> {
    let options =
        with_overrides::<GarbageCollectorOptions>(options_json, "garbage collector options");
    spawn_reply_with(env, reply_ref, options, |options| async move {
        admin.admin.run_gc_once(options).await?;
        Ok(ok_atom())
    })
}

/// Creates the database at `clone_path` (in the same store, under the same
/// URL prefix) as a clone of this admin's database, as of `checkpoint` or its
/// latest state. The clone shares the source's files instead of copying them.
///
/// The clone is built from this admin, so it uses this admin's store
/// instance. That matters for `:memory` stores, where a new store would be
/// empty.
#[rustler::nif]
fn admin_clone<'a>(
    env: Env<'a>,
    admin: ResourceArc<AdminResource>,
    clone_path: String,
    checkpoint: Option<String>,
    reply_ref: Term<'a>,
) -> Term<'a> {
    let source_path = admin.path.clone();
    let source = checkpoint
        .as_deref()
        .map(checkpoint_id)
        .transpose()
        .map(|id| match id {
            Some(id) => CloneSourceSpec::with_checkpoint(source_path, id),
            None => CloneSourceSpec::new(source_path),
        });
    let clone_path = StorePath::from(join_path(&admin.prefix, &clone_path));
    spawn_reply_with(env, reply_ref, source, |source| async move {
        admin
            .admin
            .create_clone_builder_from_source(source)
            .with_clone_path(clone_path)
            .build()
            .await?;
        Ok(ok_atom())
    })
}

/// Submits a compaction of every sorted run into the lowest-id one, the same
/// plan as SlateDB's own full compaction (which is not public). Like that,
/// it leaves L0 SSTs to the normal compaction schedule.
///
/// The database's compactor runs it. Replies `{:ok, compaction_id}`, or
/// `{:ok, nil}` when there are no sorted runs yet.
#[rustler::nif]
fn admin_compact<'a>(
    env: Env<'a>,
    admin: ResourceArc<AdminResource>,
    reply_ref: Term<'a>,
) -> Term<'a> {
    spawn_reply(env, reply_ref, async move {
        let Some(manifest) = admin.admin.read_manifest(None).await? else {
            return Ok(ok_with(nil()));
        };
        let runs: Vec<u32> = manifest.compacted().iter().map(|run| run.id).collect();
        let Some(destination) = runs.iter().copied().min() else {
            return Ok(ok_with(nil()));
        };
        let sources = runs.into_iter().map(SourceId::SortedRun).collect();
        let compaction = admin
            .admin
            .submit_compaction(CompactionSpec::new(sources, destination))
            .await?;
        Ok(ok_with(compaction.id().to_string()))
    })
}

/// Replies `{:ok, unix_ms | nil}`: when the write with sequence number `seq`
/// happened, as far as SlateDB's sequence tracker knows.
#[rustler::nif]
fn admin_timestamp_for_seq<'a>(
    env: Env<'a>,
    admin: ResourceArc<AdminResource>,
    seq: u64,
    round_up: bool,
    reply_ref: Term<'a>,
) -> Term<'a> {
    spawn_reply(env, reply_ref, async move {
        let ts = admin
            .admin
            .get_timestamp_for_sequence(seq, round_up)
            .await?
            .map(|t| t.timestamp_millis());
        Ok(ok_with(ts))
    })
}

/// Replies `{:ok, seq | nil}`: the sequence number of the write at `unix_ms`.
#[rustler::nif]
fn admin_seq_for_timestamp<'a>(
    env: Env<'a>,
    admin: ResourceArc<AdminResource>,
    unix_ms: i64,
    round_up: bool,
    reply_ref: Term<'a>,
) -> Term<'a> {
    let ts = chrono::DateTime::from_timestamp_millis(unix_ms)
        .ok_or_else(|| NifError::invalid("timestamp is out of range"));
    spawn_reply_with(env, reply_ref, ts, |ts| async move {
        Ok(ok_with(
            admin.admin.get_sequence_for_timestamp(ts, round_up).await?,
        ))
    })
}
