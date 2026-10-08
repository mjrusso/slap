//! Opening, closing and inspecting a database.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use rustler::types::atom::nil;
use rustler::{Encoder, Env, NifUnitEnum, Resource, ResourceArc, Term};
use slatedb::config::{FlushOptions, FlushType, Settings};
use slatedb::db_cache::moka::{MokaCache, MokaCacheOptions};
use slatedb::db_cache::DbCache;
use slatedb::object_store::path::Path as StorePath;
use slatedb::CompactorBuilder;
use slatedb_common::metrics::{DefaultMetricsRecorder, MetricValue};
use tokio::sync::RwLockReadGuard;

use crate::atoms;
use crate::compaction_filter::{self, PrefixFilterResource};
use crate::gate::Gate;
use crate::merge::Builtin;
use crate::options::{with_overrides, CacheChoice};
use crate::reply::{ok_atom, ok_with, spawn_reply, spawn_reply_with, NifError, NifOutcome};
use crate::store::{open_store, StoreSpec};

pub(crate) struct DbResource {
    pub(crate) db: slatedb::Db,
    pub(crate) gate: Gate,
    metrics: Arc<DefaultMetricsRecorder>,
    /// The highest sequence number written through this handle.
    last_write_seq: AtomicU64,
    merge_operator: Option<Builtin>,
}

impl DbResource {
    pub(crate) async fn enter(&self) -> NifOutcome<RwLockReadGuard<'_, bool>> {
        self.gate.enter().await
    }

    /// Checks a merge operand against the database's merge operator.
    pub(crate) fn validate_operand(&self, operand: &[u8]) -> NifOutcome<()> {
        match self.merge_operator {
            Some(op) => op.validate_operand(operand),
            None => Err(NifError::invalid(
                "this database was opened without a :merge_operator",
            )),
        }
    }

    pub(crate) fn record_write(&self, seq: u64) {
        self.last_write_seq.fetch_max(seq, Ordering::Relaxed);
    }
}

#[rustler::resource_impl]
impl Resource for DbResource {
    fn destructor(self, _env: Env<'_>) {
        // Snapshots, transactions and iterators hold a reference to this
        // resource, so this runs only when none of them are left.
        let Self { db, gate, .. } = self;
        gate.close_dropped("database", "Slap.SlateDB.close/1", async move {
            db.close().await
        });
    }
}

pub(crate) struct SnapshotResource {
    pub(crate) db: ResourceArc<DbResource>,
    pub(crate) snapshot: Arc<slatedb::DbSnapshot>,
}

#[rustler::resource_impl]
impl Resource for SnapshotResource {}

/// A block and metadata cache that several databases can share.
pub(crate) struct CacheResource {
    pub(crate) cache: Arc<dyn DbCache>,
    next_id: AtomicU64,
}

impl CacheResource {
    /// A `db_cache_id` no other open of this cache has used. SlateDB requires
    /// a distinct one for each database that shares a cache.
    pub(crate) fn new_id(&self) -> u64 {
        self.next_id.fetch_add(1, Ordering::Relaxed)
    }
}

#[rustler::resource_impl]
impl Resource for CacheResource {}

#[rustler::nif]
#[allow(clippy::too_many_arguments)]
fn db_open<'a>(
    env: Env<'a>,
    path: String,
    store: StoreSpec,
    settings_json: Option<String>,
    cache: CacheChoice,
    merge_operator: Option<Builtin>,
    filter: Option<ResourceArc<PrefixFilterResource>>,
    reply_ref: Term<'a>,
) -> Term<'a> {
    let settings = parse_settings(settings_json, filter.is_some());
    spawn_reply_with(env, reply_ref, settings, |settings| async move {
        let (object_store, path) = open_store(store, path).await?;
        let path = StorePath::from(path);
        let metrics = Arc::new(DefaultMetricsRecorder::new());
        let compactor_options = settings.compactor_options.clone();
        let mut builder = slatedb::Db::builder(path.clone(), object_store.clone())
            .with_settings(settings)
            .with_metrics_recorder(metrics.clone());
        builder = match cache {
            CacheChoice::Default => builder,
            CacheChoice::Disabled => builder.with_db_cache_disabled(),
            CacheChoice::Shared(cache) => {
                builder.with_db_cache(cache.cache.clone(), cache.new_id())
            }
        };
        if let Some(op) = merge_operator {
            builder = builder.with_merge_operator(op.operator());
        }
        if let (Some(filter), Some(options)) = (filter, compactor_options) {
            // `DbBuilder` takes a compaction filter only through a compactor
            // builder. Passing the same store `Arc` lets the compactor share
            // the database's object store cache.
            let compactor = CompactorBuilder::new(path, object_store)
                .with_options(options)
                .with_compaction_filter_supplier(Arc::new(compaction_filter::Supplier(filter)));
            builder = builder.with_compactor_builder(compactor);
        }
        let db = builder.build().await?;
        Ok(ok_with(ResourceArc::new(DbResource {
            db,
            gate: Gate::new(),
            metrics,
            last_write_seq: AtomicU64::new(0),
            merge_operator,
        })))
    })
}

fn parse_settings(json: Option<String>, has_filter: bool) -> NifOutcome<Settings> {
    let settings = with_overrides::<Settings>(json, "settings")?;
    if has_filter && settings.compactor_options.is_none() {
        return Err(NifError::invalid(
            "a compaction_filter needs the compactor, but settings turn it off \
             (compactor_options is null)",
        ));
    }
    Ok(settings)
}

#[rustler::nif]
fn db_validate_settings<'a>(env: Env<'a>, settings_json: Option<String>) -> Term<'a> {
    match parse_settings(settings_json, false) {
        Ok(_) => atoms::ok().encode(env),
        Err(error) => error.encode(env),
    }
}

/// Closes the database. Waits for calls already in flight to finish. Calls
/// made after this starts fail with a `:closed` error. Closing again returns
/// `:ok`.
#[rustler::nif]
fn db_close<'a>(env: Env<'a>, db: ResourceArc<DbResource>, reply_ref: Term<'a>) -> Term<'a> {
    spawn_reply(env, reply_ref, async move {
        if db.gate.close().await {
            db.db.close().await?;
        }
        Ok(ok_atom())
    })
}

#[derive(NifUnitEnum)]
enum Flush {
    Wal,
    Memtable,
}

/// Flushes the WAL (`:wal`) or the memtable (`:memtable`) to object storage.
#[rustler::nif]
fn db_flush<'a>(
    env: Env<'a>,
    db: ResourceArc<DbResource>,
    flush: Flush,
    reply_ref: Term<'a>,
) -> Term<'a> {
    let flush_type = match flush {
        Flush::Wal => FlushType::Wal,
        Flush::Memtable => FlushType::MemTable,
    };
    spawn_reply(env, reply_ref, async move {
        let _open = db.enter().await?;
        db.db
            .flush_with_options(FlushOptions { flush_type })
            .await?;
        Ok(ok_atom())
    })
}

#[rustler::nif]
fn db_snapshot<'a>(env: Env<'a>, db: ResourceArc<DbResource>, reply_ref: Term<'a>) -> Term<'a> {
    spawn_reply(env, reply_ref, async move {
        let snapshot = {
            let _open = db.enter().await?;
            db.db.snapshot().await?
        };
        Ok(ok_with(ResourceArc::new(SnapshotResource { db, snapshot })))
    })
}

/// Creates an in-memory block and metadata cache of `capacity_bytes`, for
/// sharing between databases. SlateDB keeps each database's entries apart.
///
/// Returns an encoded term because `dyn DbCache` is not `RefUnwindSafe`, which
/// Rustler requires of a returned `ResourceArc`.
#[rustler::nif]
fn cache_new(env: Env<'_>, capacity_bytes: u64) -> Term<'_> {
    let cache = MokaCache::new_with_opts(MokaCacheOptions {
        max_capacity: capacity_bytes,
        time_to_live: None,
        time_to_idle: None,
    });
    ResourceArc::new(CacheResource {
        cache: Arc::new(cache),
        next_id: AtomicU64::new(0),
    })
    .encode(env)
}

/// Returns the highest sequence number that is durable in object storage.
#[rustler::nif]
fn db_durable_seq(db: ResourceArc<DbResource>) -> u64 {
    db.db.subscribe().borrow().durable_seq
}

/// Returns `{durable_seq, last_write_seq, l0_sst_count, sorted_run_count}`.
/// `last_write_seq` covers writes made through this handle.
#[rustler::nif]
fn db_stats(db: ResourceArc<DbResource>) -> (u64, u64, usize, usize) {
    let status = db.db.subscribe();
    let status = status.borrow();
    (
        status.durable_seq,
        db.last_write_seq.load(Ordering::Relaxed),
        status.current_manifest.l0().len(),
        status.current_manifest.compacted().len(),
    )
}

/// Returns cache hits and misses without encoding every registered metric.
#[rustler::nif]
fn db_cache_stats(db: ResourceArc<DbResource>) -> (u64, u64) {
    let snapshot = db.metrics.snapshot();
    let mut hits = 0;
    let mut misses = 0;

    for metric in snapshot
        .all()
        .iter()
        .filter(|metric| metric.name == "slatedb.db_cache.access_count")
    {
        if let MetricValue::Counter(value) = &metric.value {
            match metric.labels.iter().find(|(key, _)| key == "result") {
                Some((_, result)) if result == "hit" => hits += *value,
                Some((_, result)) if result == "miss" => misses += *value,
                _ => {}
            }
        }
    }

    (hits, misses)
}

/// A float, or `None` (`nil`) when it is not finite. An empty histogram has
/// infinite `min` and `max`, and the BEAM has no infinite floats.
fn finite(value: f64) -> Option<f64> {
    value.is_finite().then_some(value)
}

/// Returns every metric SlateDB has registered for this database, as
/// `{name, [{label, value}], value}`. A histogram's value is
/// `{:histogram, count, sum, min, max, boundaries, bucket_counts}`.
#[rustler::nif]
fn db_metrics(env: Env<'_>, db: ResourceArc<DbResource>) -> Term<'_> {
    let snapshot = db.metrics.snapshot();
    let metrics: Vec<Term<'_>> = snapshot
        .all()
        .iter()
        .map(|metric| {
            let value = match &metric.value {
                MetricValue::Counter(v) => v.encode(env),
                MetricValue::Gauge(v) | MetricValue::UpDownCounter(v) => v.encode(env),
                MetricValue::Histogram {
                    count,
                    sum,
                    min,
                    max,
                    boundaries,
                    bucket_counts,
                } => (
                    atoms::histogram(),
                    *count,
                    finite(*sum),
                    finite(*min),
                    finite(*max),
                    boundaries.iter().map(|b| finite(*b)).collect::<Vec<_>>(),
                    bucket_counts.clone(),
                )
                    .encode(env),
                #[allow(unreachable_patterns)]
                _ => nil().encode(env),
            };
            (metric.name.as_str(), metric.labels.clone(), value).encode(env)
        })
        .collect();
    metrics.encode(env)
}
