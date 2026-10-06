//! Decoding options from Elixir. `Slap.SlateDB.Options` checks them first.

use rustler::{NifMap, NifTaggedEnum, NifUnitEnum, ResourceArc};
use slatedb::config::{DurabilityLevel, ReadOptions, ScanOptions};
use slatedb::IterationOrder;
use uuid::Uuid;

use crate::db::CacheResource;
use crate::reply::{NifError, NifOutcome};

#[derive(NifUnitEnum, Clone, Copy)]
pub(crate) enum Durability {
    Memory,
    Remote,
}

impl From<Durability> for DurabilityLevel {
    fn from(durability: Durability) -> Self {
        match durability {
            Durability::Memory => DurabilityLevel::Memory,
            Durability::Remote => DurabilityLevel::Remote,
        }
    }
}

#[derive(NifUnitEnum, Clone, Copy)]
pub(crate) enum Order {
    Asc,
    Desc,
}

/// Read options for point reads. `nil` fields keep SlateDB's default.
#[derive(NifMap)]
pub(crate) struct ReadOpts {
    durability: Option<Durability>,
    dirty: Option<bool>,
    cache_blocks: Option<bool>,
}

impl ReadOpts {
    pub(crate) fn build(&self) -> ReadOptions {
        let mut opts = ReadOptions::default();
        if let Some(durability) = self.durability {
            opts.durability_filter = durability.into();
        }
        if let Some(dirty) = self.dirty {
            opts.dirty = dirty;
        }
        if let Some(cache_blocks) = self.cache_blocks {
            opts.cache_blocks = cache_blocks;
        }
        opts
    }
}

/// Options for scans. `nil` fields keep SlateDB's default.
#[derive(NifMap)]
pub(crate) struct ScanOpts {
    durability: Option<Durability>,
    dirty: Option<bool>,
    cache_blocks: Option<bool>,
    read_ahead_bytes: Option<usize>,
    max_fetch_tasks: Option<usize>,
    order: Option<Order>,
}

impl ScanOpts {
    pub(crate) fn build(&self) -> ScanOptions {
        let mut opts = ScanOptions::default();
        if let Some(durability) = self.durability {
            opts.durability_filter = durability.into();
        }
        if let Some(dirty) = self.dirty {
            opts.dirty = dirty;
        }
        if let Some(cache_blocks) = self.cache_blocks {
            opts.cache_blocks = cache_blocks;
        }
        if let Some(bytes) = self.read_ahead_bytes {
            opts.read_ahead_bytes = bytes;
        }
        if let Some(tasks) = self.max_fetch_tasks {
            opts.max_fetch_tasks = tasks;
        }
        if let Some(order) = self.order {
            opts.order = match order {
                Order::Asc => IterationOrder::Ascending,
                Order::Desc => IterationOrder::Descending,
            };
        }
        opts
    }
}

/// How a database or reader gets its block and metadata cache: `:default`
/// (SlateDB's private in-memory cache), `:disabled` or `{:shared, cache}`.
#[derive(NifTaggedEnum)]
pub(crate) enum CacheChoice {
    Default,
    Disabled,
    Shared(ResourceArc<CacheResource>),
}

/// Starts from `T::default()` and deep-merges the caller's JSON object over
/// it, so callers only pass the fields they want to change. Used for
/// `Settings`, `DbReaderOptions` and `GarbageCollectorOptions`.
pub(crate) fn with_overrides<T>(json: Option<String>, what: &str) -> NifOutcome<T>
where
    T: Default + serde::Serialize + serde::de::DeserializeOwned,
{
    let Some(json) = json else {
        return Ok(T::default());
    };
    let overrides: serde_json::Value = serde_json::from_str(&json)
        .map_err(|e| NifError::invalid(format!("invalid {what} JSON: {e}")))?;
    let mut merged = serde_json::to_value(T::default())
        .map_err(|e| NifError::internal(format!("cannot encode default {what}: {e}")))?;
    merge_json(&mut merged, overrides);
    serde_json::from_value(merged).map_err(|e| NifError::invalid(format!("invalid {what}: {e}")))
}

fn merge_json(base: &mut serde_json::Value, overrides: serde_json::Value) {
    match (base, overrides) {
        (serde_json::Value::Object(base), serde_json::Value::Object(overrides)) => {
            for (key, value) in overrides {
                match base.get_mut(&key) {
                    Some(slot) => merge_json(slot, value),
                    None => {
                        base.insert(key, value);
                    }
                }
            }
        }
        (slot, value) => *slot = value,
    }
}

pub(crate) fn checkpoint_id(id: &str) -> NifOutcome<Uuid> {
    Uuid::parse_str(id).map_err(|e| NifError::invalid(format!("invalid checkpoint id {id:?}: {e}")))
}
