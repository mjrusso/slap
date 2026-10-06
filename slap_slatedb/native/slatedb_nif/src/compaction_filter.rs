//! A built-in compaction filter that deletes every key under a set of
//! prefixes, with the set kept up to date from Elixir.
//!
//! The use case is deleting a large key range cheaply, such as everything
//! stored for a deleted stream: add its prefix to the set, and compactions
//! remove its entries as they rewrite them.
//!
//! Matching entries become tombstones rather than being dropped. A dropped
//! entry could uncover an older version of the key in a sorted run that this
//! compaction does not include, bringing it back. A tombstone hides older
//! versions, and SlateDB removes it once it reaches the last sorted run.
//!
//! The filter only acts on data as it is compacted. Until then, reads still
//! see the keys. Each compaction job takes the set as it is when the job
//! starts.

use std::collections::BTreeSet;
use std::ops::Bound;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, RwLock};

use async_trait::async_trait;
use rustler::{Binary, Encoder, Env, Resource, ResourceArc, Term};
use slatedb::bytes::Bytes;
use slatedb::{
    CompactionFilter, CompactionFilterDecision, CompactionFilterError, CompactionFilterSupplier,
    CompactionJobContext, RowEntry, ValueDeletable,
};

use crate::binaries::bin;

/// The prefixes, as the user gave them, and the same set reduced so that no
/// prefix is a prefix of another. Matching uses the reduced set.
#[derive(Default)]
struct PrefixSet {
    all: BTreeSet<Bytes>,
    reduced: Arc<BTreeSet<Bytes>>,
}

impl PrefixSet {
    fn rebuild(&mut self) {
        let mut reduced: BTreeSet<Bytes> = BTreeSet::new();
        // In sorted order, a prefix comes right before the keys it covers, so
        // it is enough to compare each entry with the last one kept.
        for prefix in &self.all {
            match reduced.iter().next_back() {
                Some(kept) if prefix.starts_with(kept) => {}
                _ => {
                    reduced.insert(prefix.clone());
                }
            }
        }
        self.reduced = Arc::new(reduced);
    }
}

/// Returns true if `key` starts with one of `prefixes`.
///
/// `prefixes` must be reduced (no entry is a prefix of another). Then the only
/// candidate is the largest prefix that is not greater than `key`: any prefix
/// between that one and the key would have to start with it.
fn matches(prefixes: &BTreeSet<Bytes>, key: &[u8]) -> bool {
    prefixes
        .range::<[u8], _>((Bound::Unbounded, Bound::Included(key)))
        .next_back()
        .is_some_and(|prefix| key.starts_with(prefix))
}

pub(crate) struct PrefixFilterResource {
    set: RwLock<PrefixSet>,
    /// Entries turned into tombstones, over all compactions.
    tombstoned: Arc<AtomicU64>,
}

#[rustler::resource_impl]
impl Resource for PrefixFilterResource {}

impl PrefixFilterResource {
    fn snapshot(&self) -> Arc<BTreeSet<Bytes>> {
        self.set
            .read()
            .expect("prefix set lock poisoned")
            .reduced
            .clone()
    }
}

/// The supplier SlateDB's compactor calls at the start of each job.
pub(crate) struct Supplier(pub(crate) ResourceArc<PrefixFilterResource>);

#[async_trait]
impl CompactionFilterSupplier for Supplier {
    async fn create_compaction_filter(
        &self,
        _context: &CompactionJobContext,
    ) -> Result<Box<dyn CompactionFilter>, CompactionFilterError> {
        Ok(Box::new(Filter {
            prefixes: self.0.snapshot(),
            tombstoned: self.0.tombstoned.clone(),
            count: 0,
        }))
    }
}

struct Filter {
    prefixes: Arc<BTreeSet<Bytes>>,
    tombstoned: Arc<AtomicU64>,
    count: u64,
}

#[async_trait]
impl CompactionFilter for Filter {
    async fn filter(
        &mut self,
        entry: &RowEntry,
    ) -> Result<CompactionFilterDecision, CompactionFilterError> {
        if matches!(entry.value, ValueDeletable::Tombstone) || !matches(&self.prefixes, &entry.key)
        {
            return Ok(CompactionFilterDecision::Keep);
        }
        self.count += 1;
        Ok(CompactionFilterDecision::Modify(ValueDeletable::Tombstone))
    }

    async fn on_compaction_end(&mut self) -> Result<(), CompactionFilterError> {
        self.tombstoned.fetch_add(self.count, Ordering::Relaxed);
        Ok(())
    }
}

fn to_set(prefixes: Vec<Binary<'_>>) -> Vec<Bytes> {
    prefixes
        .into_iter()
        .map(|p| Bytes::copy_from_slice(p.as_slice()))
        .collect()
}

#[rustler::nif]
fn prefix_filter_new(prefixes: Vec<Binary<'_>>) -> ResourceArc<PrefixFilterResource> {
    let mut set = PrefixSet {
        all: to_set(prefixes).into_iter().collect(),
        ..PrefixSet::default()
    };
    set.rebuild();
    ResourceArc::new(PrefixFilterResource {
        set: RwLock::new(set),
        tombstoned: Arc::new(AtomicU64::new(0)),
    })
}

/// Adds `add` to the set and removes `remove` from it.
#[rustler::nif]
fn prefix_filter_update(
    filter: ResourceArc<PrefixFilterResource>,
    add: Vec<Binary<'_>>,
    remove: Vec<Binary<'_>>,
) -> rustler::Atom {
    let add = to_set(add);
    let remove = to_set(remove);
    let mut set = filter.set.write().expect("prefix set lock poisoned");
    for prefix in remove {
        set.all.remove(&prefix);
    }
    set.all.extend(add);
    set.rebuild();
    crate::atoms::ok()
}

/// Returns `{prefixes, tombstoned}`.
#[rustler::nif]
fn prefix_filter_info<'a>(env: Env<'a>, filter: ResourceArc<PrefixFilterResource>) -> Term<'a> {
    let set = filter.set.read().expect("prefix set lock poisoned");
    let prefixes: Vec<Term<'a>> = set.all.iter().map(|p| bin(env, p)).collect();
    (prefixes, filter.tombstoned.load(Ordering::Relaxed)).encode(env)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn set(prefixes: &[&str]) -> BTreeSet<Bytes> {
        let mut s = PrefixSet {
            all: prefixes
                .iter()
                .map(|p| Bytes::copy_from_slice(p.as_bytes()))
                .collect(),
            ..PrefixSet::default()
        };
        s.rebuild();
        (*s.reduced).clone()
    }

    #[test]
    fn nested_prefixes_are_reduced_and_still_match() {
        let s = set(&["a", "ab0", "b", "stream/7/"]);
        assert_eq!(s.len(), 3);
        for key in ["a", "ab5", "az", "b", "bz", "stream/7/x"] {
            assert!(matches(&s, key.as_bytes()), "{key}");
        }
        for key in ["", "0", "c", "stream/", "stream/8/x", "stream/70"] {
            assert!(!matches(&s, key.as_bytes()), "{key}");
        }
    }
}
