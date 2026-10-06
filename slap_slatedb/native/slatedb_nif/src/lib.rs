//! Rustler NIF for SlateDB.
//!
//! Every SlateDB call is async. A NIF must not block a BEAM scheduler, so the
//! I/O NIFs here do not wait for the result. Each one:
//!
//! 1. copies its arguments out of the calling process's heap (large binaries
//!    are shared instead; see `binaries`),
//! 2. spawns a future on a shared multi-threaded Tokio runtime, and
//! 3. returns `:ok` at once.
//!
//! When the future finishes, the result is sent to the calling process as
//! `{:slap_slatedb_reply, ref, result}`, where `ref` is a reference the caller
//! made (see `reply`). `Slap.SlateDB.Native.call/2` on the Elixir side waits
//! for that message.
//!
//! NIFs that only touch memory (transaction puts and deletes, for example) run
//! directly on the scheduler and return their result.
//!
//! Atom arguments decode into enums (`NifUnitEnum`); the Elixir side checks
//! options first, so a value that does not decode is a `badarg`.

mod admin;
mod binaries;
mod compaction_filter;
mod db;
mod gate;
mod logging;
mod merge;
mod objstore;
mod options;
mod probe;
mod read;
mod reader;
mod reply;
mod store;
mod subscription;
mod transaction;
mod write;

use std::sync::OnceLock;

use rustler::{Encoder, Env, Term};
use tokio::runtime::{Builder, Runtime};

mod atoms {
    rustler::atoms! {
        ok,
        error,
        slap_slatedb_reply,
        slap_slatedb_durable,
        slap_slatedb_closed,
        slap_slatedb_log,
        // store probe
        unsupported,
        skipped,
        failed,
        create,
        create_again,
        stale_if_match,
        current_if_match,
        // error kinds
        conflict,
        closed,
        unavailable,
        invalid,
        data,
        internal,
        // close reasons
        clean,
        fenced,
        panic,
        unknown,
        // write batch ops
        put,
        merge,
        delete,
        // metrics
        histogram,
        // object store
        eof,
    }
}

struct RuntimeState {
    threads: Option<usize>,
    runtime: Runtime,
}

static RUNTIME: OnceLock<RuntimeState> = OnceLock::new();

impl RuntimeState {
    fn new(threads: Option<usize>) -> Self {
        let mut builder = Builder::new_multi_thread();
        if let Some(threads) = threads {
            builder.worker_threads(threads);
        }
        let runtime = builder
            .enable_all()
            .thread_name("slap-slatedb-rt")
            .build()
            .expect("failed to build SlateDB Tokio runtime");
        Self { threads, runtime }
    }
}

#[rustler::nif(schedule = "DirtyCpu")]
fn runtime_init<'a>(env: Env<'a>, threads: Option<usize>) -> Term<'a> {
    if threads == Some(0) {
        return reply::NifError::invalid("runtime_threads must be positive").encode(env);
    }

    let state = RUNTIME.get_or_init(|| RuntimeState::new(threads));
    if state.threads == threads {
        atoms::ok().encode(env)
    } else {
        reply::NifError::invalid(
            "SlateDB runtime already has a different thread count; restart the VM to change it",
        )
        .encode(env)
    }
}

// SlateDB captures `Handle::current()` when a database opens and keeps using
// it for background work (WAL flush, memtable flush, compaction, GC). The
// runtime must live as long as the NIF library, so it is a process global.
fn runtime() -> &'static Runtime {
    &RUNTIME.get_or_init(|| RuntimeState::new(None)).runtime
}

rustler::init!("Elixir.Slap.SlateDB.Native");
