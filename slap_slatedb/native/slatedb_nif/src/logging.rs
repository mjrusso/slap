//! Forwards SlateDB's log records to an Elixir process.
//!
//! SlateDB logs through the `log` crate, and its `tracing` events also reach
//! `log` (the `tracing/log` feature). This module installs a `log::Log` that
//! puts each record on a bounded channel. One plain OS thread takes records
//! off the channel and sends them to the registered process as
//! `{:slap_slatedb_log, level, target, message, dropped}`.
//!
//! Logging never blocks SlateDB: when the channel is full, records are
//! dropped and counted, and the next message carries the count.
//! `send_and_clear` must not run on a scheduler thread, which is why a
//! separate thread does the sending.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::mpsc::{sync_channel, Receiver, SyncSender, TrySendError};
use std::sync::{Mutex, OnceLock};

use log::{Level, LevelFilter, Log, Metadata, Record};
use rustler::{Atom, Encoder, LocalPid, NifUnitEnum, OwnedEnv};

use crate::atoms;

const CHANNEL_CAPACITY: usize = 1024;

struct LogRecord {
    level: Level,
    target: String,
    message: String,
}

static SENDER: OnceLock<SyncSender<LogRecord>> = OnceLock::new();
static TARGET_PID: Mutex<Option<LocalPid>> = Mutex::new(None);
static DROPPED: AtomicU64 = AtomicU64::new(0);

struct Forwarder;

static FORWARDER: Forwarder = Forwarder;

impl Log for Forwarder {
    fn enabled(&self, metadata: &Metadata<'_>) -> bool {
        metadata.level() <= log::max_level()
    }

    fn log(&self, record: &Record<'_>) {
        if !self.enabled(record.metadata()) {
            return;
        }
        let Some(sender) = SENDER.get() else {
            return;
        };
        let record = LogRecord {
            level: record.level(),
            target: record.target().to_string(),
            message: record.args().to_string(),
        };
        if let Err(TrySendError::Full(_)) = sender.try_send(record) {
            DROPPED.fetch_add(1, Ordering::Relaxed);
        }
    }

    fn flush(&self) {}
}

/// A `Logger` level, or `:none`.
#[derive(NifUnitEnum, Clone, Copy)]
enum LogLevel {
    Debug,
    Info,
    Warning,
    Error,
    None,
}

impl From<Level> for LogLevel {
    fn from(level: Level) -> Self {
        match level {
            Level::Error => LogLevel::Error,
            Level::Warn => LogLevel::Warning,
            Level::Info => LogLevel::Info,
            Level::Debug | Level::Trace => LogLevel::Debug,
        }
    }
}

impl From<LogLevel> for LevelFilter {
    fn from(level: LogLevel) -> Self {
        match level {
            LogLevel::Debug => LevelFilter::Debug,
            LogLevel::Info => LevelFilter::Info,
            LogLevel::Warning => LevelFilter::Warn,
            LogLevel::Error => LevelFilter::Error,
            LogLevel::None => LevelFilter::Off,
        }
    }
}

fn forward(receiver: Receiver<LogRecord>) {
    let mut env = OwnedEnv::new();
    for record in receiver {
        let pid = *TARGET_PID.lock().expect("log target lock poisoned");
        let Some(pid) = pid else {
            continue;
        };
        let dropped = DROPPED.swap(0, Ordering::Relaxed);
        // If the process is gone there is nobody to tell; wait for a new one.
        let _ = env.send_and_clear(&pid, |env| {
            (
                atoms::slap_slatedb_log(),
                LogLevel::from(record.level),
                record.target.as_str(),
                record.message.as_str(),
                dropped,
            )
                .encode(env)
        });
    }
}

/// Sends SlateDB's log records at `level` or above to `pid`. Calling it again
/// replaces the process and level.
#[rustler::nif]
fn log_init(pid: LocalPid, level: LogLevel) -> Atom {
    *TARGET_PID.lock().expect("log target lock poisoned") = Some(pid);
    SENDER.get_or_init(|| {
        let (sender, receiver) = sync_channel(CHANNEL_CAPACITY);
        std::thread::Builder::new()
            .name("slap-slatedb-log".into())
            .spawn(move || forward(receiver))
            .expect("failed to start the log forwarding thread");
        // Fails only if another logger is already installed in this OS
        // process. Records then go to that logger instead.
        let _ = log::set_logger(&FORWARDER);
        sender
    });
    log::set_max_level(level.into());
    atoms::ok()
}

/// Changes the lowest level that is forwarded. `:none` turns logging off.
#[rustler::nif]
fn log_set_level(level: LogLevel) -> Atom {
    log::set_max_level(level.into());
    atoms::ok()
}
