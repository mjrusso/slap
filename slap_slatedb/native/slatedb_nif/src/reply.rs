//! Errors, and replies to the calling process.

use std::future::Future;

use rustler::{Atom, Encoder, Env, LocalPid, OwnedEnv, Term};
use slatedb::{CloseReason, ErrorKind};

use crate::{atoms, runtime};

/// An error in a form that can cross into any `Env`.
///
/// It encodes as `{:error, {kind, close_reason | nil, message}}`.
pub(crate) struct NifError {
    kind: Atom,
    reason: Option<Atom>,
    message: String,
}

impl NifError {
    fn new(kind: Atom, message: impl Into<String>) -> Self {
        Self {
            kind,
            reason: None,
            message: message.into(),
        }
    }

    pub(crate) fn invalid(message: impl Into<String>) -> Self {
        Self::new(atoms::invalid(), message)
    }

    pub(crate) fn internal(message: impl Into<String>) -> Self {
        Self::new(atoms::internal(), message)
    }

    pub(crate) fn unavailable(message: impl Into<String>) -> Self {
        Self::new(atoms::unavailable(), message)
    }

    /// The error for calls on a database or reader that has been closed.
    pub(crate) fn closed() -> Self {
        Self {
            reason: Some(atoms::clean()),
            ..Self::new(atoms::closed(), "the database is closed")
        }
    }

    pub(crate) fn encode<'a>(&self, env: Env<'a>) -> Term<'a> {
        (
            atoms::error(),
            (self.kind, self.reason, self.message.as_str()),
        )
            .encode(env)
    }
}

pub(crate) fn close_reason_atom(reason: CloseReason) -> Atom {
    match reason {
        CloseReason::Clean => atoms::clean(),
        CloseReason::Fenced => atoms::fenced(),
        CloseReason::Panic => atoms::panic(),
        #[allow(unreachable_patterns)]
        _ => atoms::unknown(),
    }
}

impl From<slatedb::Error> for NifError {
    fn from(err: slatedb::Error) -> Self {
        let (kind, reason) = match err.kind() {
            ErrorKind::Transaction => (atoms::conflict(), None),
            ErrorKind::Closed(reason) => (atoms::closed(), Some(close_reason_atom(reason))),
            ErrorKind::Unavailable => (atoms::unavailable(), None),
            ErrorKind::Invalid => (atoms::invalid(), None),
            ErrorKind::Data => (atoms::data(), None),
            ErrorKind::Internal => (atoms::internal(), None),
            #[allow(unreachable_patterns)]
            _ => (atoms::internal(), None),
        };
        Self {
            kind,
            reason,
            message: err.to_string(),
        }
    }
}

pub(crate) type NifOutcome<T> = Result<T, NifError>;

/// Builds the success term for a reply inside the message environment.
pub(crate) type Encode = Box<dyn for<'a> FnOnce(Env<'a>) -> Term<'a> + Send>;

pub(crate) fn ok_atom() -> Encode {
    Box::new(|env| atoms::ok().encode(env))
}

/// Replies `{:ok, value}`. `None` encodes as `nil`.
pub(crate) fn ok_with<T: Encoder + Send + 'static>(value: T) -> Encode {
    Box::new(move |env| (atoms::ok(), value).encode(env))
}

/// Spawns `fut` on the runtime and sends its result to the calling process
/// as `{:slap_slatedb_reply, reply_ref, result}`. Returns `:ok` at once.
pub(crate) fn spawn_reply<'a, F>(env: Env<'a>, reply_ref: Term<'a>, fut: F) -> Term<'a>
where
    F: Future<Output = NifOutcome<Encode>> + Send + 'static,
{
    let pid = env.pid();
    let mut msg_env = OwnedEnv::new();
    let saved_ref = msg_env.save(reply_ref);

    runtime().spawn(async move {
        // Run the work as its own task so that a panic inside SlateDB turns
        // into an error reply instead of a caller that waits forever.
        let result = match runtime().spawn(fut).await {
            Ok(result) => result,
            Err(join_err) => Err(NifError::internal(format!(
                "SlateDB task failed: {join_err}"
            ))),
        };
        send_reply(&mut msg_env, &pid, saved_ref, result);
    });

    atoms::ok().encode(env)
}

/// Like `spawn_reply`, for a NIF that prepares its arguments first: if
/// `args` is an error, returns it at once (the Elixir side returns any value
/// other than `:ok` without waiting for a reply); otherwise spawns `f(args)`.
pub(crate) fn spawn_reply_with<'a, A, F, Fut>(
    env: Env<'a>,
    reply_ref: Term<'a>,
    args: NifOutcome<A>,
    f: F,
) -> Term<'a>
where
    F: FnOnce(A) -> Fut,
    Fut: Future<Output = NifOutcome<Encode>> + Send + 'static,
{
    match args {
        Ok(args) => spawn_reply(env, reply_ref, f(args)),
        Err(err) => err.encode(env),
    }
}

fn send_reply(
    msg_env: &mut OwnedEnv,
    pid: &LocalPid,
    saved_ref: rustler::env::SavedTerm,
    result: NifOutcome<Encode>,
) {
    // If the caller has exited there is nobody to tell, so a send error is
    // ignored.
    let _ = msg_env.send_and_clear(pid, move |env| {
        let reply_ref = saved_ref.load(env);
        let payload = match result {
            Ok(encode) => encode(env),
            Err(err) => err.encode(env),
        };
        (atoms::slap_slatedb_reply(), reply_ref, payload).encode(env)
    });
}
