//! Moving keys and values between BEAM binaries and `Bytes`.
//!
//! Small binaries are copied. Binaries of `LARGE_BINARY` bytes or more are
//! shared instead of copied:
//!
//! - **In:** a large Elixir binary is a reference-counted binary. Saving the
//!   term in an `OwnedEnv` takes a reference to the same data without copying
//!   it, and the `Bytes` keeps that environment alive.
//! - **Out:** a large `Bytes` goes into a resource, and the returned Elixir
//!   binary points into it (`enif_make_resource_binary`). The resource, and so
//!   the `Bytes`, lives until the Elixir binary is garbage collected.

use rustler::{Binary, Encoder, Env, NewBinary, OwnedEnv, Resource, ResourceArc, Term};
use slatedb::bytes::Bytes;

/// Binaries this size or larger are shared instead of copied.
pub(crate) const LARGE_BINARY: usize = 64 * 1024;

/// Keeps a BEAM binary alive for as long as a `Bytes` points into it.
struct TermBytes {
    // Holds a reference to the binary's data. Dropping the environment
    // releases it.
    _env: OwnedEnv,
    ptr: *const u8,
    len: usize,
}

// SAFETY: the data behind `ptr` is an immutable, reference-counted BEAM
// binary. It stays valid while `_env` holds a term that refers to it, and
// `OwnedEnv` can be dropped from any thread.
unsafe impl Send for TermBytes {}

impl AsRef<[u8]> for TermBytes {
    fn as_ref(&self) -> &[u8] {
        // SAFETY: see `unsafe impl Send` above.
        unsafe { std::slice::from_raw_parts(self.ptr, self.len) }
    }
}

/// Converts a binary argument into `Bytes`, sharing large binaries.
pub(crate) fn to_bytes(bin: Binary<'_>) -> Bytes {
    if bin.len() < LARGE_BINARY {
        return Bytes::copy_from_slice(bin.as_slice());
    }
    let env = OwnedEnv::new();
    let saved = env.save(bin);
    let (ptr, len) = env.run(|env| {
        let copy: Binary = saved
            .load(env)
            .decode()
            .expect("a saved binary decodes as a binary");
        (copy.as_slice().as_ptr(), copy.len())
    });
    Bytes::from_owner(TermBytes {
        _env: env,
        ptr,
        len,
    })
}

/// Holds a large value that an Elixir binary points into.
struct BytesResource(Bytes);

#[rustler::resource_impl]
impl Resource for BytesResource {}

/// Makes an Elixir binary from `data`, sharing large values.
pub(crate) fn bin<'a>(env: Env<'a>, data: &Bytes) -> Term<'a> {
    if data.len() >= LARGE_BINARY {
        return ResourceArc::new(BytesResource(data.clone()))
            .make_binary(env, |res| res.0.as_ref())
            .encode(env);
    }
    let mut out = NewBinary::new(env, data.len());
    out.as_mut_slice().copy_from_slice(data);
    out.into()
}
