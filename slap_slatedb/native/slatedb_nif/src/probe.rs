//! A check that an object store honours conditional writes, and a store that
//! ignores them, for testing the check.
//!
//! SlateDB fences writers with create-if-absent PUTs (`PutMode::Create`). A
//! store that silently overwrites instead lets two writers corrupt a
//! database, so a caller can run this probe before opening anything.

use std::fmt::{Debug, Display, Formatter};
use std::sync::Arc;

use futures::stream::BoxStream;
use rustler::{Atom, Encoder, Env, Term};
use slatedb::object_store::path::Path as StorePath;
use slatedb::object_store::{
    CopyOptions, Error as StoreError, GetOptions, GetResult, ListResult, MultipartUpload,
    ObjectMeta, ObjectStore, ObjectStoreExt, PutMode, PutMultipartOptions, PutOptions, PutPayload,
    PutResult, Result as StoreResult, UpdateVersion,
};

use crate::atoms;
use crate::reply::{spawn_reply, Encode};
use crate::store::{open_store, StoreSpec};

/// Wraps a store and turns every conditional PUT into an overwrite, like an
/// S3-compatible server that ignores `If-None-Match` and `If-Match`. Only for
/// testing the probe: SlateDB is not safe on it.
#[derive(Debug)]
pub(crate) struct IgnorePreconditions(pub(crate) Arc<dyn ObjectStore>);

impl Display for IgnorePreconditions {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "IgnorePreconditions({})", self.0)
    }
}

#[async_trait::async_trait]
impl ObjectStore for IgnorePreconditions {
    async fn put_opts(
        &self,
        location: &StorePath,
        payload: PutPayload,
        opts: PutOptions,
    ) -> StoreResult<PutResult> {
        let opts = PutOptions {
            mode: PutMode::Overwrite,
            ..opts
        };
        self.0.put_opts(location, payload, opts).await
    }

    async fn put_multipart_opts(
        &self,
        location: &StorePath,
        opts: PutMultipartOptions,
    ) -> StoreResult<Box<dyn MultipartUpload>> {
        self.0.put_multipart_opts(location, opts).await
    }

    async fn get_opts(&self, location: &StorePath, options: GetOptions) -> StoreResult<GetResult> {
        self.0.get_opts(location, options).await
    }

    fn delete_stream(
        &self,
        locations: BoxStream<'static, StoreResult<StorePath>>,
    ) -> BoxStream<'static, StoreResult<StorePath>> {
        self.0.delete_stream(locations)
    }

    fn list(&self, prefix: Option<&StorePath>) -> BoxStream<'static, StoreResult<ObjectMeta>> {
        self.0.list(prefix)
    }

    async fn list_with_delimiter(&self, prefix: Option<&StorePath>) -> StoreResult<ListResult> {
        self.0.list_with_delimiter(prefix).await
    }

    async fn copy_opts(
        &self,
        from: &StorePath,
        to: &StorePath,
        options: CopyOptions,
    ) -> StoreResult<()> {
        self.0.copy_opts(from, to, options).await
    }
}

/// The outcome of one probe step.
enum Step {
    Ok,
    /// The store does not implement this kind of conditional write.
    Unsupported,
    Failed(String),
    /// Not run, because an earlier step failed.
    Skipped,
}

impl Step {
    fn encode<'a>(&self, env: Env<'a>, name: Atom) -> Term<'a> {
        match self {
            Step::Ok => (name, atoms::ok()).encode(env),
            Step::Unsupported => (name, atoms::unsupported()).encode(env),
            Step::Skipped => (name, atoms::skipped()).encode(env),
            Step::Failed(message) => (name, (atoms::failed(), message.as_str())).encode(env),
        }
    }
}

fn unexpected(what: &str, err: StoreError) -> Step {
    Step::Failed(format!("{what}: {err}"))
}

fn is_unsupported(err: &StoreError) -> bool {
    matches!(
        err,
        StoreError::NotImplemented { .. } | StoreError::NotSupported { .. }
    )
}

async fn put(
    store: &dyn ObjectStore,
    path: &StorePath,
    body: &'static str,
    mode: PutMode,
) -> StoreResult<PutResult> {
    store
        .put_opts(path, PutPayload::from_static(body.as_bytes()), mode.into())
        .await
}

/// Runs the five steps against a new object at `path`:
///
/// 1. create-if-absent succeeds;
/// 2. create-if-absent again is refused;
/// 3. an update with a stale ETag is refused (or not supported);
/// 4. an update with the current ETag succeeds (or not supported);
/// 5. delete.
///
/// SlateDB needs 1 and 2. 3 and 4 matter to anything that updates objects
/// in place with `If-Match`; a store may not support them (the local file
/// system does not), but must never accept a stale ETag.
async fn run(store: &dyn ObjectStore, path: &StorePath) -> [Step; 5] {
    let created = match put(store, path, "1", PutMode::Create).await {
        Ok(result) => result,
        Err(err) => {
            return [
                unexpected("create-if-absent of a new object failed", err),
                Step::Skipped,
                Step::Skipped,
                Step::Skipped,
                Step::Skipped,
            ]
        }
    };

    let create_again = match put(store, path, "2", PutMode::Create).await {
        Err(StoreError::AlreadyExists { .. } | StoreError::Precondition { .. }) => Step::Ok,
        Ok(_) => Step::Failed("create-if-absent overwrote an existing object".into()),
        Err(err) => unexpected("create-if-absent of an existing object", err),
    };

    let stale = UpdateVersion {
        e_tag: Some("\"slatedb-probe-stale-etag\"".into()),
        version: None,
    };
    let stale_if_match = match put(store, path, "3", PutMode::Update(stale)).await {
        Err(StoreError::Precondition { .. }) => Step::Ok,
        Err(err) if is_unsupported(&err) => Step::Unsupported,
        Ok(_) => Step::Failed("an update with a stale ETag succeeded".into()),
        Err(err) => unexpected("an update with a stale ETag", err),
    };

    let current_if_match = match stale_if_match {
        Step::Ok => {
            let current = UpdateVersion {
                e_tag: created.e_tag.clone(),
                version: created.version.clone(),
            };
            match put(store, path, "4", PutMode::Update(current)).await {
                Ok(_) => Step::Ok,
                Err(err) if is_unsupported(&err) => Step::Unsupported,
                Err(err) => unexpected("an update with the current ETag", err),
            }
        }
        Step::Unsupported => Step::Unsupported,
        _ => Step::Skipped,
    };

    let delete = match store.delete(path).await {
        Ok(()) => Step::Ok,
        Err(err) => unexpected("delete", err),
    };

    [
        Step::Ok,
        create_again,
        stale_if_match,
        current_if_match,
        delete,
    ]
}

/// Replies `{:ok, [{step, :ok | :unsupported | :skipped | {:failed, message}}]}`,
/// or an error if the store cannot be built.
#[rustler::nif]
fn store_probe<'a>(env: Env<'a>, store: StoreSpec, path: String, reply_ref: Term<'a>) -> Term<'a> {
    spawn_reply(env, reply_ref, async move {
        let (store, path) = open_store(store, path).await?;
        let steps = run(store.as_ref(), &StorePath::from(path)).await;
        let encode: Encode = Box::new(move |env| {
            let names = [
                atoms::create(),
                atoms::create_again(),
                atoms::stale_if_match(),
                atoms::current_if_match(),
                atoms::delete(),
            ];
            let rows: Vec<Term<'_>> = steps
                .iter()
                .zip(names)
                .map(|(step, name)| step.encode(env, name))
                .collect();
            (atoms::ok(), rows).encode(env)
        });
        Ok(encode)
    })
}
