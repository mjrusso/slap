//! Direct access to objects in a store: small objects with conditional
//! writes (coordination objects kept next to the databases, such as leases),
//! and streamed uploads and downloads of large ones (file bodies).

use std::sync::Arc;

use futures::stream::BoxStream;
use futures::{StreamExt, TryStreamExt};
use rustler::types::atom::nil;
use rustler::{Binary, Encoder, Env, NifTaggedEnum, Resource, ResourceArc, Term};
use slatedb::bytes::Bytes;
use slatedb::object_store::path::Path as StorePath;
use slatedb::object_store::{
    Error as StoreError, ObjectStore, ObjectStoreExt, PutMode, PutPayload, UpdateVersion,
    WriteMultipart,
};
use tokio::sync::Mutex;

use crate::atoms;
use crate::binaries::{bin, to_bytes};
use crate::reply::{ok_atom, ok_with, spawn_reply, Encode, NifError};
use crate::store::{join_path, open_store, StoreSpec};

pub(crate) struct ObjectStoreResource {
    store: Arc<dyn ObjectStore>,
    /// Objects are under this path (the store URL's prefix joined with the
    /// path given to open).
    root: String,
}

#[rustler::resource_impl]
impl Resource for ObjectStoreResource {}

impl ObjectStoreResource {
    fn path(&self, key: &str) -> StorePath {
        StorePath::from(join_path(&self.root, key))
    }
}

/// An object's version, `{e_tag, version}`, either of which may be nil.
type Version = (Option<String>, Option<String>);

/// An error the call does not handle itself. Like SlateDB, which reports
/// object store errors other than a missing object as `:unavailable`.
fn store_error(what: &str, err: StoreError) -> NifError {
    NifError::unavailable(format!("{what}: {err}"))
}

#[rustler::nif]
fn objstore_open<'a>(
    env: Env<'a>,
    store: StoreSpec,
    path: String,
    reply_ref: Term<'a>,
) -> Term<'a> {
    spawn_reply(env, reply_ref, async move {
        let (store, root) = open_store(store, path).await?;
        Ok(ok_with(ResourceArc::new(ObjectStoreResource {
            store,
            root,
        })))
    })
}

/// Replies `{:ok, {body, version}}` or `{:ok, nil}` when there is no object.
#[rustler::nif]
fn objstore_get<'a>(
    env: Env<'a>,
    res: ResourceArc<ObjectStoreResource>,
    key: String,
    reply_ref: Term<'a>,
) -> Term<'a> {
    spawn_reply(env, reply_ref, async move {
        let path = res.path(&key);

        match res.store.get(&path).await {
            Ok(result) => {
                let version: Version = (result.meta.e_tag.clone(), result.meta.version.clone());
                let bytes = result
                    .bytes()
                    .await
                    .map_err(|e| store_error("reading an object", e))?;

                let encode: Encode =
                    Box::new(move |env| (atoms::ok(), (bin(env, &bytes), version)).encode(env));

                Ok(encode)
            }
            Err(StoreError::NotFound { .. }) => Ok(ok_with(nil())),
            Err(e) => Err(store_error("getting an object", e)),
        }
    })
}

#[derive(NifTaggedEnum)]
enum Mode {
    Overwrite,
    Create,
    Update(Version),
}

/// `mode` is `:overwrite`, `:create` or `{:update, version}`. Replies
/// `{:ok, {:ok, version}}`, `{:ok, :conflict}` (the object exists, for
/// `:create`; it changed or is gone, for `:update`) or `{:ok, :unsupported}`
/// (the store has no conditional updates, like the local file system).
#[rustler::nif]
fn objstore_put<'a>(
    env: Env<'a>,
    res: ResourceArc<ObjectStoreResource>,
    key: String,
    body: Binary<'a>,
    mode: Mode,
    reply_ref: Term<'a>,
) -> Term<'a> {
    let payload = PutPayload::from(to_bytes(body));
    let mode = match mode {
        Mode::Overwrite => PutMode::Overwrite,
        Mode::Create => PutMode::Create,
        Mode::Update((e_tag, version)) => PutMode::Update(UpdateVersion { e_tag, version }),
    };

    spawn_reply(env, reply_ref, async move {
        let path = res.path(&key);

        match res.store.put_opts(&path, payload, mode.into()).await {
            Ok(result) => {
                let version: Version = (result.e_tag, result.version);
                Ok(ok_with((atoms::ok(), version)))
            }
            Err(StoreError::AlreadyExists { .. })
            | Err(StoreError::Precondition { .. })
            | Err(StoreError::NotFound { .. }) => Ok(ok_with(atoms::conflict())),
            Err(StoreError::NotImplemented { .. }) | Err(StoreError::NotSupported { .. }) => {
                Ok(ok_with(atoms::unsupported()))
            }
            Err(e) => Err(store_error("putting an object", e)),
        }
    })
}

/// Replies `:ok`; deleting an object that does not exist is not an error.
#[rustler::nif]
fn objstore_delete<'a>(
    env: Env<'a>,
    res: ResourceArc<ObjectStoreResource>,
    key: String,
    reply_ref: Term<'a>,
) -> Term<'a> {
    spawn_reply(env, reply_ref, async move {
        match res.store.delete(&res.path(&key)).await {
            Ok(()) | Err(StoreError::NotFound { .. }) => Ok(ok_atom()),
            Err(e) => Err(store_error("deleting an object", e)),
        }
    })
}

/// Replies `{:ok, [key]}`: the keys under `prefix`, relative to the root.
#[rustler::nif]
fn objstore_list<'a>(
    env: Env<'a>,
    res: ResourceArc<ObjectStoreResource>,
    prefix: String,
    reply_ref: Term<'a>,
) -> Term<'a> {
    spawn_reply(env, reply_ref, async move {
        let root = res.path("").to_string();
        let root_prefix = (!root.is_empty()).then(|| format!("{root}/"));
        let keys: Vec<String> = res
            .store
            .list(Some(&res.path(&prefix)))
            .map_ok(|meta| {
                let full = meta.location.to_string();
                match &root_prefix {
                    Some(prefix) => full.strip_prefix(prefix).unwrap_or(&full).to_string(),
                    None => full,
                }
            })
            .try_collect()
            .await
            .map_err(|e| store_error("listing objects", e))?;

        Ok(ok_with(keys))
    })
}

/// A multipart upload in progress. Parts are uploaded as the buffer fills,
/// at most `UPLOAD_CONCURRENCY` at a time; nothing is visible at the key
/// until the upload finishes.
pub(crate) struct UploadResource {
    /// `None` once finished or aborted.
    writer: Mutex<Option<WriteMultipart>>,
}

#[rustler::resource_impl]
impl Resource for UploadResource {}

/// Parts of one upload in flight at once.
const UPLOAD_CONCURRENCY: usize = 8;

fn upload_done() -> NifError {
    NifError::invalid("the upload is already finished or aborted")
}

/// Replies `{:ok, upload}`.
#[rustler::nif]
fn objstore_upload_open<'a>(
    env: Env<'a>,
    res: ResourceArc<ObjectStoreResource>,
    key: String,
    reply_ref: Term<'a>,
) -> Term<'a> {
    spawn_reply(env, reply_ref, async move {
        let upload = res
            .store
            .put_multipart(&res.path(&key))
            .await
            .map_err(|e| store_error("starting an upload", e))?;

        Ok(ok_with(ResourceArc::new(UploadResource {
            writer: Mutex::new(Some(WriteMultipart::new(upload))),
        })))
    })
}

/// Adds `chunk` to the upload. Replies `:ok` once it is buffered, waiting
/// while too many parts are in flight.
#[rustler::nif]
fn objstore_upload_write<'a>(
    env: Env<'a>,
    upload: ResourceArc<UploadResource>,
    chunk: Binary<'a>,
    reply_ref: Term<'a>,
) -> Term<'a> {
    let chunk = to_bytes(chunk);

    spawn_reply(env, reply_ref, async move {
        let mut guard = upload.writer.lock().await;
        let writer = guard.as_mut().ok_or_else(upload_done)?;
        writer
            .wait_for_capacity(UPLOAD_CONCURRENCY)
            .await
            .map_err(|e| store_error("uploading a part", e))?;
        writer.put(chunk);
        Ok(ok_atom())
    })
}

/// Uploads what is buffered and completes the upload. Replies `{:ok,
/// version}`.
#[rustler::nif]
fn objstore_upload_finish<'a>(
    env: Env<'a>,
    upload: ResourceArc<UploadResource>,
    reply_ref: Term<'a>,
) -> Term<'a> {
    spawn_reply(env, reply_ref, async move {
        let writer = upload.writer.lock().await.take().ok_or_else(upload_done)?;
        let result = writer
            .finish()
            .await
            .map_err(|e| store_error("completing an upload", e))?;
        let version: Version = (result.e_tag, result.version);
        Ok(ok_with(version))
    })
}

/// Aborts the upload, removing its parts. Replies `:ok`, also for an upload
/// that is already finished or aborted.
#[rustler::nif]
fn objstore_upload_abort<'a>(
    env: Env<'a>,
    upload: ResourceArc<UploadResource>,
    reply_ref: Term<'a>,
) -> Term<'a> {
    spawn_reply(env, reply_ref, async move {
        if let Some(writer) = upload.writer.lock().await.take() {
            writer
                .abort()
                .await
                .map_err(|e| store_error("aborting an upload", e))?;
        }
        Ok(ok_atom())
    })
}

/// An object being read, one chunk at a time.
pub(crate) struct DownloadResource {
    stream: Mutex<BoxStream<'static, Result<Bytes, StoreError>>>,
}

#[rustler::resource_impl]
impl Resource for DownloadResource {}

/// Replies `{:ok, {download, size, version}}`, or `{:ok, nil}` when there is
/// no object.
#[rustler::nif]
fn objstore_download_open<'a>(
    env: Env<'a>,
    res: ResourceArc<ObjectStoreResource>,
    key: String,
    reply_ref: Term<'a>,
) -> Term<'a> {
    spawn_reply(env, reply_ref, async move {
        match res.store.get(&res.path(&key)).await {
            Ok(result) => {
                let size = result.meta.size;
                let version: Version = (result.meta.e_tag.clone(), result.meta.version.clone());
                let download = ResourceArc::new(DownloadResource {
                    stream: Mutex::new(result.into_stream()),
                });
                Ok(ok_with((download, size, version)))
            }
            Err(StoreError::NotFound { .. }) => Ok(ok_with(nil())),
            Err(e) => Err(store_error("getting an object", e)),
        }
    })
}

/// Replies `{:ok, chunk}`, or `{:ok, :eof}` after the last one.
#[rustler::nif]
fn objstore_download_next<'a>(
    env: Env<'a>,
    download: ResourceArc<DownloadResource>,
    reply_ref: Term<'a>,
) -> Term<'a> {
    spawn_reply(env, reply_ref, async move {
        match download.stream.lock().await.next().await {
            Some(Ok(bytes)) => {
                let encode: Encode =
                    Box::new(move |env| (atoms::ok(), bin(env, &bytes)).encode(env));
                Ok(encode)
            }
            Some(Err(e)) => Err(store_error("reading an object", e)),
            None => Ok(ok_with(atoms::eof())),
        }
    })
}
