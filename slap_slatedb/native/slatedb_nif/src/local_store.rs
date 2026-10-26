use std::fmt::{Display, Formatter};
use std::fs::{self, File, OpenOptions, TryLockError};
use std::future::Future;
use std::io::{self, Write};
use std::ops::Range;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use futures::stream::BoxStream;
use futures::StreamExt;
use slatedb::bytes::Bytes;
use slatedb::object_store::local::LocalFileSystem;
use slatedb::object_store::path::Path as StorePath;
use slatedb::object_store::{
    CopyOptions, Error, GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta,
    ObjectStore, ObjectStoreExt, PutMode, PutMultipartOptions, PutOptions, PutPayload, PutResult,
    RenameOptions, Result,
};

// object_store hides numeric # suffixes; multipart staging starts at #1.
const LOCK_FILE: &str = ".slap-lock#0";

#[derive(Clone, Debug)]
pub(crate) struct LocalStore {
    inner: LocalFileSystem,
    root: PathBuf,
    // ETags include the mtime in microseconds. Coarser mtimes can repeat an
    // ETag, so updates are refused.
    exact_mtimes: bool,
}

impl LocalStore {
    pub(crate) fn new(root: &Path) -> Result<Self> {
        fs::create_dir_all(root)
            .map_err(|error| filesystem_error(io_context("create directories", root, error)))?;
        let root = fs::canonicalize(root)
            .map_err(|error| filesystem_error(io_context("canonicalize directory", root, error)))?;
        #[cfg(unix)]
        {
            let directory = File::open(&root).map_err(|error| {
                filesystem_error(io_context("open store directory", &root, error))
            })?;
            crate::local_filesystem::inspect(&directory, &root).map_err(|error| {
                filesystem_error(io_context("inspect filesystem", &root, error))
            })?;
        }
        sync_root(&root).map_err(filesystem_error)?;
        let lock = open_lock(&root).map_err(filesystem_error)?;
        let exact_mtimes = keeps_microseconds(&lock).map_err(|error| {
            filesystem_error(io_context("probe modification times", &root, error))
        })?;
        let inner = LocalFileSystem::new_with_prefix(&root)?.with_fsync(true);
        Ok(Self {
            inner,
            root,
            exact_mtimes,
        })
    }

    async fn mutate<T, F, Fut>(&self, operation: F) -> Result<T>
    where
        T: Send + 'static,
        F: FnOnce(Self, Arc<File>) -> Fut + Send + 'static,
        Fut: Future<Output = Result<T>> + Send,
    {
        let root = self.root.clone();
        let lock = tokio::task::spawn_blocking(move || open_lock(&root))
            .await
            .map_err(|source| Error::JoinError { source })?
            .map_err(filesystem_error)?;
        // object_store 0.14.2 implements create-only rename as copy then delete.
        // An intervening overwrite could be deleted, so every mutation locks exclusively.
        loop {
            match lock.try_lock() {
                Ok(()) => break,
                Err(TryLockError::WouldBlock) => tokio::time::sleep(Duration::from_millis(5)).await,
                Err(TryLockError::Error(error)) => {
                    return Err(filesystem_error(io_context(
                        "lock store",
                        &self.root,
                        error,
                    )));
                }
            }
        }
        // Dropping the caller's future must not unlock while blocking I/O can still publish.
        let store = self.clone();
        let lock = Arc::new(lock);
        let held = Arc::clone(&lock);
        // Closing the last descriptor unlocks, so this copy outlives the operation's.
        tokio::spawn(async move {
            let result = operation(store, held).await;
            drop(lock);
            result
        })
        .await
        .map_err(|source| Error::JoinError { source })?
    }

    async fn put_unlocked(
        &self,
        location: &StorePath,
        payload: PutPayload,
        opts: PutOptions,
        lock: Arc<File>,
    ) -> Result<PutResult> {
        if !opts.attributes.is_empty() {
            return Err(Error::NotImplemented {
                operation: "`put_opts` with `opts.attributes` specified".into(),
                implementer: self.to_string(),
            });
        }
        if let PutMode::Update(version) = &opts.mode {
            if !self.exact_mtimes {
                return Err(Error::NotImplemented {
                    operation: "`put_opts` with mode `PutMode::Update` without microsecond modification times".into(),
                    implementer: self.to_string(),
                });
            }
            let expected = version
                .e_tag
                .as_ref()
                .ok_or_else(|| precondition(location, "ETag required"))?;
            let current = match self.inner.head(location).await {
                Ok(meta) => meta,
                Err(Error::NotFound { .. }) => {
                    return Err(precondition(location, "object missing"))
                }
                Err(error) => return Err(error),
            };
            if current.e_tag.as_ref() != Some(expected) {
                return Err(precondition(location, "ETag mismatch"));
            }
        }
        self.prepare_destination(location).await?;
        let path = self.inner.path_to_filesystem(location)?;
        let create = matches!(opts.mode, PutMode::Create);
        let exact = self.exact_mtimes;
        tokio::task::spawn_blocking(move || write_object(&path, &payload, create, &lock, exact))
            .await
            .map_err(|source| Error::JoinError { source })??;
        Ok(PutResult {
            e_tag: self.inner.head(location).await?.e_tag,
            version: None,
            extensions: Default::default(),
        })
    }

    // A copy or rename restamps its source first: the destination gets the
    // source's inode and would otherwise show an earlier ETag.
    async fn restamp(
        &self,
        location: &StorePath,
        related: &StorePath,
        lock: Arc<File>,
    ) -> Result<()> {
        if !self.exact_mtimes {
            return Ok(());
        }
        let path = self.inner.path_to_filesystem(location)?;
        let related = self.inner.path_to_filesystem(related)?;
        let name = location.to_string();
        tokio::task::spawn_blocking(move || {
            let file = OpenOptions::new().write(true).open(&path)?;
            stamp(&file, &[&path, &related], &lock)
                .and_then(|()| file.sync_all())
                .map_err(|error| io_context("set modification time", &path, error))
        })
        .await
        .map_err(|source| Error::JoinError { source })?
        .map_err(|source| match source.kind() {
            io::ErrorKind::NotFound => Error::NotFound {
                path: name,
                source: source.into(),
            },
            _ => filesystem_error(source),
        })
    }

    async fn delete_unlocked(&self, location: &StorePath) -> Result<()> {
        let path = self.inner.path_to_filesystem(location)?;
        // object_store 0.14.2 ignores with_fsync for standalone deletes.
        let result = self.inner.delete(location).await;
        if result.is_ok() || matches!(&result, Err(Error::NotFound { .. })) {
            // A missing object may retry an unlink whose sync failed.
            tokio::task::spawn_blocking(move || sync_deleted_parent(&path))
                .await
                .map_err(|source| Error::JoinError { source })?
                .map_err(filesystem_error)?;
        }
        result
    }

    async fn prepare_destination(&self, location: &StorePath) -> Result<()> {
        let path = self.inner.path_to_filesystem(location)?;
        let root = self.root.clone();
        tokio::task::spawn_blocking(move || prepare_destination(&path, &root))
            .await
            .map_err(|source| Error::JoinError { source })?
            .map_err(filesystem_error)
    }
}

impl Display for LocalStore {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        Display::fmt(&self.inner, f)
    }
}

fn open_lock(root: &Path) -> io::Result<File> {
    let path = root.join(LOCK_FILE);
    let mut options = OpenOptions::new();
    options.read(true).write(true).create(true).truncate(false);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.custom_flags(libc::O_NOFOLLOW | libc::O_NONBLOCK);
    }
    let file = options
        .open(&path)
        .map_err(|error| io_context("open mutation lock", &path, error))?;
    if !file
        .metadata()
        .map_err(|error| io_context("inspect mutation lock", &path, error))?
        .is_file()
    {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("mutation lock {} must be a regular file", path.display()),
        ));
    }
    Ok(file)
}

fn keeps_microseconds(lock: &File) -> io::Result<bool> {
    lock.lock()?;
    let mut next = micros(SystemTime::now()).max(micros(lock.metadata()?.modified()?) + 1);
    // A value ending in zero would not show truncation to tens of microseconds
    // or any coarser precision.
    if next.is_multiple_of(10) {
        next += 1;
    }
    let mtime = UNIX_EPOCH + Duration::from_micros(next);
    lock.set_modified(mtime)?;
    let kept = lock.metadata()?.modified()? == mtime;
    lock.unlock()?;
    Ok(kept)
}

// Above every earlier time and the related files' mtimes, so ETags do not
// repeat when the clock moves backwards or an inode is reused. The lock file's
// mtime is the mark. It is not synced: no version is persisted, and only an OS
// crash, which ends every process holding one, loses it.
fn next_mtime(lock: &File, related: &[&Path]) -> io::Result<SystemTime> {
    let mut floor = micros(lock.metadata()?.modified()?);
    for path in related {
        match fs::metadata(path) {
            Ok(metadata) => floor = floor.max(micros(metadata.modified()?)),
            Err(error) if error.kind() == io::ErrorKind::NotFound => {}
            Err(error) => return Err(error),
        }
    }
    let mtime = UNIX_EPOCH + Duration::from_micros(micros(SystemTime::now()).max(floor + 1));
    lock.set_modified(mtime)?;
    Ok(mtime)
}

fn micros(time: SystemTime) -> u64 {
    time.duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_micros() as u64
}

fn stamp(file: &File, related: &[&Path], lock: &File) -> io::Result<()> {
    let mtime = next_mtime(lock, related)?;
    file.set_modified(mtime)?;
    if file.metadata()?.modified()? != mtime {
        return Err(io::Error::other(
            "filesystem did not keep the modification time",
        ));
    }
    Ok(())
}

// Readers do not lock, so the object is stamped before it is published.
fn write_object(
    path: &Path,
    payload: &PutPayload,
    create: bool,
    lock: &File,
    exact: bool,
) -> Result<()> {
    let (mut file, staging) = new_staging(path).map_err(filesystem_error)?;
    let filled = payload
        .iter()
        .try_for_each(|chunk| file.write_all(chunk))
        .and_then(|()| {
            if exact {
                stamp(&file, &[path], lock)
            } else {
                Ok(())
            }
        })
        .and_then(|()| file.sync_all())
        .map_err(|error| io_context("write staging file", &staging, error));
    drop(file);
    if let Err(error) = filled {
        let _ = fs::remove_file(&staging);
        return Err(filesystem_error(error));
    }
    let published = if create {
        fs::hard_link(&staging, path)
    } else {
        fs::rename(&staging, path)
    };
    // After a rename the staging name is free, and a multipart upload may take it.
    if create || published.is_err() {
        let _ = fs::remove_file(&staging);
    }
    match published {
        Err(error) if error.kind() == io::ErrorKind::AlreadyExists => {
            return Err(Error::AlreadyExists {
                path: path.display().to_string(),
                source: error.into(),
            })
        }
        Err(error) => return Err(filesystem_error(io_context("publish object", path, error))),
        Ok(()) => {}
    }
    match path.parent() {
        Some(parent) => sync_directory(parent).map_err(filesystem_error),
        None => Ok(()),
    }
}

// Multipart uploads stage outside the lock, so names are claimed exclusively.
fn new_staging(path: &Path) -> io::Result<(File, PathBuf)> {
    let mut n = 1u64;
    loop {
        let mut name = path.as_os_str().to_owned();
        name.push(format!("#{n}"));
        let staging = PathBuf::from(name);
        match OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&staging)
        {
            Ok(file) => return Ok((file, staging)),
            Err(error) if error.kind() == io::ErrorKind::AlreadyExists => n += 1,
            Err(error) => return Err(io_context("create staging file", &staging, error)),
        }
    }
}

fn precondition(location: &StorePath, message: &str) -> Error {
    Error::Precondition {
        path: location.to_string(),
        source: io::Error::other(message).into(),
    }
}

fn sync_root(root: &Path) -> io::Result<()> {
    // A failed or concurrent open can leave existing ancestors unsynced.
    #[cfg(unix)]
    for directory in root.ancestors() {
        sync_directory(directory)?;
    }
    #[cfg(not(unix))]
    let _ = root;
    Ok(())
}

fn prepare_destination(path: &Path, root: &Path) -> io::Result<()> {
    // Existing directories may remain unsynced after an earlier creation failed.
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)
            .map_err(|error| io_context("create directories", parent, error))?;
        for directory in parent.ancestors().take_while(|p| p.starts_with(root)) {
            sync_directory(directory)?;
        }
    }
    Ok(())
}

fn io_context(operation: &str, path: &Path, error: io::Error) -> io::Error {
    io::Error::new(
        error.kind(),
        format!("{operation} {}: {error}", path.display()),
    )
}

fn filesystem_error(source: io::Error) -> Error {
    Error::Generic {
        store: "LocalFileSystem",
        source: Box::new(source),
    }
}

fn sync_directory(directory: &Path) -> io::Result<()> {
    #[cfg(unix)]
    File::open(directory)
        .map_err(|error| io_context("open directory for sync", directory, error))?
        .sync_all()
        .map_err(|error| io_context("sync directory", directory, error))?;
    #[cfg(not(unix))]
    let _ = directory;
    Ok(())
}

fn sync_deleted_parent(path: &Path) -> io::Result<()> {
    let mut directory = path.parent();
    while let Some(parent) = directory {
        match sync_directory(parent) {
            Err(error) if error.kind() == io::ErrorKind::NotFound => directory = parent.parent(),
            result => return result,
        }
    }
    Ok(())
}

#[async_trait::async_trait]
impl ObjectStore for LocalStore {
    async fn put_opts(
        &self,
        location: &StorePath,
        payload: PutPayload,
        opts: PutOptions,
    ) -> Result<PutResult> {
        let location = location.clone();
        self.mutate(move |store, lock| async move {
            store.put_unlocked(&location, payload, opts, lock).await
        })
        .await
    }

    async fn put_multipart_opts(
        &self,
        location: &StorePath,
        opts: PutMultipartOptions,
    ) -> Result<Box<dyn MultipartUpload>> {
        self.prepare_destination(location).await?;
        let inner = self.inner.clone();
        let location_for_upload = location.clone();
        let location = location.clone();
        // Upstream prepares multipart files synchronously when polled.
        let upload = tokio::task::spawn_blocking(move || {
            tokio::runtime::Handle::current().block_on(inner.put_multipart_opts(&location, opts))
        })
        .await
        .map_err(|source| Error::JoinError { source })??;
        Ok(Box::new(LocalUpload {
            inner: Some(upload),
            store: self.clone(),
            location: location_for_upload,
        }))
    }

    async fn get_opts(&self, location: &StorePath, options: GetOptions) -> Result<GetResult> {
        self.inner.get_opts(location, options).await
    }

    async fn get_ranges(&self, location: &StorePath, ranges: &[Range<u64>]) -> Result<Vec<Bytes>> {
        self.inner.get_ranges(location, ranges).await
    }

    fn delete_stream(
        &self,
        locations: BoxStream<'static, Result<StorePath>>,
    ) -> BoxStream<'static, Result<StorePath>> {
        let store = self.clone();
        locations
            .map(move |location| {
                let store = store.clone();
                async move {
                    let location = location?;
                    store
                        .mutate(move |store, _lock| async move {
                            store.delete_unlocked(&location).await?;
                            Ok(location)
                        })
                        .await
                }
            })
            .buffered(10)
            .boxed()
    }

    fn list(&self, prefix: Option<&StorePath>) -> BoxStream<'static, Result<ObjectMeta>> {
        self.inner.list(prefix)
    }

    fn list_with_offset(
        &self,
        prefix: Option<&StorePath>,
        offset: &StorePath,
    ) -> BoxStream<'static, Result<ObjectMeta>> {
        self.inner.list_with_offset(prefix, offset)
    }

    async fn list_with_delimiter(&self, prefix: Option<&StorePath>) -> Result<ListResult> {
        self.inner.list_with_delimiter(prefix).await
    }

    async fn copy_opts(
        &self,
        from: &StorePath,
        to: &StorePath,
        options: CopyOptions,
    ) -> Result<()> {
        let (from, to) = (from.clone(), to.clone());
        self.mutate(move |store, lock| async move {
            store.prepare_destination(&to).await?;
            store.restamp(&from, &to, lock).await?;
            store.inner.copy_opts(&from, &to, options).await
        })
        .await
    }

    async fn rename_opts(
        &self,
        from: &StorePath,
        to: &StorePath,
        options: RenameOptions,
    ) -> Result<()> {
        let (from, to) = (from.clone(), to.clone());
        self.mutate(move |store, lock| async move {
            store.prepare_destination(&to).await?;
            store.restamp(&from, &to, lock).await?;
            store.inner.rename_opts(&from, &to, options).await
        })
        .await
    }
}

#[derive(Debug)]
struct LocalUpload {
    inner: Option<Box<dyn MultipartUpload>>,
    store: LocalStore,
    location: StorePath,
}

impl LocalUpload {
    fn take(&mut self) -> Result<Box<dyn MultipartUpload>> {
        self.inner.take().ok_or_else(|| {
            filesystem_error(io::Error::other("upload already completed or aborted"))
        })
    }
}

#[async_trait::async_trait]
impl MultipartUpload for LocalUpload {
    fn put_part(&mut self, data: PutPayload) -> slatedb::object_store::UploadPart {
        match self.inner.as_mut() {
            Some(upload) => upload.put_part(data),
            None => Box::pin(async {
                Err(filesystem_error(io::Error::other(
                    "upload already completed or aborted",
                )))
            }),
        }
    }

    async fn complete(&mut self) -> Result<PutResult> {
        let mut upload = self.take()?;
        let location = self.location.clone();
        self.store
            .mutate(move |store, lock| async move {
                store.prepare_destination(&location).await?;
                let mut result = upload.complete().await?;
                // object_store publishes multipart uploads itself, so an unlocked
                // reader can briefly see the ETag of the upload's own timestamp.
                store.restamp(&location, &location, lock).await?;
                result.e_tag = store.inner.head(&location).await?.e_tag;
                Ok(result)
            })
            .await
    }

    async fn abort(&mut self) -> Result<()> {
        self.take()?.abort().await
    }
}

#[cfg(test)]
#[path = "local_store_tests.rs"]
mod tests;
