use std::fmt::{Display, Formatter};
use std::fs;
#[cfg(unix)]
use std::fs::File;
use std::io;
use std::ops::Range;
use std::path::{Path, PathBuf};

use futures::stream::BoxStream;
use futures::StreamExt;
use slatedb::bytes::Bytes;
use slatedb::object_store::local::LocalFileSystem;
use slatedb::object_store::path::Path as StorePath;
use slatedb::object_store::{
    CopyOptions, Error, GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta,
    ObjectStore, ObjectStoreExt, PutMultipartOptions, PutOptions, PutPayload, PutResult,
    RenameOptions, Result,
};

#[derive(Debug)]
pub(crate) struct LocalStore {
    inner: LocalFileSystem,
    root: PathBuf,
}

impl LocalStore {
    pub(crate) fn new(root: &Path) -> Result<Self> {
        fs::create_dir_all(root)
            .map_err(|error| filesystem_error(io_context("create directories", root, error)))?;
        let root = fs::canonicalize(root)
            .map_err(|error| filesystem_error(io_context("canonicalize directory", root, error)))?;
        sync_root(&root).map_err(filesystem_error)?;
        let inner = LocalFileSystem::new_with_prefix(&root)?.with_fsync(true);
        Ok(Self { inner, root })
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
        self.prepare_destination(location).await?;
        self.inner.put_opts(location, payload, opts).await
    }

    async fn put_multipart_opts(
        &self,
        location: &StorePath,
        opts: PutMultipartOptions,
    ) -> Result<Box<dyn MultipartUpload>> {
        self.prepare_destination(location).await?;
        let inner = self.inner.clone();
        let location = location.clone();
        // Upstream prepares multipart files synchronously when polled.
        tokio::task::spawn_blocking(move || {
            tokio::runtime::Handle::current().block_on(inner.put_multipart_opts(&location, opts))
        })
        .await
        .map_err(|source| Error::JoinError { source })?
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
        let store = self.inner.clone();
        locations
            .map(move |location| {
                let store = store.clone();
                async move {
                    let location = location?;
                    let path = store.path_to_filesystem(&location)?;
                    // object_store 0.14.2 ignores with_fsync for standalone deletes.
                    let result = store.delete(&location).await;
                    if result.is_ok() || matches!(&result, Err(Error::NotFound { .. })) {
                        // A missing object may be a retry after unlink succeeded but sync failed.
                        tokio::task::spawn_blocking(move || sync_deleted_parent(&path))
                            .await
                            .map_err(|source| Error::JoinError { source })?
                            .map_err(filesystem_error)?;
                    }
                    result.map(|()| location)
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
        self.prepare_destination(to).await?;
        self.inner.copy_opts(from, to, options).await
    }

    async fn rename_opts(
        &self,
        from: &StorePath,
        to: &StorePath,
        options: RenameOptions,
    ) -> Result<()> {
        self.prepare_destination(to).await?;
        self.inner.rename_opts(from, to, options).await
    }
}
