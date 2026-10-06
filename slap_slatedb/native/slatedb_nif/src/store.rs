//! Building the object store a database, reader, admin handle or object
//! store handle works on.

use std::sync::Arc;

use rustler::NifUnitEnum;
use slatedb::object_store::aws::{AmazonS3Builder, AmazonS3ConfigKey, S3ConditionalPut};
use slatedb::object_store::azure::MicrosoftAzureBuilder;
use slatedb::object_store::gcp::GoogleCloudStorageBuilder;
use slatedb::object_store::local::LocalFileSystem;
use slatedb::object_store::memory::InMemory;
use slatedb::object_store::{ObjectStore, ObjectStoreScheme};

use crate::probe;
use crate::reply::{NifError, NifOutcome};

#[derive(NifUnitEnum)]
pub(crate) enum StoreKind {
    Memory,
    /// In memory, ignoring conditional writes. Only for testing the probe.
    MemoryIgnoringPreconditions,
    Local,
    Url,
}

/// `{kind, location, env_options, options}`, from `Slap.SlateDB.Options.store/1`.
pub(crate) type StoreSpec = (
    StoreKind,
    String,
    Vec<(String, String)>,
    Vec<(String, String)>,
);

/// Opens the store described by `spec` and returns it with the full path
/// (the URL's prefix joined with `path`).
pub(crate) async fn open_store(
    spec: StoreSpec,
    path: String,
) -> NifOutcome<(Arc<dyn ObjectStore>, String)> {
    let (store, prefix) = open_store_with_prefix(spec).await?;
    Ok((store, join_path(&prefix, &path)))
}

/// Opens the store described by `spec` and returns it with the path prefix
/// taken from a store URL (the `prefix` in `s3://bucket/prefix`).
///
/// Building a store can touch the file system, so it runs on a blocking
/// thread.
pub(crate) async fn open_store_with_prefix(
    spec: StoreSpec,
) -> NifOutcome<(Arc<dyn ObjectStore>, String)> {
    let (kind, location, env_options, options) = spec;
    tokio::task::spawn_blocking(move || build_store(kind, &location, env_options, options))
        .await
        .map_err(|e| NifError::internal(format!("building the store failed: {e}")))?
}

pub(crate) fn join_path(prefix: &str, path: &str) -> String {
    if prefix.is_empty() {
        path.to_string()
    } else {
        format!("{prefix}/{path}")
    }
}

fn build_store(
    kind: StoreKind,
    location: &str,
    env_options: Vec<(String, String)>,
    options: Vec<(String, String)>,
) -> NifOutcome<(Arc<dyn ObjectStore>, String)> {
    match kind {
        StoreKind::Memory => Ok((Arc::new(InMemory::new()), String::new())),
        StoreKind::MemoryIgnoringPreconditions => Ok((
            Arc::new(probe::IgnorePreconditions(Arc::new(InMemory::new()))),
            String::new(),
        )),
        StoreKind::Local => {
            std::fs::create_dir_all(location)
                .map_err(|e| NifError::invalid(format!("cannot create {location}: {e}")))?;
            let store = LocalFileSystem::new_with_prefix(location)
                .map_err(|e| NifError::invalid(format!("invalid local store {location}: {e}")))?;
            Ok((Arc::new(store), String::new()))
        }
        StoreKind::Url => {
            let parsed = url::Url::parse(location)
                .map_err(|e| NifError::invalid(format!("invalid store url {location}: {e}")))?;
            let (scheme, prefix) = ObjectStoreScheme::parse(&parsed)
                .map_err(|e| NifError::invalid(format!("unsupported store url {location}: {e}")))?;
            let store = match scheme {
                ObjectStoreScheme::AmazonS3 => build_s3_store(location, env_options, options)?,
                ObjectStoreScheme::MicrosoftAzure => {
                    build_azure_store(location, env_options, options)?
                }
                ObjectStoreScheme::GoogleCloudStorage => {
                    build_gcs_store(location, env_options, options)?
                }
                other => {
                    return Err(NifError::invalid(format!(
                        "store url scheme {other:?} is not supported; use s3://, az:// or gs://"
                    )))
                }
            };
            Ok((store, prefix.to_string()))
        }
    }
}

/// Applies store configuration to a builder.
///
/// `env_options` are the matching environment variables (`AWS_*`, `AZURE_*`
/// or `GOOGLE_*`), lowercased, as the Elixir side sees them. They are read
/// there, not with the builders' `from_env`, because `System.put_env/2`
/// changes the VM's copy of the environment and not the one Rust reads. Like
/// `from_env`, unknown environment keys are skipped.
///
/// `options` are applied after them, so they win. An unknown option key is an
/// error rather than being ignored, so a typo does not silently change
/// behavior.
fn configure<B, K>(
    mut builder: B,
    location: &str,
    env_options: Vec<(String, String)>,
    options: Vec<(String, String)>,
    with_config: fn(B, K, String) -> B,
    with_url: fn(B, String) -> B,
    store_name: &str,
) -> NifOutcome<B>
where
    K: std::str::FromStr,
{
    for (key, value) in env_options {
        if let Ok(parsed) = key.parse::<K>() {
            builder = with_config(builder, parsed, value);
        }
    }
    builder = with_url(builder, location.to_string());
    for (key, value) in options {
        let parsed: K = key
            .to_ascii_lowercase()
            .parse()
            .map_err(|_| NifError::invalid(format!("unknown {store_name} store option {key:?}")))?;
        builder = with_config(builder, parsed, value);
    }
    Ok(builder)
}

/// Builds an S3 store.
///
/// SlateDB depends on conditional puts to fence a second writer. The store
/// always uses ETag-based conditional puts, and any other
/// `conditional_put` setting (from the environment or from options) is
/// rejected.
fn build_s3_store(
    location: &str,
    env_options: Vec<(String, String)>,
    options: Vec<(String, String)>,
) -> NifOutcome<Arc<dyn ObjectStore>> {
    let builder = configure(
        AmazonS3Builder::new(),
        location,
        env_options,
        options,
        AmazonS3Builder::with_config,
        |b, url| b.with_url(url),
        "S3",
    )?;
    let conditional_put = builder
        .get_config_value(&AmazonS3ConfigKey::ConditionalPut)
        .unwrap_or_default();
    if conditional_put != "etag" {
        return Err(NifError::invalid(format!(
            "conditional_put is {conditional_put:?}, but SlateDB needs \"etag\" to fence \
             other writers"
        )));
    }
    let store = builder
        .with_conditional_put(S3ConditionalPut::ETagMatch)
        .build()
        .map_err(|e| NifError::invalid(format!("cannot build S3 store for {location}: {e}")))?;
    Ok(Arc::new(store))
}

/// Builds an Azure Blob Storage store. Azure supports the conditional puts
/// SlateDB needs without extra configuration.
fn build_azure_store(
    location: &str,
    env_options: Vec<(String, String)>,
    options: Vec<(String, String)>,
) -> NifOutcome<Arc<dyn ObjectStore>> {
    let store = configure(
        MicrosoftAzureBuilder::new(),
        location,
        env_options,
        options,
        MicrosoftAzureBuilder::with_config,
        |b, url| b.with_url(url),
        "Azure",
    )?
    .build()
    .map_err(|e| NifError::invalid(format!("cannot build Azure store for {location}: {e}")))?;
    Ok(Arc::new(store))
}

/// Builds a Google Cloud Storage store. GCS supports the conditional puts
/// SlateDB needs without extra configuration.
fn build_gcs_store(
    location: &str,
    env_options: Vec<(String, String)>,
    options: Vec<(String, String)>,
) -> NifOutcome<Arc<dyn ObjectStore>> {
    let store = configure(
        GoogleCloudStorageBuilder::new(),
        location,
        env_options,
        options,
        GoogleCloudStorageBuilder::with_config,
        |b, url| b.with_url(url),
        "GCS",
    )?
    .build()
    .map_err(|e| NifError::invalid(format!("cannot build GCS store for {location}: {e}")))?;
    Ok(Arc::new(store))
}
