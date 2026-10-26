use std::collections::HashMap;
use std::fs;
use std::io::Write as _;
use std::path::Path;
use std::process::Command;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use futures::{stream, StreamExt, TryStreamExt};
use slatedb::object_store::path::Path as StorePath;
use slatedb::object_store::{Error, ObjectStore, ObjectStoreExt, PutMode, Result};

use super::{build_store, StoreKind};
use crate::local_store::LocalStore;

const TEST: &str = "store::local_durability_tests::filesystem_operations_sync_before_success";

#[derive(Clone, Copy, Debug)]
enum Write {
    Put,
    Create,
    Copy,
    Rename,
    Multipart,
}

impl Write {
    fn needs_source(self) -> bool {
        matches!(self, Self::Copy | Self::Rename)
    }
}

#[derive(Clone, Copy, Debug)]
enum Directories {
    New,
    Existing,
}

#[derive(Clone, Copy, Debug)]
enum Upload {
    Put,
    Multipart,
}

#[derive(Clone, Copy, Debug)]
enum PublicationSync {
    File,
    Parent,
}

#[derive(Clone, Copy, Debug)]
enum Case {
    Preparation(Write, Directories),
    Publication(Upload, PublicationSync),
    Deletion,
    Opening,
}

fn cases() -> Vec<Case> {
    let preparation = [
        Write::Put,
        Write::Create,
        Write::Copy,
        Write::Rename,
        Write::Multipart,
    ]
    .into_iter()
    .flat_map(|operation| {
        [Directories::New, Directories::Existing]
            .map(|directories| Case::Preparation(operation, directories))
    });
    let publication = [Upload::Put, Upload::Multipart]
        .into_iter()
        .flat_map(|upload| {
            [PublicationSync::File, PublicationSync::Parent]
                .map(|sync| Case::Publication(upload, sync))
        });
    preparation
        .chain(publication)
        .chain([Case::Deletion, Case::Opening])
        .collect()
}

#[derive(Debug)]
struct Call {
    thread: String,
    text: String,
}

#[test]
fn filesystem_operations_sync_before_success() {
    if let Ok(root) = std::env::var("SLAP_LOCAL_SYNC_ROOT") {
        child(
            Path::new(&root),
            &std::env::var("SLAP_LOCAL_SYNC_CASE").unwrap(),
            std::env::var_os("SLAP_LOCAL_SYNC_INJECT").is_some(),
        );
        return;
    }
    let temp = std::env::temp_dir().join(format!(
        "slap-local-sync-{}-{}",
        std::process::id(),
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    fs::create_dir(&temp).unwrap();
    let temp = fs::canonicalize(temp).unwrap();
    let root = temp.join("new/ancestors/root");
    let trace = run_trace(&temp, &root, "success", None);
    check_success(&trace, &root);
    check_multipart_thread(&trace);

    for case in cases() {
        let id = format!("{case:?}");
        let probe = run_trace(&temp, &root, &id, None);
        let position = fault_position(&probe, &root, case);
        let target = &probe[position];
        let ordinal = probe[..=position]
            .iter()
            .filter(|call| call.thread == target.thread && call.text.starts_with("fsync("))
            .count();
        let trace = run_trace(&temp, &root, &id, Some(ordinal));
        let failures: Vec<_> = trace
            .iter()
            .enumerate()
            .filter(|(_, call)| call.text.contains("EIO"))
            .collect();
        assert_eq!(failures.len(), 1, "{}: {trace:#?}", id);
        let (failure, call) = failures[0];
        assert!(
            matches_fault(call, &root, case),
            "{}: wrong sync failed: {call:?}",
            id
        );
        let rejected = ack(&trace, "rejected");
        assert!(failure < rejected, "{}: {trace:#?}", id);
        match case {
            Case::Preparation(..) => check_retry_directories(&trace, &root, &root.join("nested/a")),
            Case::Opening => check_retry_directories(&trace, &root, &root.join("opened/nested")),
            Case::Deletion => check_retry_directories(&trace, &root, &root),
            Case::Publication(..) => (),
        }
    }
    fs::remove_dir_all(temp).unwrap();
}

fn check_retry_directories(trace: &[Call], root: &Path, parent: &Path) {
    let rejected = ack(trace, "rejected");
    let retry = ack(trace, "retry");
    for directory in parent.ancestors().take_while(|p| p.starts_with(root)) {
        assert!(
            trace[rejected + 1..retry]
                .iter()
                .any(|call| sync_path(call, directory)),
            "retry did not sync {}: {trace:#?}",
            directory.display()
        );
    }
}

fn fault_position(trace: &[Call], root: &Path, case: Case) -> usize {
    let mut start = ack(trace, "attempt");
    if matches!(case, Case::Publication(_, PublicationSync::Parent)) {
        start = trace
            .iter()
            .enumerate()
            .skip(start + 1)
            .find(|(_, call)| publication(call, &root.join("value")))
            .unwrap()
            .0;
    }
    trace
        .iter()
        .enumerate()
        .skip(start + 1)
        .find(|(_, call)| matches_fault(call, root, case) && call.text.ends_with("= 0"))
        .unwrap_or_else(|| panic!("{case:?}: no fault target: {trace:#?}"))
        .0
}

fn matches_fault(call: &Call, root: &Path, case: Case) -> bool {
    if !call.text.starts_with("fsync(") {
        return false;
    }
    let path = match case {
        Case::Preparation(..) => root.join("nested"),
        Case::Opening => root.join("opened"),
        Case::Deletion | Case::Publication(_, PublicationSync::Parent) => root.to_owned(),
        Case::Publication(_, PublicationSync::File) => {
            return call
                .text
                .contains(&format!("<{}#", root.join("value").display()));
        }
    };
    call.text.contains(&format!("<{}>", path.display()))
}

fn publication(call: &Call, path: &Path) -> bool {
    (call.text.starts_with("rename") || call.text.starts_with("link"))
        && call.text.contains(&format!("\"{}\"", path.display()))
        && call.text.ends_with("= 0")
}

fn check_multipart_thread(trace: &[Call]) {
    let start = ack(trace, "multipart-start");
    let end = ack(trace, "multipart-open");
    let worker = &trace[start].thread;
    let calls = &trace[start + 1..end];
    assert!(
        calls.iter().any(|call| call.text.starts_with("fsync(")),
        "multipart preparation did not sync directories"
    );
    for call in calls.iter().filter(|call| {
        ["openat(", "fsync(", "mkdir(", "mkdirat("]
            .iter()
            .any(|name| call.text.starts_with(name))
    }) {
        assert_ne!(
            &call.thread, worker,
            "multipart preparation blocked the async worker: {call:?}"
        );
    }
}

fn run_trace(temp: &Path, root: &Path, case: &str, inject: Option<usize>) -> Vec<Call> {
    if temp.join("new").exists() {
        fs::remove_dir_all(temp.join("new")).unwrap();
    }
    let log = temp.join(format!(
        "{case}-{}.trace",
        if inject.is_some() { "fault" } else { "probe" }
    ));
    let mut cmd = Command::new("strace");
    cmd.args([
        "-f", "-yy", "-s", "512", "-e",
        "trace=openat,fsync,fdatasync,rename,renameat,renameat2,link,linkat,unlink,unlinkat,mkdir,mkdirat,write",
        "-o",
    ]).arg(&log);
    if let Some(nth) = inject {
        cmd.args(["-e", &format!("inject=fsync:error=EIO:when={nth}")]);
        cmd.env("SLAP_LOCAL_SYNC_INJECT", "1");
    } else {
        cmd.env_remove("SLAP_LOCAL_SYNC_INJECT");
    }
    let output = cmd
        .arg(std::env::current_exe().unwrap())
        .args(["--exact", TEST, "--nocapture", "--test-threads=1"])
        .env("SLAP_LOCAL_SYNC_ROOT", root)
        .env("SLAP_LOCAL_SYNC_CASE", case)
        .output()
        .expect("strace is required for Linux filesystem durability tests");
    assert!(
        output.status.success(),
        "{case}: stdout={} stderr={} trace={}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr),
        fs::read_to_string(&log).unwrap()
    );
    completed_calls(&fs::read_to_string(log).unwrap())
}

fn completed_calls(trace: &str) -> Vec<Call> {
    let mut pending = HashMap::new();
    let mut calls = Vec::new();
    for line in trace.lines() {
        let (thread, text) = line.split_once(char::is_whitespace).unwrap();
        let text = text.trim_start();
        if let Some(start) = text.strip_suffix("<unfinished ...>") {
            pending.insert(thread, start);
        } else if let Some((_, end)) = text.split_once(" resumed>") {
            calls.push(Call {
                thread: thread.to_owned(),
                text: format!("{}{end}", pending.remove(thread).unwrap()),
            });
        } else {
            calls.push(Call {
                thread: thread.to_owned(),
                text: text.to_owned(),
            });
        }
    }
    assert!(pending.is_empty());
    calls
}

fn ack(trace: &[Call], phase: &str) -> usize {
    trace
        .iter()
        .position(|call| call.text.contains(&format!("ACK {phase}\\n")))
        .unwrap()
}

fn sync_path(call: &Call, path: &Path) -> bool {
    call.text.starts_with("fsync(")
        && call.text.contains(&format!("<{}>", path.display()))
        && call.text.ends_with("= 0")
}

fn check_success(trace: &[Call], root: &Path) {
    let first = ack(trace, "root-new");
    let second = ack(trace, "root-existing");
    for ancestor in root.ancestors() {
        assert!(
            trace[..first].iter().any(|line| sync_path(line, ancestor)),
            "missing root sync: {}",
            ancestor.display()
        );
        assert!(
            trace[first + 1..second]
                .iter()
                .any(|line| sync_path(line, ancestor)),
            "missing existing-root sync: {}",
            ancestor.display()
        );
    }
    let mut before = second;
    for (phase, key, publication) in [
        ("put", "put/a/object", "rename"),
        ("overwrite", "put/a/object", "rename"),
        ("create", "create/a/object", "link"),
        ("multipart", "multipart/a/object", "rename"),
    ] {
        let end = ack(trace, phase);
        let calls = &trace[before + 1..end];
        let path = root.join(key);
        let publish = calls
            .iter()
            .position(|line| {
                line.text.starts_with(publication)
                    && line.text.contains(&format!("\"{}\"", path.display()))
                    && line.text.ends_with("= 0")
            })
            .unwrap_or_else(|| panic!("{phase}: no publication: {calls:#?}"));
        assert!(
            calls[..publish].iter().any(|line| {
                line.text.starts_with("fsync(")
                    && line.text.contains(&format!("<{}#", path.display()))
                    && line.text.ends_with("= 0")
            }),
            "{phase}: contents not synced before publication: {calls:#?}"
        );
        assert!(
            calls[publish + 1..]
                .iter()
                .any(|line| sync_path(line, path.parent().unwrap())),
            "{phase}: publication directory not synced: {calls:#?}"
        );
        for directory in [
            path.parent().unwrap(),
            path.parent().unwrap().parent().unwrap(),
            root,
        ] {
            assert!(
                calls[..publish]
                    .iter()
                    .any(|line| sync_path(line, directory)),
                "{phase}: destination directory not synced before publication: {}",
                directory.display()
            );
        }
        before = end;
    }
    for phase in ["delete", "delete-missing", "delete-never"] {
        let end = ack(trace, phase);
        let calls = &trace[before + 1..end];
        let directory = if phase == "delete-never" {
            root.to_path_buf()
        } else {
            root.join("put/a")
        };
        let sync = calls
            .iter()
            .position(|line| sync_path(line, &directory))
            .unwrap_or_else(|| panic!("{phase}: deletion directory not synced: {calls:#?}"));
        if phase == "delete" {
            let unlink = calls
                .iter()
                .position(|line| line.text.starts_with("unlink") && line.text.ends_with("= 0"))
                .unwrap();
            assert!(unlink < sync);
        }
        before = end;
    }
    let end = ack(trace, "delete-batch");
    let calls = &trace[before + 1..end];
    for key in ["create/a/object", "multipart/a/object"] {
        let path = root.join(key);
        let unlink = calls
            .iter()
            .position(|line| {
                line.text.starts_with("unlink")
                    && line.text.contains(&format!("\"{}\"", path.display()))
                    && line.text.ends_with("= 0")
            })
            .unwrap();
        assert!(calls[unlink + 1..]
            .iter()
            .any(|line| sync_path(line, path.parent().unwrap())));
    }
}

fn acknowledged(phase: &str) {
    std::io::stdout()
        .write_all(format!("ACK {phase}\n").as_bytes())
        .unwrap();
}

async fn open_store(root: &Path) -> Arc<dyn ObjectStore> {
    let root = root.to_owned();
    tokio::task::spawn_blocking(move || {
        build_store(StoreKind::Local, root.to_str().unwrap(), vec![], vec![])
            .unwrap_or_else(|_| panic!("cannot open local store"))
            .0
    })
    .await
    .unwrap()
}

fn child(root: &Path, id: &str, inject: bool) {
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .max_blocking_threads(1)
        .build()
        .unwrap();
    rt.block_on(async {
        let store = open_store(root).await;
        if id == "success" {
            acknowledged("root-new");
            let _ = open_store(root).await;
            acknowledged("root-existing");
            success(store.as_ref()).await;
            return;
        }
        let case = cases()
            .into_iter()
            .find(|case| format!("{case:?}") == id)
            .unwrap_or_else(|| panic!("unknown case: {id}"));
        match case {
            Case::Preparation(operation, directories) => {
                preparation(store.as_ref(), root, operation, directories, inject).await
            }
            Case::Publication(upload, _) => {
                acknowledged("attempt");
                let operation = match upload {
                    Upload::Put => Write::Put,
                    Upload::Multipart => Write::Multipart,
                };
                let result = write(store.as_ref(), operation, &StorePath::from("value")).await;
                finish_attempt(result, inject);
            }
            Case::Deletion => deletion(store.as_ref(), root, inject).await,
            Case::Opening => opening(root, inject).await,
        }
    });
}

fn finish_attempt(result: Result<()>, inject: bool) {
    if inject {
        assert!(result.is_err(), "sync failure was acknowledged");
        acknowledged("rejected");
    } else {
        result.unwrap();
        acknowledged("complete");
    }
}

fn check_sync_error(result: &Result<()>, path: &Path) {
    let error = result.as_ref().unwrap_err().to_string();
    assert!(
        error.contains(&format!("sync directory {}:", path.display())),
        "{error}"
    );
}

async fn preparation(
    store: &dyn ObjectStore,
    root: &Path,
    operation: Write,
    directories: Directories,
    inject: bool,
) {
    let key = StorePath::from("nested/a/object");
    if matches!(directories, Directories::Existing) {
        fs::create_dir_all(root.join("nested/a")).unwrap();
    }
    if operation.needs_source() {
        store
            .put(&StorePath::from("source"), "old".into())
            .await
            .unwrap();
    }
    acknowledged("attempt");
    let result = write(store, operation, &key).await;
    if inject {
        check_sync_error(&result, &root.join("nested"));
    }
    finish_attempt(result, inject);
    if inject {
        assert!(
            matches!(store.get(&key).await, Err(Error::NotFound { .. })),
            "preparation failure published the destination"
        );
        if operation.needs_source() {
            assert_eq!(
                store
                    .get(&StorePath::from("source"))
                    .await
                    .unwrap()
                    .bytes()
                    .await
                    .unwrap(),
                "old"
            );
        }
        write(store, operation, &key).await.unwrap();
        acknowledged("retry");
    }
}

async fn deletion(store: &dyn ObjectStore, root: &Path, inject: bool) {
    let key = StorePath::from("value");
    store.put(&key, "old".into()).await.unwrap();
    acknowledged("attempt");
    let result = store.delete(&key).await;
    if inject {
        check_sync_error(&result, root);
    }
    finish_attempt(result, inject);
    if inject {
        assert!(matches!(
            store.delete(&key).await,
            Err(Error::NotFound { .. })
        ));
        acknowledged("retry");
    }
}

async fn opening(root: &Path, inject: bool) {
    let destination = root.join("opened/nested");
    acknowledged("attempt");
    let result = open_local(destination.clone()).await;
    if inject {
        check_sync_error(&result, &root.join("opened"));
    }
    finish_attempt(result, inject);
    if inject {
        open_local(destination).await.unwrap();
        acknowledged("retry");
    }
}

// build_store's error conversion uses Rustler atoms and requires a running BEAM.
async fn open_local(root: std::path::PathBuf) -> Result<()> {
    tokio::task::spawn_blocking(move || LocalStore::new(&root).map(|_| ()))
        .await
        .unwrap()
}

async fn write(store: &dyn ObjectStore, operation: Write, key: &StorePath) -> Result<()> {
    let source = StorePath::from("source");
    match operation {
        Write::Put => store.put(key, "new".into()).await.map(|_| ()),
        Write::Create => store
            .put_opts(key, "new".into(), PutMode::Create.into())
            .await
            .map(|_| ()),
        Write::Copy => store.copy_opts(&source, key, Default::default()).await,
        Write::Rename => store.rename_opts(&source, key, Default::default()).await,
        Write::Multipart => {
            let mut upload = store.put_multipart(key).await?;
            upload.put_part("new".into()).await?;
            upload.complete().await.map(|_| ())
        }
    }
}

async fn success(store: &dyn ObjectStore) {
    let put = StorePath::from("put/a/object");
    store.put(&put, "one".into()).await.unwrap();
    acknowledged("put");
    store.put(&put, "two".into()).await.unwrap();
    acknowledged("overwrite");
    let create = StorePath::from("create/a/object");
    store
        .put_opts(&create, "one".into(), PutMode::Create.into())
        .await
        .unwrap();
    acknowledged("create");
    let multipart = StorePath::from("multipart/a/object");
    acknowledged("multipart-start");
    let mut upload = store.put_multipart(&multipart).await.unwrap();
    acknowledged("multipart-open");
    upload.put_part("part one".into()).await.unwrap();
    upload.put_part("part two".into()).await.unwrap();
    assert!(matches!(
        store.get(&multipart).await,
        Err(Error::NotFound { .. })
    ));
    upload.complete().await.unwrap();
    acknowledged("multipart");
    store.delete(&put).await.unwrap();
    acknowledged("delete");
    assert!(matches!(
        store.delete(&put).await,
        Err(Error::NotFound { .. })
    ));
    acknowledged("delete-missing");
    assert!(matches!(
        store.delete(&StorePath::from("never/a/object")).await,
        Err(Error::NotFound { .. })
    ));
    acknowledged("delete-never");
    store
        .delete_stream(stream::iter([Ok(create), Ok(multipart)]).boxed())
        .try_collect::<Vec<_>>()
        .await
        .unwrap();
    acknowledged("delete-batch");
}
