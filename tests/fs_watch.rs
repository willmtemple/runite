//! `fs::watch`: change notification driven by the runtime's own reactor.

#![cfg(target_os = "linux")]

mod common;

use std::time::Duration;

use common::block_on;
use runite::fs::watch::{EventKind, Recursive, Watcher};
use runite::io::StreamExt;

fn temp_dir(label: &str) -> std::path::PathBuf {
    let dir = std::env::current_dir()
        .expect("current dir")
        .join("target")
        .join("runite-watch-tests")
        .join(format!("{label}-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).expect("create watch test directory");
    dir
}

/// A change under a watched directory is reported, and the path names the
/// entry that changed rather than the directory.
#[test]
fn a_created_file_is_reported_against_its_own_path() {
    let dir = temp_dir("create");
    let target = dir.join("appeared.txt");

    let observed = block_on(move || async move {
        let mut watcher = Watcher::new().expect("watcher should be created");
        watcher
            .watch(&dir, Recursive::No)
            .expect("watching an existing directory should succeed");

        std::fs::write(&target, b"hello").expect("write should succeed");

        // Several events may describe one write; take the first that names the
        // file, since kinds are not portable but paths are.
        let deadline = runite::time::timeout(Duration::from_secs(10), async {
            loop {
                let event = watcher
                    .next()
                    .await
                    .expect("the stream is infinite")
                    .expect("watching should not error");
                if event.path == target {
                    return event;
                }
            }
        });
        deadline.await.expect("the change should be reported")
    });

    assert!(
        matches!(observed.kind, EventKind::Created | EventKind::Modified),
        "a new file should read as created or modified, saw {:?}",
        observed.kind
    );
}

/// A recursive watch covers directories that already exist beneath the root.
#[test]
fn a_recursive_watch_covers_existing_subdirectories() {
    let dir = temp_dir("recursive");
    let nested = dir.join("a").join("b");
    std::fs::create_dir_all(&nested).expect("create nested directories");
    let target = nested.join("deep.txt");

    let observed = block_on(move || async move {
        let mut watcher = Watcher::new().expect("watcher should be created");
        watcher
            .watch(&dir, Recursive::Yes)
            .expect("recursive watch should succeed");

        std::fs::write(&target, b"deep").expect("write should succeed");

        runite::time::timeout(Duration::from_secs(10), async {
            loop {
                let event = watcher
                    .next()
                    .await
                    .expect("the stream is infinite")
                    .expect("watching should not error");
                if event.path == target {
                    return event;
                }
            }
        })
        .await
        .expect("a change two levels down should be reported")
    });

    assert_eq!(observed.path.file_name().unwrap(), "deep.txt");
}

/// Unwatching stops delivery, and unwatching twice is not an error.
#[test]
fn unwatch_is_idempotent() {
    let dir = temp_dir("unwatch");

    block_on(move || async move {
        let mut watcher = Watcher::new().expect("watcher should be created");
        let id = watcher.watch(&dir, Recursive::No).expect("watch");
        watcher.unwatch(id).expect("first unwatch");
        watcher
            .unwatch(id)
            .expect("unwatching an already-removed watch is not an error");
    });
}

/// Watching something that is not there fails at registration rather than
/// silently yielding nothing.
#[test]
fn watching_a_missing_path_fails() {
    let dir = temp_dir("missing");
    block_on(move || async move {
        let mut watcher = Watcher::new().expect("watcher should be created");
        let error = watcher
            .watch(&dir.join("nope"), Recursive::No)
            .expect_err("a missing path should not register");
        assert_eq!(error.kind(), std::io::ErrorKind::NotFound);
    });
}
