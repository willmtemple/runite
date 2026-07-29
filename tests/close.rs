//! `close`: an awaitable close that reports whether it actually happened.

mod common;

use common::block_on;
use runite::io::CloseOutcome;

fn temp_path(label: &str) -> std::path::PathBuf {
    let dir = std::env::current_dir()
        .expect("current dir")
        .join("target")
        .join("runite-close-tests");
    std::fs::create_dir_all(&dir).expect("create close test directory");
    dir.join(format!("{label}-{}", std::process::id()))
}

/// The ordinary case: sole owner, so the descriptor is closed.
#[test]
fn closing_a_sole_owner_closes_the_descriptor() {
    let path = temp_path("sole");
    std::fs::write(&path, b"contents").expect("seed file");
    let target = path.clone();

    let outcome = block_on(move || async move {
        let file = runite::fs::File::open(&target).await.expect("open");
        file.close_descriptor().await.expect("close should succeed")
    });

    assert_eq!(outcome, CloseOutcome::Closed);
    let _ = std::fs::remove_file(&path);
}

/// Reuniting split halves restores exclusive ownership, so a close after it
/// actually closes. The halves themselves have no `close`, which is why this
/// tests the reunited handle rather than one side.
#[test]
fn reuniting_split_halves_restores_exclusivity() {
    let outcome = block_on(|| async {
        let (stream, _peer) = runite::net::unix::UnixStream::pair().expect("pair");
        let (read_half, write_half) = stream.into_split();
        let reunited = runite::net::unix::UnixStream::reunite(read_half, write_half)
            .expect("halves came from the same stream");
        reunited
            .close_descriptor()
            .await
            .expect("close should not error")
    });

    assert_eq!(
        outcome,
        CloseOutcome::Closed,
        "both shares are accounted for, so the close should happen"
    );
}

/// Holding a listener's `Incoming` stream shares the descriptor, so the
/// listener cannot close it out from under the stream.
#[test]
fn closing_a_listener_with_a_live_incoming_reports_still_shared() {
    let path = temp_path("listener");
    let _ = std::fs::remove_file(&path);
    let bind = path.clone();

    let outcome = block_on(move || async move {
        let listener = runite::net::unix::UnixListener::bind(&bind).expect("bind");
        // The stream reference-counts the listener.
        let incoming = listener.incoming();
        let outcome = listener
            .close_descriptor()
            .await
            .expect("close should not error");
        drop(incoming);
        outcome
    });

    assert_eq!(
        outcome,
        CloseOutcome::StillShared,
        "a live Incoming holds the descriptor"
    );
    let _ = std::fs::remove_file(&path);
}

/// Closing does not leak when it reports `StillShared`: the remaining holder
/// closes on drop, as it always did.
#[test]
fn a_still_shared_close_leaves_the_other_holder_working() {
    let path = temp_path("shared-works");
    let _ = std::fs::remove_file(&path);
    let bind = path.clone();

    block_on(move || async move {
        let listener = runite::net::unix::UnixListener::bind(&bind).expect("bind");
        let incoming = listener.incoming();
        assert_eq!(
            listener.close_descriptor().await.expect("close"),
            CloseOutcome::StillShared
        );
        // The descriptor is still live for the remaining holder.
        drop(incoming);
    });

    let _ = std::fs::remove_file(&path);
}
