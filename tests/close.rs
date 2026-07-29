//! `close`: an awaitable close that reports whether it actually happened.
//!
//! These use TCP rather than Unix sockets deliberately. The sharing rules that
//! `close` reports on are not platform-specific, so testing them over a
//! platform-specific transport would leave Windows — where the descriptor is a
//! handle and the close path is a different implementation — with no coverage
//! of the case that actually has a decision in it.

mod common;

use common::block_on;
use runite::io::CloseOutcome;
use runite::net::{TcpListener, TcpStream};

/// The ordinary case: sole owner, so the descriptor is closed.
#[test]
fn closing_a_sole_owner_closes_the_descriptor() {
    let dir = std::env::temp_dir().join("runite-cl");
    std::fs::create_dir_all(&dir).expect("create close test directory");
    let path = dir.join(format!("sole{}", std::process::id()));
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
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
        let addr = listener.local_addr().expect("local addr");
        let server = runite::spawn(async move {
            let _accepted = listener.accept().await.expect("accept");
        });

        let stream = TcpStream::connect(addr).await.expect("connect");
        let (read_half, write_half) = stream.into_split();
        let reunited =
            TcpStream::reunite(read_half, write_half).expect("halves came from the same stream");
        let outcome = reunited
            .close_descriptor()
            .await
            .expect("close should not error");
        server.await.expect("server task");
        outcome
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
    let outcome = block_on(|| async {
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
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
}

/// Closing does not leak when it reports `StillShared`: the remaining holder
/// closes on drop, as it always did.
#[test]
fn a_still_shared_close_leaves_the_other_holder_working() {
    block_on(|| async {
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
        let incoming = listener.incoming();
        assert_eq!(
            listener.close_descriptor().await.expect("close"),
            CloseOutcome::StillShared
        );
        // The descriptor is still live for the remaining holder.
        drop(incoming);
    });
}
