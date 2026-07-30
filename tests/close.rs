//! `close`: an awaitable close that reports whether it actually happened.
//!
//! These use TCP rather than Unix sockets deliberately. The sharing rules that
//! `close` reports on are not platform-specific, so testing them over a
//! platform-specific transport would leave Windows — where the descriptor is a
//! handle and the close path is a different implementation — with no coverage
//! of the case that actually has a decision in it.

//! The Unix-only socket types get their own cases at the bottom, because their
//! `close_descriptor` implementations are separate copies of the same shape and
//! nothing else observes what they return.

mod common;

use common::block_on;
use runite::io::{CloseOutcome, StreamExt};
use runite::net::{TcpListener, TcpStream, UdpSocket};

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

/// A refused close leaves the descriptor working for the holder that refused
/// it. Re-checking the return value would only repeat the test above; what
/// distinguishes this one is accepting a connection *after* the close was
/// declined.
#[test]
fn a_still_shared_close_leaves_the_other_holder_working() {
    block_on(|| async {
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
        let addr = listener.local_addr().expect("local addr");
        let mut incoming = listener.incoming();

        assert_eq!(
            listener.close_descriptor().await.expect("close"),
            CloseOutcome::StillShared
        );

        let client = runite::spawn(async move { TcpStream::connect(addr).await });
        let accepted = incoming
            .next()
            .await
            .expect("incoming is infinite")
            .expect("the refused close must leave the listener accepting");
        assert_eq!(
            accepted.peer_addr().expect("peer addr").ip(),
            addr.ip(),
            "the accepted connection came through the listener that refused to close"
        );
        client.await.expect("client task").expect("connect");
    });
}

/// A datagram socket is portable and sole-owner, so it must report `Closed`.
#[test]
fn closing_a_udp_socket_closes_the_descriptor() {
    let outcome = block_on(|| async {
        let socket = UdpSocket::bind("127.0.0.1:0").await.expect("bind");
        socket.close_descriptor().await.expect("close")
    });

    assert_eq!(outcome, CloseOutcome::Closed);
}

/// The Unix socket types repeat the `Arc::try_unwrap` shape once each, and the
/// portable tests above cannot reach any of those copies.
#[cfg(unix)]
mod unix {
    use super::{CloseOutcome, StreamExt, block_on};
    use runite::net::unix::{UnixDatagram, UnixListener, UnixStream};
    use std::path::PathBuf;

    /// Short, and under the system temp directory: a path under `target/`
    /// exceeds `sun_path` once the crate is unpacked for packaging.
    fn socket_path(name: &str) -> PathBuf {
        let path = std::env::temp_dir().join(format!("rn-cl-{}-{name}", std::process::id()));
        let _ = std::fs::remove_file(&path);
        path
    }

    #[test]
    fn closing_a_sole_owner_stream_closes_the_descriptor() {
        let path = socket_path("st");
        let bound = path.clone();

        let outcome = block_on(move || async move {
            let listener = UnixListener::bind(&bound).expect("bind");
            let server = runite::spawn(async move { listener.accept().await });
            let stream = UnixStream::connect(&bound).await.expect("connect");
            let outcome = stream.close_descriptor().await.expect("close");
            server.await.expect("server task").expect("accept");
            outcome
        });

        assert_eq!(outcome, CloseOutcome::Closed);
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn closing_a_sole_owner_listener_closes_the_descriptor() {
        let path = socket_path("ls");
        let bound = path.clone();

        let outcome = block_on(move || async move {
            let listener = UnixListener::bind(&bound).expect("bind");
            listener.close_descriptor().await.expect("close")
        });

        assert_eq!(outcome, CloseOutcome::Closed);
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn closing_a_listener_with_a_live_incoming_reports_still_shared() {
        let path = socket_path("li");
        let bound = path.clone();

        let outcome = block_on(move || async move {
            let listener = UnixListener::bind(&bound).expect("bind");
            let mut incoming = listener.incoming();
            let outcome = listener.close_descriptor().await.expect("close");

            // The refused close must leave the remaining holder usable.
            let client = runite::spawn(async move { UnixStream::connect(&bound).await });
            incoming
                .next()
                .await
                .expect("incoming is infinite")
                .expect("the refused close must leave the listener accepting");
            client.await.expect("client task").expect("connect");
            outcome
        });

        assert_eq!(outcome, CloseOutcome::StillShared);
        let _ = std::fs::remove_file(&path);
    }

    /// A datagram socket owns its descriptor outright — it has no `Arc` and no
    /// way to be split — so `Closed` is unconditional. Pinned here so that
    /// giving it a shareable handle without revisiting the outcome fails.
    #[test]
    fn closing_a_datagram_socket_always_reports_closed() {
        let path = socket_path("dg");
        let bound = path.clone();

        let outcome = block_on(move || async move {
            let socket = UnixDatagram::bind(&bound).expect("bind");
            socket.close_descriptor().await.expect("close")
        });

        assert_eq!(outcome, CloseOutcome::Closed);
        let _ = std::fs::remove_file(&path);
    }
}
