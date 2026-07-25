//! End-to-end regression test for raw `AsyncWrite::poll_write` ownership.
//!
//! The unit tests in `io::pending` cover the state machine directly. This test
//! drives the whole stack over a real TCP connection on every platform, because
//! the failure it guards against is silent: a write reports a byte count it
//! never wrote, and nothing else in the system notices.

mod common;

use std::future::poll_fn;
use std::io::Read as _;
use std::net::TcpListener;
use std::pin::Pin;
use std::task::Poll;
use std::time::Duration;

use common::block_on;
use runite::io::AsyncWrite;
use runite::net::TcpStream;

/// Bytes per attempt while filling the peer's receive window. Large enough to
/// reach a blocked write in a handful of iterations on every platform.
const FILL_CHUNK: usize = 4 * 1024 * 1024;
/// Bounded so the test fails fast rather than looping forever if a platform
/// accepts arbitrarily large writes.
const MAX_FILL_ATTEMPTS: usize = 64;

/// A raw `poll_write` that is abandoned mid-flight must not have its byte count
/// credited to the next write, which carries entirely different bytes.
///
/// Before the pending-ownership fix, every `AsyncWrite::poll_write` impl passed
/// the same untracked generation, so the second write below matched the first
/// one's in-flight operation and returned its count -- reporting megabytes
/// written for a sixteen-byte buffer, while those sixteen bytes were never
/// submitted at all.
#[test]
fn abandoned_raw_write_is_not_credited_to_a_different_buffer() {
    let listener = TcpListener::bind("127.0.0.1:0").expect("bind loopback listener");
    let addr = listener.local_addr().expect("listener address");

    // The peer accepts but does not read until signalled, so the client's
    // writes back up and an operation stays in flight.
    let (drain_tx, drain_rx) = std::sync::mpsc::channel::<()>();
    let peer = std::thread::spawn(move || {
        let (mut socket, _) = listener.accept().expect("accept client");
        drain_rx.recv().expect("drain signal");
        let mut sink = vec![0u8; 256 * 1024];
        // Drain until the client closes, so the pending write can complete.
        while socket.read(&mut sink).map(|read| read > 0).unwrap_or(false) {}
        socket
    });

    let small = *b"sixteen bytes!!!";
    let reported = block_on(move || async move {
        let mut stream = TcpStream::connect(addr).await.expect("connect to peer");
        let bulk = vec![b'A'; FILL_CHUNK];
        let mut attempts = 0usize;
        let mut blocked = false;

        // Submit bulk writes until one stays in flight, then abandon it by
        // never polling that buffer again.
        poll_fn(|cx| {
            loop {
                match Pin::new(&mut stream).poll_write(cx, &bulk) {
                    Poll::Ready(Ok(_)) => {
                        attempts += 1;
                        assert!(
                            attempts < MAX_FILL_ATTEMPTS,
                            "peer accepted {MAX_FILL_ATTEMPTS} chunks without blocking"
                        );
                        continue;
                    }
                    Poll::Ready(Err(error)) => panic!("bulk write failed: {error}"),
                    Poll::Pending => {
                        blocked = true;
                        return Poll::Ready(());
                    }
                }
            }
        })
        .await;
        assert!(blocked, "the bulk write should have been left in flight");

        // Let the peer drain so the abandoned operation can complete.
        drain_tx.send(()).expect("signal peer to drain");

        // Now a different, much smaller buffer. Whatever this reports must
        // describe *these* bytes.
        let written = poll_fn(|cx| Pin::new(&mut stream).poll_write(cx, &small)).await;
        written.expect("small write should succeed")
    });

    assert!(
        reported <= small.len(),
        "poll_write reported {reported} bytes for a {}-byte buffer; an abandoned \
         write's count was credited to different bytes",
        small.len()
    );
    assert!(reported > 0, "the small write should have made progress");

    let _ = peer.join();
}

/// `flush()` must not report success while a write is still in flight.
///
/// The pending-ownership model deliberately keeps an abandoned write owned by
/// the socket, so a flush that returns `Ok` unconditionally tells the caller
/// bytes are visible when they are not, and discards that operation's error.
#[test]
fn flush_waits_for_an_abandoned_write() {
    let listener = TcpListener::bind("127.0.0.1:0").expect("bind loopback listener");
    let addr = listener.local_addr().expect("listener address");

    let (drain_tx, drain_rx) = std::sync::mpsc::channel::<()>();
    let peer = std::thread::spawn(move || {
        let (mut socket, _) = listener.accept().expect("accept client");
        drain_rx.recv().expect("drain signal");
        let mut sink = vec![0u8; 256 * 1024];
        while socket.read(&mut sink).map(|read| read > 0).unwrap_or(false) {}
    });

    let flush_was_pending = block_on(move || async move {
        let mut stream = TcpStream::connect(addr).await.expect("connect to peer");
        let bulk = vec![b'A'; FILL_CHUNK];
        let mut attempts = 0usize;

        // Leave one write in flight, then abandon it.
        poll_fn(|cx| {
            loop {
                match Pin::new(&mut stream).poll_write(cx, &bulk) {
                    Poll::Ready(Ok(_)) => {
                        attempts += 1;
                        assert!(attempts < MAX_FILL_ATTEMPTS, "peer never blocked");
                        continue;
                    }
                    Poll::Ready(Err(error)) => panic!("bulk write failed: {error}"),
                    Poll::Pending => return Poll::Ready(()),
                }
            }
        })
        .await;

        // The very next flush poll must not claim success: the write is still
        // outstanding and the peer is not reading yet.
        let mut observed_pending = false;
        let first = poll_fn(|cx| {
            let poll = Pin::new(&mut stream).poll_flush(cx);
            observed_pending = poll.is_pending();
            Poll::Ready(())
        });
        first.await;

        drain_tx.send(()).expect("signal peer to drain");
        poll_fn(|cx| Pin::new(&mut stream).poll_flush(cx))
            .await
            .expect("flush should succeed once the write completes");
        observed_pending
    });

    assert!(
        flush_was_pending,
        "flush reported success while a write was still in flight"
    );
    let _ = peer.join();
}

/// The companion property: re-polling the same buffer is the documented
/// contract for a raw caller and must still resolve to that write's own result.
#[test]
fn repolled_raw_write_resolves_to_its_own_result() {
    let listener = TcpListener::bind("127.0.0.1:0").expect("bind loopback listener");
    let addr = listener.local_addr().expect("listener address");
    let peer = std::thread::spawn(move || {
        let (mut socket, _) = listener.accept().expect("accept client");
        let mut sink = vec![0u8; 64 * 1024];
        let mut total = 0usize;
        while let Ok(read) = socket.read(&mut sink) {
            if read == 0 {
                break;
            }
            total += read;
        }
        total
    });

    let payload = *b"a modest payload";
    let written = block_on(move || async move {
        let mut stream = TcpStream::connect(addr).await.expect("connect to peer");
        let written = poll_fn(|cx| Pin::new(&mut stream).poll_write(cx, &payload))
            .await
            .expect("write should succeed");
        // Close so the peer's read loop terminates.
        drop(stream);
        written
    });

    assert_eq!(written, payload.len());
    // Give the peer a moment to observe the close on slower CI hosts.
    std::thread::sleep(Duration::from_millis(50));
    let received = peer.join().expect("peer thread should finish");
    assert_eq!(received, payload.len(), "the peer must receive those bytes");
}
