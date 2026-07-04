//! `UnixStream` async I/O trait parity with `TcpStream`: `AsyncRead`/
//! `AsyncWrite`, `shutdown`, and owned split halves.

#![cfg(unix)]

use runite::io::{AsyncReadExt, AsyncWriteExt};
use runite::net::unix::UnixStream;

/// `write_all` + `shutdown` on one end, `read_to_end` (an `AsyncReadExt` method,
/// so it exercises the `AsyncRead` impl) draining to EOF on the other.
#[runite::test]
async fn async_read_write_and_shutdown() {
    let (mut a, mut b) = UnixStream::pair().expect("pair");

    let writer = runite::spawn(async move {
        a.write_all(b"streamed bytes").await.expect("write_all");
        a.shutdown(std::net::Shutdown::Write)
            .await
            .expect("shutdown");
    });

    let mut out = Vec::new();
    b.read_to_end(&mut out).await.expect("read_to_end");
    writer.await.expect("writer task");

    assert_eq!(out, b"streamed bytes");
}

/// The owned read and write halves operate concurrently on the same socket.
#[runite::test]
async fn split_halves_read_and_write() {
    let (a, b) = UnixStream::pair().expect("pair");
    let (mut read_a, mut write_a) = a.into_split();
    let mut b = b;

    let echo = runite::spawn(async move {
        let mut buf = vec![0u8; 4];
        b.read_exact(&mut buf).await.expect("server read");
        b.write_all(&buf).await.expect("server echo");
    });

    write_a.write_all(b"ping").await.expect("client write");
    let mut back = vec![0u8; 4];
    read_a.read_exact(&mut back).await.expect("client read");
    echo.await.expect("echo task");

    assert_eq!(&back, b"ping");
}

/// Matching halves reunite; halves from different streams do not.
#[runite::test]
async fn reunite_checks_origin() {
    let (a, _a_peer) = UnixStream::pair().expect("pair");
    let (read, write) = a.into_split();
    assert!(UnixStream::reunite(read, write).is_ok());

    let (b, _b_peer) = UnixStream::pair().expect("pair");
    let (c, _c_peer) = UnixStream::pair().expect("pair");
    let (read_b, _write_b) = b.into_split();
    let (_read_c, write_c) = c.into_split();
    assert!(UnixStream::reunite(read_b, write_c).is_err());
}
