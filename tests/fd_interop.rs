//! Tests for file-descriptor interop (release plan 3.3): `AsFd`/`AsRawFd`,
//! `From<OwnedFd>`, and `from_std` on the fd-backed I/O types, plus the
//! `impl AsFd` readiness helpers. These are Unix-only.

#![cfg(unix)]

use std::os::fd::{AsFd, AsRawFd, OwnedFd};

/// A `std::net::TcpListener` adopted via `from_std` is switched to non-blocking
/// mode and works as a runite listener (accepts a connection driven by the
/// event loop), and the fd accessors return the live descriptor.
#[runite::test]
async fn tcp_listener_from_std_accepts() {
    let std_listener = std::net::TcpListener::bind("127.0.0.1:0").expect("std bind");
    let addr = std_listener.local_addr().expect("local addr");

    let listener = runite::net::TcpListener::from_std(std_listener).expect("from_std");
    assert!(listener.as_raw_fd() >= 0);
    assert_eq!(listener.as_fd().as_raw_fd(), listener.as_raw_fd());

    let server = runite::spawn(async move {
        let (stream, _peer) = listener.accept().await.expect("accept");
        // The accepted stream also exposes its descriptor.
        assert!(stream.as_raw_fd() >= 0);
    });

    let _client = runite::net::TcpStream::connect(addr).await.expect("connect");
    server.await.expect("server task");
}

/// `From<OwnedFd>` adopts a raw owned descriptor without touching its mode.
#[runite::test]
async fn tcp_listener_from_owned_fd_constructs() {
    let std_listener = std::net::TcpListener::bind("127.0.0.1:0").expect("std bind");
    let owned = OwnedFd::from(std_listener);
    let raw = owned.as_raw_fd();

    let listener = runite::net::TcpListener::from(owned);
    assert_eq!(listener.as_raw_fd(), raw);
}

/// A `std::fs::File` adopted via `from_std` reads through the runite driver.
#[runite::test]
async fn file_from_std_reads() {
    use std::io::Write;

    let path = std::env::temp_dir().join(format!("runite-fd-interop-{}", std::process::id()));
    {
        let mut file = std::fs::File::create(&path).expect("create fixture");
        file.write_all(b"adopted").expect("write fixture");
    }

    let std_file = std::fs::File::open(&path).expect("std open");
    let mut file = runite::fs::File::from_std(std_file);
    assert!(file.as_raw_fd() >= 0);

    let mut contents = Vec::new();
    file.read_to_end(&mut contents).await.expect("read adopted file");
    assert_eq!(contents, b"adopted");

    std::fs::remove_file(&path).expect("cleanup");
}
