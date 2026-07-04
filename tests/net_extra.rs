//! Additional deterministic networking coverage for the public runite net API.

mod common;

use common::block_on;
use runite::io::{AsyncReadExt, AsyncWriteExt, StreamExt};
use runite::net::{TcpListener, TcpSocket, TcpStream, UdpSocket};
use std::io::ErrorKind;
use std::net::SocketAddr;
use std::time::Duration;

#[test]
fn tcp_split_reunite_rejects_mismatched_halves_and_preserves_them() {
    block_on(|| async {
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind listener");
        let addr = listener.local_addr().expect("listener address");

        let server = runite::spawn(async move {
            let (first, _) = listener.accept().await.expect("accept first");
            let (second, _) = listener.accept().await.expect("accept second");
            (first, second)
        });

        let first = TcpStream::connect(addr)
            .await
            .expect("connect first client");
        let second = TcpStream::connect(addr)
            .await
            .expect("connect second client");
        let (first_read, first_write) = first.into_split();
        let (second_read, second_write) = second.into_split();

        let err = match TcpStream::reunite(first_read, second_write) {
            Ok(_) => panic!("halves from different streams must not reunite"),
            Err(error) => error,
        };
        assert_eq!(
            err.to_string(),
            "the provided halves are not from the same TcpStream"
        );

        let first_read = err.0;
        let second_write = err.1;
        TcpStream::reunite(first_read, first_write).expect("first halves still reunite");
        second_write
            .reunite(second_read)
            .expect("second halves still reunite");

        let (_accepted_first, _accepted_second) = server.await.expect("server task");
    });
}

#[test]
fn tcp_split_halves_read_write_shutdown_and_report_addresses() {
    block_on(|| async {
        let listener = TcpListener::bind(SocketAddr::from(([127, 0, 0, 1], 0)))
            .await
            .expect("bind listener");
        let addr = listener.local_addr().expect("listener address");

        let server = runite::spawn(async move {
            let (mut stream, peer) = listener.accept().await.expect("accept");
            assert_eq!(peer.ip(), addr.ip());
            let mut request = [0; 4];
            stream.read_exact(&mut request).await.expect("server read");
            stream.write_all(b"pong").await.expect("server write");
            let mut eof = [0; 1];
            let read = stream.read(&mut eof).await.expect("server eof read");
            (request, read)
        });

        let client = TcpStream::connect(addr).await.expect("client connect");
        let (mut read_half, mut write_half) = client.into_split();
        assert_eq!(read_half.peer_addr().expect("read peer"), addr);
        assert_eq!(write_half.peer_addr().expect("write peer"), addr);
        assert_eq!(
            read_half.local_addr().expect("read local"),
            write_half.local_addr().expect("write local")
        );

        let writer = runite::spawn(async move {
            write_half.write_all(b"ping").await.expect("split write");
            write_half.shutdown().await.expect("split shutdown");
            write_half
        });

        let mut response = [0; 4];
        read_half
            .read_exact(&mut response)
            .await
            .expect("split read");
        assert_eq!(&response, b"pong");
        let write_half = writer.await.expect("writer task");
        TcpStream::reunite(read_half, write_half).expect("matching halves reunite");

        let (request, eof_read) = server.await.expect("server task");
        assert_eq!(&request, b"ping");
        assert_eq!(eof_read, 0);
    });
}

#[test]
fn tcp_read_and_write_timeouts_surface_timed_out() {
    block_on(|| async {
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind listener");
        let addr = listener.local_addr().expect("listener address");
        let server = runite::spawn(async move {
            let (stream, _) = listener.accept().await.expect("accept");
            stream
        });

        let mut client = TcpStream::connect(addr).await.expect("connect client");
        client
            .set_read_timeout(Some(Duration::from_millis(5)))
            .expect("set read timeout");
        assert_eq!(
            client.read_timeout().expect("read timeout"),
            Some(Duration::from_millis(5))
        );
        assert_eq!(
            client
                .set_read_timeout(Some(Duration::ZERO))
                .expect_err("zero read timeout")
                .kind(),
            ErrorKind::InvalidInput
        );
        let mut byte = [0; 1];
        let read_error = client
            .read(&mut byte)
            .await
            .expect_err("read should time out");
        assert_eq!(read_error.kind(), ErrorKind::TimedOut);

        client
            .set_write_timeout(Some(Duration::from_millis(5)))
            .expect("set write timeout");
        assert_eq!(
            client.write_timeout().expect("write timeout"),
            Some(Duration::from_millis(5))
        );
        assert_eq!(
            client
                .set_write_timeout(Some(Duration::ZERO))
                .expect_err("zero write timeout")
                .kind(),
            ErrorKind::InvalidInput
        );
        let chunk = vec![0xa5; 1024 * 1024];
        let mut writes = 0usize;
        loop {
            match client.write(&chunk).await {
                Ok(0) => panic!("write returned zero before timing out"),
                Ok(_) => writes += 1,
                Err(error) if error.kind() == ErrorKind::TimedOut => break,
                Err(error) => panic!("unexpected write error: {error:?}"),
            }
            assert!(
                writes < 256,
                "loopback send buffer did not fill before test bound"
            );
        }

        let _held_server_stream = server.await.expect("server task");
    });
}

#[test]
fn tcp_listener_incoming_yields_local_connections() {
    block_on(|| async {
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind listener");
        let addr = listener.local_addr().expect("listener address");

        let server = runite::spawn(async move {
            let mut incoming = listener.incoming();
            let mut peers = Vec::new();
            for _ in 0..2 {
                let stream = incoming
                    .next()
                    .await
                    .expect("incoming is infinite")
                    .expect("incoming accept");
                peers.push(stream.peer_addr().expect("peer addr"));
            }
            peers
        });

        let first = TcpStream::connect(addr).await.expect("connect first");
        let second = TcpStream::connect(addr).await.expect("connect second");
        drop((first, second));

        let peers = server.await.expect("server task");
        assert_eq!(peers.len(), 2);
        assert!(peers.iter().all(|peer| peer.ip() == addr.ip()));
    });
}

#[test]
fn tcp_socket_options_listen_connect_and_refused_error() {
    let closed_listener = std::net::TcpListener::bind("127.0.0.1:0").expect("reserve closed port");
    let closed_addr = closed_listener.local_addr().expect("closed port addr");
    drop(closed_listener);

    block_on(move || async move {
        let refused = match TcpStream::connect(closed_addr).await {
            Ok(_) => panic!("connect to closed port should fail"),
            Err(error) => error,
        };
        assert!(
            matches!(
                refused.kind(),
                ErrorKind::ConnectionRefused | ErrorKind::BrokenPipe
            ),
            "unexpected closed-port connect error: {refused:?}"
        );

        let socket = TcpSocket::new_v4().expect("new socket");
        socket.set_reuseaddr(true).expect("set reuseaddr");
        assert!(socket.reuseaddr().expect("reuseaddr"));
        match socket.set_reuseport(true) {
            Ok(()) => assert!(socket.reuseport().expect("reuseport")),
            Err(error) if error.kind() == ErrorKind::Unsupported => {}
            Err(error) => panic!("unexpected reuseport error: {error:?}"),
        }
        socket
            .bind("127.0.0.1:0".parse().expect("parse bind addr"))
            .expect("bind socket");
        let listener = socket.listen(16).expect("listen");
        let addr = listener.local_addr().expect("listener addr");

        let server = runite::spawn(async move {
            let (mut stream, _) = listener.accept().await.expect("accept");
            let mut got = [0; 3];
            stream.read_exact(&mut got).await.expect("server read");
            stream.write_all(b"ack").await.expect("server write");
            got
        });

        let client_socket = TcpSocket::new_v4().expect("client socket");
        let mut client = client_socket.connect(addr).await.expect("socket connect");
        client.write_all(b"hey").await.expect("client write");
        let mut ack = [0; 3];
        client.read_exact(&mut ack).await.expect("client read");
        assert_eq!(&ack, b"ack");
        assert_eq!(&server.await.expect("server task"), b"hey");
    });
}

#[test]
fn udp_peek_recv_and_truncation_on_connected_socket() {
    block_on(|| async {
        let left = UdpSocket::bind("127.0.0.1:0").await.expect("bind left");
        let right = UdpSocket::bind("127.0.0.1:0").await.expect("bind right");
        let left_addr = left.local_addr().expect("left addr");
        let right_addr = right.local_addr().expect("right addr");
        left.connect(right_addr).await.expect("connect left");
        right.connect(left_addr).await.expect("connect right");
        assert_eq!(left.peer_addr().expect("left peer"), right_addr);
        assert_eq!(right.peer_addr().expect("right peer"), left_addr);

        left.send(b"abcdef").await.expect("send datagram");
        let mut small = [0; 3];
        let peeked = right.peek(&mut small).await.expect("peek");
        assert_eq!(peeked, 3);
        assert_eq!(&small, b"abc");

        let read = right.recv(&mut small).await.expect("truncated recv");
        assert_eq!(read, 3);
        assert_eq!(&small, b"abc");

        right
            .set_read_timeout(Some(Duration::from_millis(5)))
            .expect("set read timeout");
        let timeout = right
            .recv(&mut small)
            .await
            .expect_err("truncated datagram was consumed");
        assert_eq!(timeout.kind(), ErrorKind::TimedOut);
    });
}

/// A read submitted with a large buffer that is then abandoned, and later polled
/// with a smaller buffer, must not lose the received bytes: the surplus is
/// retained and served by subsequent reads. Regression for the pending-op /
/// buffer-shrink data loss.
#[test]
fn tcp_read_overflow_preserves_bytes_when_buffer_shrinks() {
    use runite::io::AsyncRead;
    use std::future::poll_fn;
    use std::pin::Pin;
    use std::task::Poll;

    let received = block_on(|| async {
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
        let addr = listener.local_addr().expect("local addr");

        let server = runite::spawn(async move {
            let (mut stream, _) = listener.accept().await.expect("accept");

            // Submit a read with a large (64-byte) buffer, then abandon that
            // poll so the recv stays stashed on the stream sized for 64 bytes.
            poll_fn(|cx| {
                let mut big = [0u8; 64];
                let _ = Pin::new(&mut stream).poll_read(cx, &mut big);
                Poll::Ready(())
            })
            .await;

            // Read 4 bytes at a time. The first small read completes the stashed
            // 64-byte recv and must keep the surplus; the overflow buffer serves
            // the rest. No byte may be lost or spuriously error.
            let mut out = Vec::new();
            while out.len() < 10 {
                let mut small = [0u8; 4];
                let n = poll_fn(|cx| Pin::new(&mut stream).poll_read(cx, &mut small))
                    .await
                    .expect("read must not error on a shrunk buffer");
                if n == 0 {
                    break;
                }
                out.extend_from_slice(&small[..n]);
            }
            out
        });

        let mut client = TcpStream::connect(addr).await.expect("connect");
        client.write_all(b"0123456789").await.expect("write");
        let out = server.await.expect("server task");
        drop(client);
        out
    });

    assert_eq!(&received, b"0123456789");
}

/// The inherent `read()` now stashes its operation on the stream (like the
/// `AsyncRead` trait path), so abandoning a read and reading again does not lose
/// bytes or leave two recvs racing. Regression for inherent-method cancellation.
#[test]
fn tcp_inherent_read_stashes_operation() {
    use std::future::poll_fn;
    use std::pin::pin;
    use std::task::Poll;

    let received = block_on(|| async {
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
        let addr = listener.local_addr().expect("local addr");

        let server = runite::spawn(async move {
            let (mut stream, _) = listener.accept().await.expect("accept");

            // Submit an inherent read, then abandon it after a single poll. The
            // recv is stashed on the stream rather than owned by this future.
            {
                let mut scratch = [0u8; 16];
                let mut fut = pin!(stream.read(&mut scratch));
                poll_fn(|cx| {
                    let _ = fut.as_mut().poll(cx);
                    Poll::Ready(())
                })
                .await;
            }

            // The next read observes the peer's bytes via the stashed op.
            let mut out = [0u8; 16];
            let n = stream.read(&mut out).await.expect("read");
            out[..n].to_vec()
        });

        let mut client = TcpStream::connect(addr).await.expect("connect");
        client.write_all(b"hello").await.expect("write");
        let out = server.await.expect("server task");
        drop(client);
        out
    });

    assert_eq!(&received, b"hello");
}

#[cfg(feature = "hyper")]
#[test]
fn hyper_http1_client_uses_runite_tcp_stream() {
    use bytes::Bytes;
    use http_body_util::{BodyExt, Empty};
    use hyper::Request;

    block_on(|| async {
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind listener");
        let addr = listener.local_addr().expect("listener address");

        let server = runite::spawn(async move {
            let (mut stream, _) = listener.accept().await.expect("accept");
            let mut request = [0; 1024];
            let read = stream
                .read(&mut request)
                .await
                .expect("server read request");
            assert!(request[..read].windows(4).any(|window| window == b"GET "));
            assert!(request[..read].windows(6).any(|window| window == b"/hello"));
            stream
                .write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\nConnection: close\r\n\r\nhi")
                .await
                .expect("server write response");
        });

        let stream = TcpStream::connect(addr).await.expect("connect client");
        let (mut sender, connection) = hyper::client::conn::http1::handshake(stream)
            .await
            .expect("hyper handshake");
        let connection_task = runite::spawn(connection);

        let request = Request::builder()
            .method("GET")
            .uri(format!("http://{addr}/hello"))
            .header("host", addr.to_string())
            .body(Empty::<Bytes>::new())
            .expect("build request");
        let response = sender.send_request(request).await.expect("send request");
        assert_eq!(response.status(), hyper::StatusCode::OK);
        let body = response
            .into_body()
            .collect()
            .await
            .expect("collect body")
            .to_bytes();
        assert_eq!(&body[..], b"hi");

        server.await.expect("server task");
        connection_task
            .await
            .expect("connection task")
            .expect("hyper connection completes");
    });
}

#[cfg(unix)]
mod unix_extra {
    use std::path::PathBuf;
    use std::sync::atomic::{AtomicU64, Ordering};

    use super::*;
    use runite::net::unix::{UnixDatagram, UnixListener, UnixStream};
    use std::path::Path;

    #[test]
    fn unix_stream_connect_incoming_round_trip_and_eof() {
        let path = unique_socket_path("stream-incoming");
        remove_socket_file(&path);
        let path_for_test = path.clone();

        block_on(move || async move {
            let listener = UnixListener::bind(&path_for_test).expect("bind unix listener");
            assert_eq!(
                listener.local_addr().expect("local addr").as_pathname(),
                Some(path_for_test.as_path())
            );

            let server = runite::spawn(async move {
                let mut incoming = listener.incoming();
                let mut stream = incoming
                    .next()
                    .await
                    .expect("incoming is infinite")
                    .expect("accept incoming stream");
                let mut got = [0; 4];
                stream.read(&mut got).await.expect("server read");
                stream.write_all(b"pong").await.expect("server write");
                let mut eof = [0; 1];
                let eof_read = stream.read(&mut eof).await.expect("server eof");
                (got, eof_read)
            });

            let mut client = UnixStream::connect(&path_for_test)
                .await
                .expect("connect unix stream");
            assert!(
                client
                    .local_addr()
                    .expect("client local")
                    .as_pathname()
                    .is_none()
            );
            client.write_all(b"ping").await.expect("client write");
            let mut response = [0; 4];
            let read = client.read(&mut response).await.expect("client read");
            assert_eq!(read, 4);
            assert_eq!(&response, b"pong");
            drop(client);

            let (request, eof_read) = server.await.expect("server task");
            assert_eq!(&request, b"ping");
            assert_eq!(eof_read, 0);
        });

        remove_socket_file(&path);
    }

    #[test]
    fn unix_stream_connect_to_missing_path_errors() {
        let path = unique_socket_path("missing-stream");
        remove_socket_file(&path);

        block_on(move || async move {
            let error = match UnixStream::connect(&path).await {
                Ok(_) => panic!("missing stream path should fail"),
                Err(error) => error,
            };
            assert_eq!(error.kind(), ErrorKind::NotFound);
        });
    }

    #[test]
    fn unix_datagram_send_to_recv_from_truncates_and_reports_sender() {
        let server_path = unique_socket_path("dgram-server");
        let client_path = unique_socket_path("dgram-client");
        remove_socket_file(&server_path);
        remove_socket_file(&client_path);
        let cleanup_server = server_path.clone();
        let cleanup_client = client_path.clone();

        block_on(move || async move {
            let server = UnixDatagram::bind(&server_path).expect("bind server datagram");
            let client = UnixDatagram::bind(&client_path).expect("bind client datagram");
            let sent = client
                .send_to(b"abcdef", &server_path)
                .await
                .expect("send_to server");
            assert_eq!(sent, 6);

            let mut small = [0; 3];
            let (read, peer) = server
                .recv_from(&mut small)
                .await
                .expect("recv_from server");
            assert_eq!(read, 3);
            assert_eq!(&small, b"abc");
            assert_eq!(peer.as_pathname(), Some(client_path.as_path()));
        });

        remove_socket_file(&cleanup_server);
        remove_socket_file(&cleanup_client);
    }

    #[test]
    fn unix_datagram_connected_pair_round_trip() {
        block_on(|| async {
            let (left, right) = UnixDatagram::pair().expect("datagram pair");
            left.send(b"left").await.expect("left send");
            let mut got = [0; 8];
            let read = right.recv(&mut got).await.expect("right recv");
            assert_eq!(&got[..read], b"left");
            right.send(b"right").await.expect("right send");
            let read = left.recv(&mut got).await.expect("left recv");
            assert_eq!(&got[..read], b"right");
        });
    }

    #[test]
    fn unix_datagram_connect_to_path_and_unbound_send() {
        let server_path = unique_socket_path("dgram-connected-server");
        remove_socket_file(&server_path);
        let cleanup_server = server_path.clone();

        block_on(move || async move {
            let server = UnixDatagram::bind(&server_path).expect("bind server datagram");
            let client = UnixDatagram::unbound().expect("unbound datagram");
            client
                .connect(&server_path)
                .await
                .expect("connect datagram");
            client.send(b"hello").await.expect("connected send");
            let mut got = [0; 8];
            let (read, peer) = server.recv_from(&mut got).await.expect("server recv");
            assert_eq!(&got[..read], b"hello");
            assert!(peer.as_pathname().is_none());
        });

        remove_socket_file(&cleanup_server);
    }

    fn unique_socket_path(_label: &str) -> PathBuf {
        static COUNTER: AtomicU64 = AtomicU64::new(0);
        let root = std::env::current_dir()
            .expect("current dir")
            .join("target")
            .join("nuds");
        std::fs::create_dir_all(&root).expect("create unix socket test directory");
        root.join(format!(
            "u{}-{}.sock",
            std::process::id(),
            COUNTER.fetch_add(1, Ordering::Relaxed)
        ))
    }

    fn remove_socket_file(path: &Path) {
        let _ = std::fs::remove_file(path);
    }
}
