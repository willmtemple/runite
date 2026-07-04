//! End-to-end TCP and UDP tests exercising the public `runite::net` API across
//! the runtime event loop.

mod common;

use common::block_on;
use runite::net::{TcpListener, TcpSocket, TcpStream, UdpSocket};
use std::net::Shutdown;

#[test]
fn tcp_echo_roundtrip() {
    block_on(|| async {
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind listener");
        let addr = listener.local_addr().expect("listener local addr");

        let server = runite::spawn(async move {
            let (mut stream, _peer) = listener.accept().await.expect("accept connection");
            let mut buf = [0u8; 32];
            let n = stream.read(&mut buf).await.expect("server read");
            stream.write_all(&buf[..n]).await.expect("server echo");
        });

        let mut client = TcpStream::connect(addr).await.expect("client connect");
        client
            .write_all(b"hello runite")
            .await
            .expect("client write");
        let mut got = [0u8; 32];
        client
            .read_exact(&mut got[..12])
            .await
            .expect("client read");
        assert_eq!(&got[..12], b"hello runite");

        server.await.expect("server task should not be aborted");
    });
}

#[test]
fn tcp_large_transfer_in_chunks() {
    block_on(|| async {
        let payload: Vec<u8> = (0..64 * 1024).map(|i| (i % 251) as u8).collect();
        let expected = payload.clone();

        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind listener");
        let addr = listener.local_addr().expect("listener local addr");

        let server = runite::spawn(async move {
            let (mut stream, _peer) = listener.accept().await.expect("accept connection");
            let mut received = Vec::new();
            let mut buf = [0u8; 4096];
            loop {
                let n = stream.read(&mut buf).await.expect("server read");
                if n == 0 {
                    break;
                }
                received.extend_from_slice(&buf[..n]);
            }
            received
        });

        let mut client = TcpStream::connect(addr).await.expect("client connect");
        client.write_all(&payload).await.expect("client write");
        client
            .shutdown(Shutdown::Write)
            .await
            .expect("client shutdown write");

        let received = server.await.expect("server task should not be aborted");
        assert_eq!(received, expected);
    });
}

#[test]
fn tcp_socket_accept_connect_smoke() {
    block_on(|| async {
        let socket = TcpSocket::new_v4().expect("create socket");
        socket.set_reuseaddr(true).expect("set reuseaddr");
        assert!(socket.reuseaddr().expect("read reuseaddr"));
        socket
            .bind("127.0.0.1:0".parse().expect("parse bind addr"))
            .expect("bind socket");
        let listener = socket.listen(128).expect("listen socket");
        let addr = listener.local_addr().expect("listener local addr");

        let server = runite::spawn(async move {
            let (mut stream, _peer) = listener.accept().await.expect("accept connection");
            let mut buf = [0u8; 4];
            stream.read_exact(&mut buf).await.expect("server read");
            stream.write_all(b"pong").await.expect("server write");
        });

        let client_socket = TcpSocket::new_v4().expect("create client socket");
        let mut client = client_socket.connect(addr).await.expect("client connect");
        client.write_all(b"ping").await.expect("client write");
        let mut got = [0u8; 4];
        client.read_exact(&mut got).await.expect("client read");
        assert_eq!(&got, b"pong");

        server.await.expect("server task should not be aborted");
    });
}

#[cfg(unix)]
#[test]
fn tcp_socket_reuseport_allows_two_listeners_on_same_port() {
    block_on(|| async {
        let first = TcpSocket::new_v4().expect("create first socket");
        first.set_reuseaddr(true).expect("first reuseaddr");
        first.set_reuseport(true).expect("first reuseport");
        assert!(first.reuseport().expect("first read reuseport"));
        first
            .bind("127.0.0.1:0".parse().expect("parse first bind addr"))
            .expect("bind first socket");
        let addr = first.local_addr().expect("first local addr");

        let second = TcpSocket::new_v4().expect("create second socket");
        second.set_reuseaddr(true).expect("second reuseaddr");
        second.set_reuseport(true).expect("second reuseport");
        assert!(second.reuseport().expect("second read reuseport"));
        second
            .bind(addr)
            .expect("bind second socket to shared port");

        let first_listener = first.listen(128).expect("listen first socket");
        let second_listener = second.listen(128).expect("listen second socket");
        assert_eq!(
            first_listener.local_addr().expect("first listener addr"),
            addr
        );
        assert_eq!(
            second_listener.local_addr().expect("second listener addr"),
            addr
        );

        let client_socket = TcpSocket::new_v4().expect("create client socket");
        let _client = client_socket
            .connect(addr)
            .await
            .expect("connect shared port");
    });
}

/// `SO_REUSEPORT` has no Windows equivalent; the builder must report
/// `Unsupported` rather than silently misconfiguring the socket.
#[cfg(windows)]
#[test]
fn tcp_socket_reuseport_reports_unsupported() {
    block_on(|| async {
        let socket = TcpSocket::new_v4().expect("create socket");
        let error = socket
            .set_reuseport(true)
            .expect_err("set_reuseport should be unsupported on Windows");
        assert_eq!(error.kind(), std::io::ErrorKind::Unsupported);
        let error = socket
            .reuseport()
            .expect_err("reuseport should be unsupported on Windows");
        assert_eq!(error.kind(), std::io::ErrorKind::Unsupported);
    });
}

#[test]
fn udp_send_to_recv_from() {
    block_on(|| async {
        let server = UdpSocket::bind("127.0.0.1:0").await.expect("bind server");
        let server_addr = server.local_addr().expect("server local addr");
        let client = UdpSocket::bind("127.0.0.1:0").await.expect("bind client");

        let sent = client
            .send_to(b"ping", server_addr)
            .await
            .expect("client send_to");
        assert_eq!(sent, 4);

        let mut buf = [0u8; 16];
        let (n, peer) = server.recv_from(&mut buf).await.expect("server recv_from");
        assert_eq!(&buf[..n], b"ping");

        let replied = server.send_to(b"pong", peer).await.expect("server reply");
        assert_eq!(replied, 4);

        let mut reply = [0u8; 16];
        let (n, _from) = client
            .recv_from(&mut reply)
            .await
            .expect("client recv_from");
        assert_eq!(&reply[..n], b"pong");
    });
}

#[test]
fn udp_connected_send_recv() {
    block_on(|| async {
        let server = UdpSocket::bind("127.0.0.1:0").await.expect("bind server");
        let server_addr = server.local_addr().expect("server local addr");
        let client = UdpSocket::bind("127.0.0.1:0").await.expect("bind client");
        client.connect(server_addr).await.expect("client connect");

        client.send(b"datagram").await.expect("client send");
        let mut buf = [0u8; 16];
        let (n, peer) = server.recv_from(&mut buf).await.expect("server recv_from");
        assert_eq!(&buf[..n], b"datagram");

        server.send_to(b"ack", peer).await.expect("server send ack");
        let mut ack = [0u8; 16];
        let n = client.recv(&mut ack).await.expect("client recv");
        assert_eq!(&ack[..n], b"ack");
    });
}
