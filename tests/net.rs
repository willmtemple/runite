//! End-to-end TCP and UDP tests exercising the public `runite::net` API across
//! the runtime event loop.

mod common;

use common::block_on;
use runite::net::{TcpListener, TcpStream, UdpSocket};
use std::net::Shutdown;

#[test]
fn tcp_echo_roundtrip() {
    block_on(|| async {
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind listener");
        let addr = listener.local_addr().expect("listener local addr");

        let server = runite::queue_future(async move {
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

        let server = runite::queue_future(async move {
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
