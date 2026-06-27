//! Integration tests for public I/O copy helpers.

mod common;

use common::block_on;
use runite::net::{TcpListener, TcpStream};
use std::net::Shutdown;

#[test]
fn copy_bidirectional_proxies_tcp_roundtrip() {
    block_on(|| async {
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind proxy listener");
        let addr = listener.local_addr().expect("proxy listener address");

        let proxy = runite::spawn(async move {
            let (mut left, _) = listener.accept().await.expect("accept left client");
            let (mut right, _) = listener.accept().await.expect("accept right client");
            runite::io::copy_bidirectional(&mut left, &mut right)
                .await
                .expect("proxy copy should succeed")
        });

        let mut left = TcpStream::connect(addr).await.expect("connect left client");
        let mut right = TcpStream::connect(addr)
            .await
            .expect("connect right client");

        left.write_all(b"through the proxy")
            .await
            .expect("left write payload");
        left.shutdown(Shutdown::Write)
            .await
            .expect("left shutdown write");

        let mut proxied = [0; 17];
        right
            .read_exact(&mut proxied)
            .await
            .expect("right read proxied payload");
        assert_eq!(&proxied, b"through the proxy");

        right
            .write_all(b"reply payload")
            .await
            .expect("right write reply");
        right
            .shutdown(Shutdown::Write)
            .await
            .expect("right shutdown write");

        let mut reply = [0; 13];
        left.read_exact(&mut reply).await.expect("left read reply");
        assert_eq!(&reply, b"reply payload");

        let copied = proxy.await.expect("proxy task should not be aborted");
        assert_eq!(copied, (17, 13));
    });
}
