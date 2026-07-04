//! End-to-end tests for the hyper runtime glue: serving HTTP with hyper on a
//! runite thread using `RuniteExecutor` + `RuniteTimer`.

#![cfg(feature = "hyper")]

use std::convert::Infallible;

use bytes::Bytes;
use http_body_util::{BodyExt, Empty, Full};
use hyper::service::service_fn;
use hyper::{Request, Response};
use runite::hyper_rt::{RuniteExecutor, RuniteTimer};
use runite::io::AsyncReadExt;
use runite::net::{TcpListener, TcpStream};

async fn hello(_req: Request<hyper::body::Incoming>) -> Result<Response<Full<Bytes>>, Infallible> {
    Ok(Response::new(Full::new(Bytes::from_static(
        b"hello from runite",
    ))))
}

/// An http1 hyper *server* on runite, with `header_read_timeout` configured —
/// which requires a timer and panics with "no timer configured" without
/// `RuniteTimer`. The armed timeout also exercises sleep registration and the
/// cancel-on-drop path (the request arrives well within the deadline).
#[runite::test]
async fn http1_server_with_timer_serves_request() {
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let addr = listener.local_addr().expect("addr");

    let server = runite::spawn(async move {
        let (stream, _peer) = listener.accept().await.expect("accept");
        hyper::server::conn::http1::Builder::new()
            .timer(RuniteTimer)
            .header_read_timeout(std::time::Duration::from_secs(5))
            .serve_connection(stream, service_fn(hello))
            .await
            .expect("serve http1 connection");
    });

    // Raw TCP client: independent of the hyper client path.
    let mut client = TcpStream::connect(addr).await.expect("connect");
    client
        .write_all(b"GET / HTTP/1.1\r\nhost: localhost\r\nconnection: close\r\n\r\n")
        .await
        .expect("write request");
    let mut response = Vec::new();
    client
        .read_to_end(&mut response)
        .await
        .expect("read response");
    let response = String::from_utf8_lossy(&response);

    assert!(response.starts_with("HTTP/1.1 200 OK"), "got: {response}");
    assert!(response.ends_with("hello from runite"), "got: {response}");
    server.await.expect("server task");
}

/// A full HTTP/2 round trip — hyper client and hyper server on the same runite
/// loop. HTTP/2 requires an executor on both sides (per-stream futures) and a
/// timer on the server, so this drives `RuniteExecutor` end to end, including
/// spawning hyper's `!Send` internal futures.
#[runite::test]
async fn http2_client_server_round_trip() {
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let addr = listener.local_addr().expect("addr");

    let server = runite::spawn(async move {
        let (stream, _peer) = listener.accept().await.expect("accept");
        // The client closes the connection after its request; hyper reports
        // that as a clean-ish shutdown or an error depending on timing, so the
        // serve result itself is not asserted.
        let _ = hyper::server::conn::http2::Builder::new(RuniteExecutor)
            .timer(RuniteTimer)
            .serve_connection(stream, service_fn(hello))
            .await;
    });

    let stream = TcpStream::connect(addr).await.expect("connect");
    let (mut sender, connection) = hyper::client::conn::http2::handshake(RuniteExecutor, stream)
        .await
        .expect("h2 handshake");
    runite::spawn(async move {
        let _ = connection.await;
    });

    let request = Request::builder()
        .method("GET")
        .uri(format!("http://{addr}/"))
        .body(Empty::<Bytes>::new())
        .expect("request");
    let response = sender.send_request(request).await.expect("send h2 request");
    assert_eq!(response.status(), 200);
    let body = response
        .into_body()
        .collect()
        .await
        .expect("collect h2 body")
        .to_bytes();
    assert_eq!(&body[..], b"hello from runite");

    drop(sender); // close the client side so the server task can finish
    server.await.expect("server task");
}

/// HTTP over a Unix-domain socket: hyper http1 server on one end of a
/// `UnixStream::pair`, hyper http1 client on the other — the Docker-socket /
/// local-RPC shape. (`net::unix` is Unix-only.)
#[cfg(unix)]
#[runite::test]
async fn http1_over_unix_stream() {
    let (server_io, client_io) = runite::net::unix::UnixStream::pair().expect("pair");

    let server = runite::spawn(async move {
        hyper::server::conn::http1::Builder::new()
            .timer(RuniteTimer)
            .serve_connection(server_io, service_fn(hello))
            .await
            .expect("serve over unix stream");
    });

    let (mut sender, connection) = hyper::client::conn::http1::handshake(client_io)
        .await
        .expect("handshake over unix stream");
    runite::spawn(async move {
        let _ = connection.await;
    });

    let request = Request::builder()
        .method("GET")
        .uri("http://localhost/")
        .header("host", "localhost")
        .header("connection", "close")
        .body(Empty::<Bytes>::new())
        .expect("request");
    let response = sender.send_request(request).await.expect("send request");
    assert_eq!(response.status(), 200);
    let body = response
        .into_body()
        .collect()
        .await
        .expect("body")
        .to_bytes();
    assert_eq!(&body[..], b"hello from runite");

    server.await.expect("server task");
}
