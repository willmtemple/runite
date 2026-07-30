//! End-to-end TLS over real loopback sockets.
//!
//! The unit tests in `src/tls` pin the record-layer contracts against an
//! in-memory transport that can be blocked on command. These run the same
//! streams over [`runite::net::TcpStream`] and the platform's completion-based
//! backend instead, which is where a buffer handed to the kernel, a partial
//! send, and a real `shutdown(2)` actually happen.

#![cfg(feature = "rustls")]

use std::sync::{Arc, OnceLock};

use rustls::pki_types::{CertificateDer, PrivateKeyDer, PrivatePkcs8KeyDer, ServerName};
use rustls::{ClientConfig, RootCertStore, ServerConfig};

use runite::io::{AsyncReadExt as _, AsyncWriteExt as _};
use runite::net::{TcpListener, TcpStream};
use runite::tls::{TlsAcceptor, TlsConnector};

/// A CA and a `localhost` leaf certificate signed by it.
///
/// A self-signed leaf would be simpler, but path building would then have to
/// treat an end-entity certificate as a trust anchor — which is not what a real
/// deployment looks like, and not what the verifier is tuned for.
fn configurations() -> (TlsConnector, TlsAcceptor) {
    static PROVIDER: OnceLock<()> = OnceLock::new();
    PROVIDER.get_or_init(|| {
        // The test binary picks `ring`; runite itself deliberately picks
        // nothing, which is the whole point of the `rustls` feature's contract.
        let _ = rustls::crypto::ring::default_provider().install_default();
    });

    let mut authority_params = rcgen::CertificateParams::new(Vec::new()).expect("CA parameters");
    authority_params.is_ca = rcgen::IsCa::Ca(rcgen::BasicConstraints::Unconstrained);
    authority_params.key_usages = vec![
        rcgen::KeyUsagePurpose::KeyCertSign,
        rcgen::KeyUsagePurpose::DigitalSignature,
    ];
    let authority_key = rcgen::KeyPair::generate().expect("CA key");
    let authority = authority_params
        .self_signed(&authority_key)
        .expect("self-signed CA");
    let issuer = rcgen::Issuer::new(authority_params, authority_key);

    let leaf_params =
        rcgen::CertificateParams::new(vec!["localhost".to_owned()]).expect("leaf parameters");
    let leaf_key = rcgen::KeyPair::generate().expect("leaf key");
    let leaf = leaf_params
        .signed_by(&leaf_key, &issuer)
        .expect("leaf signed by the CA");

    let mut roots = RootCertStore::empty();
    roots
        .add(authority.der().clone())
        .expect("the CA is a valid trust anchor");

    let chain: Vec<CertificateDer<'static>> = vec![leaf.der().clone(), authority.der().clone()];
    let key = PrivateKeyDer::Pkcs8(PrivatePkcs8KeyDer::from(leaf_key.serialize_der()));

    let client = ClientConfig::builder()
        .with_root_certificates(roots)
        .with_no_client_auth();
    let server = ServerConfig::builder()
        .with_no_client_auth()
        .with_single_cert(chain, key)
        .expect("the leaf key matches the leaf certificate");

    (
        TlsConnector::new(Arc::new(client)),
        TlsAcceptor::new(Arc::new(server)),
    )
}

fn server_name() -> ServerName<'static> {
    ServerName::try_from("localhost").expect("valid DNS name")
}

/// A request and a response over one session, ending with the server's
/// `close_notify` observed as a clean end of stream by the client.
#[runite::test]
async fn a_client_and_server_exchange_data_and_close_cleanly() {
    let (connector, acceptor) = configurations();
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let addr = listener.local_addr().expect("addr");

    let server = runite::spawn(async move {
        let (socket, _peer) = listener.accept().await.expect("accept");
        let mut tls = acceptor.accept(socket).await.expect("server handshake");

        let mut request = [0u8; 5];
        tls.read_exact(&mut request).await.expect("server read");
        assert_eq!(&request, b"hello");

        tls.write_all(b"world").await.expect("server write");
        tls.close().await.expect("server close");
    });

    let socket = TcpStream::connect(addr).await.expect("connect");
    let mut tls = connector
        .connect(server_name(), socket)
        .await
        .expect("client handshake");
    assert_eq!(
        tls.connection().protocol_version(),
        Some(rustls::ProtocolVersion::TLSv1_3)
    );

    tls.write_all(b"hello").await.expect("client write");
    tls.flush().await.expect("client flush");

    let mut response = Vec::new();
    tls.read_to_end(&mut response)
        .await
        .expect("close_notify ends the stream cleanly");
    assert_eq!(response, b"world");

    server.await.expect("server task");
}

/// More plaintext than one TLS record and more than rustls's send buffer, so
/// the transport sees many records and several partial sends.
#[runite::test]
async fn a_large_payload_survives_record_fragmentation() {
    let (connector, acceptor) = configurations();
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let addr = listener.local_addr().expect("addr");
    let payload: Arc<Vec<u8>> =
        Arc::new((0..512 * 1024).map(|index| (index % 251) as u8).collect());

    let expected = Arc::clone(&payload);
    let server = runite::spawn(async move {
        let (socket, _peer) = listener.accept().await.expect("accept");
        let mut tls = acceptor.accept(socket).await.expect("server handshake");
        let mut received = Vec::new();
        tls.read_to_end(&mut received).await.expect("server read");
        assert_eq!(received.len(), expected.len());
        assert_eq!(received, *expected);
        tls.close().await.expect("server close");
    });

    let socket = TcpStream::connect(addr).await.expect("connect");
    let mut tls = connector
        .connect(server_name(), socket)
        .await
        .expect("client handshake");
    tls.write_all(&payload).await.expect("client write");
    tls.close().await.expect("client close");

    server.await.expect("server task");
}

/// A peer whose transport vanishes without `close_notify` is a truncated
/// stream, and must not be mistaken for the end of the data.
#[runite::test]
async fn a_transport_closed_without_close_notify_is_an_unexpected_eof() {
    let (connector, acceptor) = configurations();
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let addr = listener.local_addr().expect("addr");

    let server = runite::spawn(async move {
        let (socket, _peer) = listener.accept().await.expect("accept");
        let mut tls = acceptor.accept(socket).await.expect("server handshake");
        tls.write_all(b"truncated").await.expect("server write");
        tls.flush().await.expect("server flush");
        // Drop the socket with the session still open.
        drop(tls);
    });

    let socket = TcpStream::connect(addr).await.expect("connect");
    let mut tls = connector
        .connect(server_name(), socket)
        .await
        .expect("client handshake");
    let mut received = Vec::new();
    let error = tls
        .read_to_end(&mut received)
        .await
        .expect_err("a missing close_notify must not read as a clean EOF");
    assert_eq!(error.kind(), std::io::ErrorKind::UnexpectedEof);

    server.await.expect("server task");
}

/// A client that does not trust the server's CA must fail the handshake rather
/// than proceed, and the failure has to arrive as an error, not a hang.
#[runite::test]
async fn an_untrusted_certificate_fails_the_handshake() {
    let (_connector, acceptor) = configurations();
    // A second, unrelated CA: the server's chain cannot verify against it.
    let (stranger, _unused) = configurations();

    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let addr = listener.local_addr().expect("addr");

    let server = runite::spawn(async move {
        let (socket, _peer) = listener.accept().await.expect("accept");
        // The client rejects the certificate and sends an alert, so the server
        // handshake fails too; which error arrives depends on timing.
        let _ = acceptor.accept(socket).await;
    });

    let socket = TcpStream::connect(addr).await.expect("connect");
    let error = stranger
        .connect(server_name(), socket)
        .await
        .expect_err("the certificate is signed by an unknown CA");
    assert_eq!(error.kind(), std::io::ErrorKind::InvalidData);

    server.await.expect("server task");
}

/// ALPN is passed straight through to rustls; this only pins that both sides
/// can read the result off the stream.
#[runite::test]
async fn a_negotiated_alpn_protocol_is_visible_on_both_streams() {
    let (connector, acceptor) = configurations();
    let mut client_config = connector.config().as_ref().clone();
    client_config.alpn_protocols = vec![b"h2".to_vec(), b"http/1.1".to_vec()];
    let connector = TlsConnector::new(Arc::new(client_config));
    let mut server_config = acceptor.config().as_ref().clone();
    server_config.alpn_protocols = vec![b"http/1.1".to_vec()];
    let acceptor = TlsAcceptor::new(Arc::new(server_config));

    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let addr = listener.local_addr().expect("addr");

    let server = runite::spawn(async move {
        let (socket, _peer) = listener.accept().await.expect("accept");
        let mut tls = acceptor.accept(socket).await.expect("server handshake");
        assert_eq!(tls.connection().alpn_protocol(), Some(b"http/1.1".as_ref()));
        tls.close().await.expect("server close");
    });

    let socket = TcpStream::connect(addr).await.expect("connect");
    let mut tls = connector
        .connect(server_name(), socket)
        .await
        .expect("client handshake");
    assert_eq!(tls.connection().alpn_protocol(), Some(b"http/1.1".as_ref()));
    tls.close().await.expect("client close");

    server.await.expect("server task");
}

/// The reason this feature exists: hyper speaking HTTPS on runite, with no
/// second runtime anywhere in the process.
#[cfg(feature = "hyper")]
#[runite::test]
async fn hyper_serves_and_requests_over_tls() {
    use std::convert::Infallible;

    use bytes::Bytes;
    use http_body_util::{BodyExt as _, Empty, Full};
    use hyper::service::service_fn;
    use hyper::{Request, Response};
    use runite::hyper_rt::RuniteTimer;

    async fn hello(
        _request: Request<hyper::body::Incoming>,
    ) -> Result<Response<Full<Bytes>>, Infallible> {
        Ok(Response::new(Full::new(Bytes::from_static(b"over tls"))))
    }

    let (connector, acceptor) = configurations();
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let addr = listener.local_addr().expect("addr");

    let server = runite::spawn(async move {
        let (socket, _peer) = listener.accept().await.expect("accept");
        let tls = acceptor.accept(socket).await.expect("server handshake");
        hyper::server::conn::http1::Builder::new()
            .timer(RuniteTimer)
            .serve_connection(tls, service_fn(hello))
            .await
            .expect("serve https connection");
    });

    let socket = TcpStream::connect(addr).await.expect("connect");
    let tls = connector
        .connect(server_name(), socket)
        .await
        .expect("client handshake");
    let (mut sender, connection) = hyper::client::conn::http1::handshake(tls)
        .await
        .expect("http1 handshake");
    let driver = runite::spawn(async move {
        let _ = connection.await;
    });

    let request = Request::builder()
        .uri("/")
        .header("host", "localhost")
        .body(Empty::<Bytes>::new())
        .expect("request");
    let response = sender.send_request(request).await.expect("send request");
    assert_eq!(response.status(), 200);
    let body = response.collect().await.expect("collect body").to_bytes();
    assert_eq!(body.as_ref(), b"over tls");

    drop(sender);
    driver.await.expect("connection task");
    server.await.expect("server task");
}
