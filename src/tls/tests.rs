//! Record-layer contracts, exercised without a runtime.
//!
//! The pair of streams here is an in-memory duplex whose write side can be
//! throttled on command, which is what makes the interesting cases — a write
//! abandoned with a record half-delivered, a read that must not wait on a
//! jammed write direction — reproducible instead of dependent on when a kernel
//! socket buffer happens to fill. Everything is polled by hand with a no-op
//! waker; no wakeups are needed because the test decides when each side runs.

use core::future::Future;
use core::pin::{Pin, pin};
use core::task::{Context, Poll, Waker};
use std::cell::RefCell;
use std::collections::VecDeque;
use std::io;
use std::rc::Rc;
use std::sync::{Arc, OnceLock};

use rustls::pki_types::{CertificateDer, PrivateKeyDer, PrivatePkcs8KeyDer, ServerName};
use rustls::{ClientConfig, RootCertStore, ServerConfig};

use super::{TlsAcceptor, TlsConnector, TlsStream};
use crate::io::{AsyncRead, AsyncWrite};

/// One direction of the duplex, plus the knobs that throttle writes into it.
#[derive(Default)]
struct Pipe {
    bytes: VecDeque<u8>,
    /// Bytes a single write may take, or `None` for "as many as offered".
    chunk: Option<usize>,
    /// Bytes still accepted before writes start returning `Pending`, or `None`
    /// for a transport that never applies backpressure.
    budget: Option<usize>,
    closed: bool,
}

/// An in-memory transport implementing runite's I/O traits.
struct Duplex {
    read_from: Rc<RefCell<Pipe>>,
    write_to: Rc<RefCell<Pipe>>,
    /// Every buffer this endpoint has been handed for a write, in order, so a
    /// test can assert that a re-polled write is offered the identical slice.
    offered: Rc<RefCell<Vec<(usize, usize)>>>,
}

impl Duplex {
    fn pair() -> (Self, Self) {
        let left = Rc::new(RefCell::new(Pipe::default()));
        let right = Rc::new(RefCell::new(Pipe::default()));
        (
            Self {
                read_from: Rc::clone(&left),
                write_to: Rc::clone(&right),
                offered: Rc::new(RefCell::new(Vec::new())),
            },
            Self {
                read_from: right,
                write_to: left,
                offered: Rc::new(RefCell::new(Vec::new())),
            },
        )
    }

    fn offered(&self) -> Rc<RefCell<Vec<(usize, usize)>>> {
        Rc::clone(&self.offered)
    }

    fn outbound(&self) -> Rc<RefCell<Pipe>> {
        Rc::clone(&self.write_to)
    }
}

impl AsyncRead for Duplex {
    fn poll_read(
        self: Pin<&mut Self>,
        _cx: &mut Context<'_>,
        buf: &mut [u8],
    ) -> Poll<io::Result<usize>> {
        let mut pipe = self.read_from.borrow_mut();
        if pipe.bytes.is_empty() {
            return if pipe.closed {
                Poll::Ready(Ok(0))
            } else {
                Poll::Pending
            };
        }
        let len = buf.len().min(pipe.bytes.len());
        for slot in buf.iter_mut().take(len) {
            *slot = pipe.bytes.pop_front().expect("length was checked");
        }
        Poll::Ready(Ok(len))
    }
}

impl AsyncWrite for Duplex {
    fn poll_write(
        self: Pin<&mut Self>,
        _cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        self.offered
            .borrow_mut()
            .push((buf.as_ptr() as usize, buf.len()));
        let mut pipe = self.write_to.borrow_mut();
        let len = buf
            .len()
            .min(pipe.chunk.unwrap_or(usize::MAX))
            .min(pipe.budget.unwrap_or(usize::MAX));
        if len == 0 {
            return Poll::Pending;
        }
        pipe.bytes.extend(&buf[..len]);
        if let Some(budget) = pipe.budget.as_mut() {
            *budget -= len;
        }
        Poll::Ready(Ok(len))
    }

    fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Poll::Ready(Ok(()))
    }

    fn poll_close(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        self.write_to.borrow_mut().closed = true;
        Poll::Ready(Ok(()))
    }
}

/// A CA plus a leaf certificate for `localhost`, generated once per process.
struct Certificates {
    root: RootCertStore,
    chain: Vec<CertificateDer<'static>>,
    key: PrivateKeyDer<'static>,
}

fn certificates() -> Certificates {
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

    let mut root = RootCertStore::empty();
    root.add(authority.der().clone())
        .expect("the CA is a valid trust anchor");

    Certificates {
        root,
        chain: vec![leaf.der().clone(), authority.der().clone()],
        key: PrivateKeyDer::Pkcs8(PrivatePkcs8KeyDer::from(leaf_key.serialize_der())),
    }
}

fn install_provider() {
    static INSTALLED: OnceLock<()> = OnceLock::new();
    INSTALLED.get_or_init(|| {
        // Tests pick `ring`; runite itself deliberately picks nothing.
        let _ = rustls::crypto::ring::default_provider().install_default();
    });
}

fn configurations() -> (TlsConnector, TlsAcceptor) {
    install_provider();
    let certificates = certificates();
    let client = ClientConfig::builder()
        .with_root_certificates(certificates.root)
        .with_no_client_auth();
    let server = ServerConfig::builder()
        .with_no_client_auth()
        .with_single_cert(certificates.chain, certificates.key)
        .expect("the leaf key matches the leaf certificate");
    (
        TlsConnector::new(Arc::new(client)),
        TlsAcceptor::new(Arc::new(server)),
    )
}

fn context() -> Context<'static> {
    Context::from_waker(Waker::noop())
}

/// Polls two futures in lockstep until both resolve.
///
/// A handshake is a conversation: neither side can finish alone, and with a
/// no-op waker nothing will re-poll them. The iteration cap turns a stall into
/// a failed test instead of a hung one.
fn drive<A, B>(mut first: Pin<&mut A>, mut second: Pin<&mut B>) -> (A::Output, B::Output)
where
    A: Future,
    B: Future,
{
    let mut cx = context();
    let mut first_output = None;
    let mut second_output = None;
    for _ in 0..64 {
        if first_output.is_none()
            && let Poll::Ready(output) = first.as_mut().poll(&mut cx)
        {
            first_output = Some(output);
        }
        if second_output.is_none()
            && let Poll::Ready(output) = second.as_mut().poll(&mut cx)
        {
            second_output = Some(output);
        }
        match (first_output.take(), second_output.take()) {
            (Some(first), Some(second)) => return (first, second),
            (first, second) => {
                first_output = first;
                second_output = second;
            }
        }
    }
    panic!("the two sides stopped making progress");
}

fn handshake() -> (TlsStream<Duplex>, TlsStream<Duplex>) {
    let (connector, acceptor) = configurations();
    let (client_io, server_io) = Duplex::pair();
    let name = ServerName::try_from("localhost").expect("valid DNS name");
    let client = pin!(connector.connect(name, client_io));
    let server = pin!(acceptor.accept(server_io));
    let (client, server) = drive(client, server);
    (
        client.expect("client handshake"),
        server.expect("server handshake"),
    )
}

/// Polls `future` to completion, assuming it needs no help from the peer.
fn now<F: Future>(future: F) -> F::Output {
    let mut future = pin!(future);
    let mut cx = context();
    for _ in 0..64 {
        if let Poll::Ready(output) = future.as_mut().poll(&mut cx) {
            return output;
        }
    }
    panic!("the future never completed on its own");
}

#[test]
fn a_completed_handshake_agrees_on_the_session() {
    let (client, server) = handshake();
    assert!(!client.connection().is_handshaking());
    assert!(!server.connection().is_handshaking());
    assert_eq!(
        client.connection().protocol_version(),
        server.connection().protocol_version()
    );
    assert!(client.connection().peer_certificates().is_some());
}

#[test]
fn plaintext_written_by_one_side_is_read_by_the_other() {
    use crate::io::{AsyncReadExt as _, AsyncWriteExt as _};

    let (mut client, mut server) = handshake();
    now(client.write_all(b"ping")).expect("client write");
    now(client.flush()).expect("client flush");

    let mut received = [0u8; 4];
    now(server.read_exact(&mut received)).expect("server read");
    assert_eq!(&received, b"ping");
}

/// The contract that makes this stream safe on a completion-based backend: a
/// record already handed to the transport must be re-offered as the identical
/// slice, because that identity is what stops the runtime from submitting the
/// same bytes twice.
#[test]
fn a_partially_written_record_is_re_offered_as_the_same_slice() {
    use crate::io::AsyncWriteExt as _;

    let (mut client, _server) = handshake();
    let offered = client.get_ref().offered();
    let outbound = client.get_ref().outbound();
    offered.borrow_mut().clear();

    // Let the transport take one byte per write, so the record needs many.
    outbound.borrow_mut().chunk = Some(1);

    // `Box::pin`, not `pin!`: dropping a `Pin<&mut F>` does not drop `F`, and
    // this test is about what happens to a future that is really gone.
    let mut write = Box::pin(client.write_all(b"a record split across many writes"));
    let mut cx = context();
    for _ in 0..8 {
        let _ = write.as_mut().poll(&mut cx);
    }
    drop(write);

    let offers = offered.borrow();
    assert!(offers.len() > 2, "the record should take several writes");
    let mut previous = offers[0];
    for offer in offers.iter().skip(1) {
        assert_eq!(
            offer.0,
            previous.0 + 1,
            "each write must resume the same buffer one byte later"
        );
        assert_eq!(offer.1, previous.1 - 1);
        previous = *offer;
    }
}

/// Abandoning a write blocked on a half-delivered record must corrupt nothing:
/// the record belongs to the stream, so the next writer finishes it, and the
/// abandoned write's own plaintext was never accepted and never appears.
#[test]
fn an_abandoned_write_accepts_no_plaintext_and_leaves_the_record_intact() {
    use crate::io::{AsyncReadExt as _, AsyncWriteExt as _};

    let (mut client, mut server) = handshake();
    let outbound = client.get_ref().outbound();
    // Fewer bytes than the record needs, so it stops half-delivered.
    outbound.borrow_mut().budget = Some(5);
    now(client.write_all(b"first")).expect("the plaintext is accepted and the record staged");

    // `Box::pin`, not `pin!`: dropping a `Pin<&mut F>` does not drop `F`.
    let mut abandoned = Box::pin(client.write_all(b"lost"));
    let mut cx = context();
    assert!(
        abandoned.as_mut().poll(&mut cx).is_pending(),
        "no plaintext may be accepted while the staged record is undelivered"
    );
    drop(abandoned);

    outbound.borrow_mut().budget = None;
    now(client.write_all(b"third")).expect("the next writer resumes the stream");
    now(client.flush()).expect("client flush");

    let mut received = [0u8; 10];
    now(server.read_exact(&mut received)).expect("server read");
    assert_eq!(
        &received, b"firstthird",
        "the staged record must arrive once, and the abandoned write not at all"
    );
}

/// A peer that has stopped reading must not be able to stop us reading what it
/// already sent. Anything that makes the read path wait on the write direction
/// once the session is up turns ordinary backpressure into a deadlock.
#[test]
fn a_jammed_write_direction_does_not_stop_reads() {
    use crate::io::{AsyncReadExt as _, AsyncWriteExt as _};

    let (mut client, mut server) = handshake();
    now(server.write_all(b"inbound")).expect("server write");
    now(server.flush()).expect("server flush");

    // The client's own write direction jams with a record half-delivered.
    client.get_ref().outbound().borrow_mut().budget = Some(5);
    now(client.write_all(b"outbound")).expect("plaintext accepted, record staged");

    let mut received = [0u8; 7];
    now(client.read_exact(&mut received)).expect("a read must not wait on the write direction");
    assert_eq!(&received, b"inbound");
}

#[test]
fn close_notify_reaches_the_peer_as_a_clean_end_of_stream() {
    use crate::io::{AsyncReadExt as _, AsyncWriteExt as _};

    let (mut client, mut server) = handshake();
    now(client.write_all(b"payload")).expect("client write");
    now(client.close()).expect("client close");

    let mut received = Vec::new();
    now(server.read_to_end(&mut received)).expect("a close_notify is a clean end of stream");
    assert_eq!(received, b"payload");
}

/// The distinction `close_notify` exists to draw: a transport that simply
/// disappears is a truncated stream, not the end of the data.
#[test]
fn a_truncated_transport_is_not_reported_as_end_of_stream() {
    use crate::io::{AsyncReadExt as _, AsyncWriteExt as _};

    let (mut client, mut server) = handshake();
    now(client.write_all(b"payload")).expect("client write");
    now(client.flush()).expect("client flush");
    // Close the transport without ending the TLS session.
    client.get_ref().outbound().borrow_mut().closed = true;

    let mut received = Vec::new();
    let error = now(server.read_to_end(&mut received))
        .expect_err("a missing close_notify must not read as a clean EOF");
    assert_eq!(error.kind(), io::ErrorKind::UnexpectedEof);
}

#[test]
fn writing_after_close_fails_rather_than_silently_dropping_bytes() {
    use crate::io::AsyncWriteExt as _;

    let (mut client, _server) = handshake();
    now(client.close()).expect("client close");
    let error = now(client.write_all(b"too late")).expect_err("the session is over");
    assert_eq!(error.kind(), io::ErrorKind::BrokenPipe);
}

#[test]
fn closing_twice_is_harmless() {
    use crate::io::AsyncWriteExt as _;

    let (mut client, _server) = handshake();
    now(client.close()).expect("first close");
    now(client.close()).expect("second close");
}

/// A payload larger than both a TLS record and rustls's send buffer, so the
/// test covers fragmentation, short accepts from `Writer`, and refilling the
/// ciphertext buffer across record boundaries.
#[test]
fn a_payload_larger_than_one_record_survives_the_round_trip() {
    use crate::io::{AsyncReadExt as _, AsyncWriteExt as _};

    let (mut client, mut server) = handshake();
    let payload: Vec<u8> = (0..300_000).map(|index| (index % 251) as u8).collect();

    let mut writer = pin!(client.write_all(&payload));
    let mut received = vec![0u8; payload.len()];
    let mut reader = pin!(server.read_exact(&mut received));
    let mut cx = context();
    let mut written = None;
    let mut read = None;
    for _ in 0..4096 {
        if written.is_none()
            && let Poll::Ready(result) = writer.as_mut().poll(&mut cx)
        {
            written = Some(result);
        }
        if read.is_none()
            && let Poll::Ready(result) = reader.as_mut().poll(&mut cx)
        {
            read = Some(result);
        }
        if written.is_some() && read.is_some() {
            break;
        }
    }
    written.expect("the write finished").expect("client write");
    read.expect("the read finished").expect("server read");
    assert_eq!(received, payload);
}

/// Vectored writes exist here to save record overhead, so the slices must
/// arrive as one contiguous plaintext stream.
#[test]
fn vectored_writes_are_coalesced_into_the_stream() {
    use crate::io::{AsyncReadExt as _, AsyncWriteExt as _};

    let (mut client, mut server) = handshake();
    let written = now(client.write_vectored(&[
        io::IoSlice::new(b"header "),
        io::IoSlice::new(b"and "),
        io::IoSlice::new(b"body"),
    ]))
    .expect("vectored write");
    assert_eq!(written, b"header and body".len());
    now(client.flush()).expect("client flush");

    let mut received = [0u8; 15];
    now(server.read_exact(&mut received)).expect("server read");
    assert_eq!(&received, b"header and body");
}

/// A handshake against a peer that hangs up must fail as a truncated
/// handshake, not hang or report success.
#[test]
fn a_handshake_against_a_closed_transport_fails() {
    let (connector, _acceptor) = configurations();
    let (client_io, server_io) = Duplex::pair();
    client_io.read_from.borrow_mut().closed = true;
    drop(server_io);

    let name = ServerName::try_from("localhost").expect("valid DNS name");
    let error = now(connector.connect(name, client_io)).expect_err("no peer to handshake with");
    assert_eq!(error.kind(), io::ErrorKind::UnexpectedEof);
}
