//! TLS over runite transports, backed by [`rustls`].
//!
//! [`rustls`] is sans-I/O: a [`ClientConnection`] or [`ServerConnection`] is a
//! state machine that moves plaintext and ciphertext through memory buffers and
//! never touches a socket. This module is the missing half — the part that has
//! to know about the runtime — and nothing more. It drives that state machine
//! over any transport implementing runite's [`AsyncRead`] and [`AsyncWrite`],
//! so a [`TcpStream`](crate::net::TcpStream), a Unix socket, or an in-memory
//! duplex all work the same way, with no second reactor in the process.
//!
//! [`TlsConnector`] performs client handshakes, [`TlsAcceptor`] server ones,
//! and both hand back a [`TlsStream`] that is itself an [`AsyncRead`] +
//! [`AsyncWrite`] transport. With the `hyper` feature also enabled, a
//! `TlsStream` implements hyper's transport traits, so `hyper` speaks HTTPS
//! over it.
//!
//! # `rustls` is part of this API
//!
//! The types here are rustls's own: [`TlsConnector::new`] takes an
//! `Arc<ClientConfig>`, [`TlsStream::connection`] hands back a `&Connection`.
//! Those types are only nameable through the exact `rustls` build runite links
//! against, so runite re-exports it as [`runite::tls::rustls`](rustls). Reach
//! for that path rather than a second `rustls` dependency of your own: two
//! semver-incompatible copies in one graph produce an `expected ClientConfig,
//! found ClientConfig` error with nothing in it to explain itself.
//!
//! The consequence runs the other way too. A `rustls` 0.24 is a breaking change
//! for runite, because it changes types this module's signatures are written
//! in; runite will take it in a major release of its own, not a patch.
//!
//! # You must choose a cryptographic provider
//!
//! runite depends on `rustls` with **no provider feature enabled**. Which
//! implementation performs the cryptography — `aws-lc-rs` (rustls's own
//! default) or `ring` — is an application decision with real build, licensing,
//! and certification consequences, so runite does not make it for you. Neither
//! is the "no C toolchain" option: both compile C in a build script. The
//! difference is how much of one — `ring` needs a C compiler (`cc`), while
//! `aws-lc-rs` builds AWS-LC through `aws-lc-sys`, which wants CMake as well.
//!
//! The consequence is that the application must supply one. If it does not,
//! building a `ClientConfig` or `ServerConfig` panics with:
//!
//! ```text
//! Could not automatically determine the process-level CryptoProvider from
//! Rustls crate features.
//! ```
//!
//! Fix it in one of two ways. Either depend on `rustls` directly with a
//! provider feature — `rustls = { version = "0.23", features = ["ring"] }` —
//! and let rustls install it, or install one explicitly before building any
//! configuration:
//!
//! ```no_run
//! runite::tls::rustls::crypto::ring::default_provider()
//!     .install_default()
//!     .expect("no other provider may be installed first");
//! ```
//!
//! A direct dependency is still the way to *enable* a provider — a feature can
//! only be turned on from a `Cargo.toml` — but write the code against
//! [`runite::tls::rustls`](rustls) so the version can never drift.
//!
//! Trust anchors are the same kind of decision and are equally out of scope:
//! `rustls-native-certs` reads the platform store, `webpki-roots` compiles a
//! root set in. This module takes a finished [`ClientConfig`]/[`ServerConfig`]
//! and stays out of the way.
//!
//! # Examples
//!
//! Wrapping a connected socket as a TLS client:
//!
//! ```no_run
//! use std::sync::Arc;
//!
//! use runite::io::AsyncWriteExt;
//! use runite::net::TcpStream;
//! use runite::tls::rustls::ClientConfig;
//! use runite::tls::TlsConnector;
//!
//! # async fn example(config: Arc<ClientConfig>) -> std::io::Result<()> {
//! let connector = TlsConnector::new(config);
//! let socket = TcpStream::connect("example.com:443").await?;
//! let mut tls = connector
//!     .connect("example.com".try_into().expect("valid DNS name"), socket)
//!     .await?;
//!
//! tls.write_all(b"GET / HTTP/1.0\r\n\r\n").await?;
//! tls.flush().await?;
//! # Ok(())
//! # }
//! ```

use core::fmt;
use core::pin::Pin;
use core::task::{Context, Poll, ready};
use std::future::poll_fn;
use std::io::{self, BufRead as _, IoSlice, Read as _, Write as _};
use std::sync::Arc;

use rustls::pki_types::ServerName;
use rustls::{ClientConfig, ClientConnection, Connection, ServerConfig, ServerConnection};

use crate::io::{AsyncRead, AsyncWrite};

/// The `rustls` build this module's signatures are written in.
///
/// Re-exported because those signatures are unusable without it: a caller who
/// declares their own `rustls` dependency is relying on cargo to unify the two,
/// and gets a type error that names the same type twice when it does not.
pub use rustls;

#[cfg(feature = "hyper")]
mod hyper_impl;
#[cfg(test)]
mod tests;

/// Ciphertext read from the transport per transport read.
///
/// rustls's deframer takes only what it can use and leaves the remainder, so
/// whatever a single transport read returns has to be held across calls anyway.
/// One TLS record's payload is 16 KiB, which makes this the size at which a
/// read stops being able to hand rustls a whole record at a time.
const CIPHERTEXT_CHUNK: usize = 16 * 1024;

/// A TLS client configuration ready to wrap transports.
///
/// Cloning is cheap: the [`ClientConfig`] is shared, as rustls intends.
#[derive(Clone, Debug)]
pub struct TlsConnector {
    config: Arc<ClientConfig>,
}

impl TlsConnector {
    /// Creates a connector from a rustls client configuration.
    ///
    /// Building that configuration is where a cryptographic provider and a set
    /// of trust anchors are chosen; see the [module documentation](self).
    #[must_use]
    pub fn new(config: Arc<ClientConfig>) -> Self {
        Self { config }
    }

    /// Returns the configuration handshakes are performed with.
    #[must_use]
    pub fn config(&self) -> &Arc<ClientConfig> {
        &self.config
    }

    /// Performs a client handshake over `stream`.
    ///
    /// `server_name` is what the server's certificate is verified against, and
    /// what is sent in the SNI extension; it is deliberately separate from the
    /// address the transport connected to, because those differ whenever a
    /// proxy, a tunnel, or an explicit IP is involved.
    ///
    /// The returned stream is ready for application data: the handshake is
    /// complete, so [`connection`](TlsStream::connection) already reports the
    /// negotiated protocol version, cipher suite, ALPN protocol, and peer
    /// certificates. Dropping this future before it resolves abandons the
    /// handshake and the transport with it.
    ///
    /// # Errors
    ///
    /// Returns [`io::ErrorKind::InvalidInput`] if the configuration cannot
    /// start a connection to `server_name`, [`io::ErrorKind::InvalidData`] if
    /// the peer fails the handshake (a bad certificate, no shared cipher
    /// suite), [`io::ErrorKind::UnexpectedEof`] if the transport closes
    /// mid-handshake, and any error the transport itself reports.
    pub async fn connect<S>(
        &self,
        server_name: ServerName<'static>,
        stream: S,
    ) -> io::Result<TlsStream<S>>
    where
        S: AsyncRead + AsyncWrite + Unpin,
    {
        let connection = ClientConnection::new(Arc::clone(&self.config), server_name)
            .map_err(|error| io::Error::new(io::ErrorKind::InvalidInput, error))?;
        let mut stream = TlsStream::new(stream, Connection::Client(connection));
        poll_fn(|cx| stream.poll_handshake(cx)).await?;
        Ok(stream)
    }
}

impl From<Arc<ClientConfig>> for TlsConnector {
    fn from(config: Arc<ClientConfig>) -> Self {
        Self::new(config)
    }
}

/// A TLS server configuration ready to wrap accepted transports.
///
/// Cloning is cheap: the [`ServerConfig`] is shared, as rustls intends.
#[derive(Clone, Debug)]
pub struct TlsAcceptor {
    config: Arc<ServerConfig>,
}

impl TlsAcceptor {
    /// Creates an acceptor from a rustls server configuration.
    ///
    /// Building that configuration is where a cryptographic provider, the
    /// certificate chain, and any client-authentication policy are chosen; see
    /// the [module documentation](self).
    #[must_use]
    pub fn new(config: Arc<ServerConfig>) -> Self {
        Self { config }
    }

    /// Returns the configuration handshakes are performed with.
    #[must_use]
    pub fn config(&self) -> &Arc<ServerConfig> {
        &self.config
    }

    /// Performs a server handshake over an accepted `stream`.
    ///
    /// Resolves once the client's `Finished` message has been verified, so a
    /// successful result means the peer is authenticated to whatever degree the
    /// configuration demands. Dropping this future before it resolves abandons
    /// the handshake and the transport with it.
    ///
    /// # Errors
    ///
    /// Returns [`io::ErrorKind::InvalidInput`] if the configuration cannot
    /// start a connection, [`io::ErrorKind::InvalidData`] if the client fails
    /// the handshake (an unacceptable certificate, no shared cipher suite, a
    /// malformed record), [`io::ErrorKind::UnexpectedEof`] if the transport closes
    /// mid-handshake — a plaintext HTTP request to a TLS port typically lands
    /// here or on `InvalidData` — and any error the transport itself reports.
    pub async fn accept<S>(&self, stream: S) -> io::Result<TlsStream<S>>
    where
        S: AsyncRead + AsyncWrite + Unpin,
    {
        let connection = ServerConnection::new(Arc::clone(&self.config))
            .map_err(|error| io::Error::new(io::ErrorKind::InvalidInput, error))?;
        let mut stream = TlsStream::new(stream, Connection::Server(connection));
        poll_fn(|cx| stream.poll_handshake(cx)).await?;
        Ok(stream)
    }
}

impl From<Arc<ServerConfig>> for TlsAcceptor {
    fn from(config: Arc<ServerConfig>) -> Self {
        Self::new(config)
    }
}

/// A TLS session over a runite transport.
///
/// Produced by [`TlsConnector::connect`] or [`TlsAcceptor::accept`], and
/// identical in use to the transport underneath it: [`AsyncRead`] yields
/// decrypted plaintext, [`AsyncWrite`] encrypts what it is given. The type is
/// the same for both roles because the difference lives entirely in the rustls
/// [`Connection`] it carries.
///
/// Like every runite I/O object this is thread-affine and effectively `!Send`:
/// it is driven by the event loop that owns the transport's wakers.
///
/// # Buffering
///
/// Two buffers sit between rustls and the transport, and both exist for the
/// same reason. Records rustls produces are staged in a buffer this stream
/// owns and handed to the transport as one stable slice until the transport
/// accepts all of it; ciphertext read from the transport is held until rustls
/// has taken every byte. A completion-based backend must be able to re-poll a
/// write with the exact buffer it was given, and rustls's own buffers do not
/// offer that guarantee across calls.
///
/// The practical consequences for a caller:
///
/// - A single [`poll_write`](AsyncWrite::poll_write) accepts at most as much
///   plaintext as rustls's send buffer allows (64 KiB by default), so
///   [`write_all`](crate::io::AsyncWriteExt::write_all) may take several turns.
/// - Accepted plaintext is not necessarily on the wire. Call
///   [`flush`](crate::io::AsyncWriteExt::flush) — or
///   [`close`](crate::io::AsyncWriteExt::close) — before you rely on the peer
///   having seen it, exactly as with a [`BufWriter`](crate::io::BufWriter).
/// - Plaintext is handed to rustls only once the previously staged ciphertext
///   has been written. A write future abandoned mid-record therefore leaves no
///   truncated record behind, and the next writer resumes the same byte stream.
pub struct TlsStream<S> {
    io: S,
    conn: Connection,
    /// Ciphertext read from the transport that rustls has not taken yet.
    incoming: Box<[u8]>,
    incoming_start: usize,
    incoming_end: usize,
    /// Ciphertext rustls has produced that the transport has not taken yet.
    outgoing: Vec<u8>,
    outgoing_start: usize,
    transport_eof: bool,
    close_notify_sent: bool,
}

impl<S> TlsStream<S> {
    fn new(io: S, conn: Connection) -> Self {
        Self {
            io,
            conn,
            incoming: vec![0; CIPHERTEXT_CHUNK].into_boxed_slice(),
            incoming_start: 0,
            incoming_end: 0,
            outgoing: Vec::new(),
            outgoing_start: 0,
            transport_eof: false,
            close_notify_sent: false,
        }
    }

    /// Returns a reference to the transport carrying the session.
    pub fn get_ref(&self) -> &S {
        &self.io
    }

    /// Returns a mutable reference to the transport carrying the session.
    ///
    /// Reading or writing the transport directly desynchronizes the record
    /// layer and breaks the session; use this for out-of-band operations such
    /// as reading a peer address or setting a socket option.
    pub fn get_mut(&mut self) -> &mut S {
        &mut self.io
    }

    /// Returns the rustls connection state.
    ///
    /// This is how negotiated properties are read — [`alpn_protocol`],
    /// [`protocol_version`], [`negotiated_cipher_suite`], and
    /// [`peer_certificates`] are all reachable through it. Exposing rustls's
    /// own type keeps runite out of the business of mirroring its API.
    ///
    /// [`alpn_protocol`]: rustls::CommonState::alpn_protocol
    /// [`protocol_version`]: rustls::CommonState::protocol_version
    /// [`negotiated_cipher_suite`]: rustls::CommonState::negotiated_cipher_suite
    /// [`peer_certificates`]: rustls::CommonState::peer_certificates
    pub fn connection(&self) -> &Connection {
        &self.conn
    }

    /// Consumes the stream and returns everything it was holding.
    ///
    /// Both ciphertext buffers come back with the transport and the session,
    /// because neither can be reconstructed from the other two: rustls has
    /// already deframed what is in
    /// [`buffered_ciphertext`](TlsParts::buffered_ciphertext) out of the
    /// transport, and it has already produced
    /// [`staged_ciphertext`](TlsParts::staged_ciphertext) out of the
    /// [`Connection`]. Dropping either one desynchronizes the record stream
    /// from the session by however many bytes it held, which surfaces later as
    /// a decrypt failure that looks like the peer's fault.
    ///
    /// See [`TlsParts`] for what a caller resuming the session has to do with
    /// each of them.
    pub fn into_parts(self) -> TlsParts<S> {
        let mut staged_ciphertext = self.outgoing;
        staged_ciphertext.drain(..self.outgoing_start);
        TlsParts {
            io: self.io,
            connection: self.conn,
            buffered_ciphertext: self.incoming[self.incoming_start..self.incoming_end].to_vec(),
            staged_ciphertext,
        }
    }

    fn staged(&self) -> usize {
        self.outgoing.len() - self.outgoing_start
    }
}

/// The pieces of a [`TlsStream`], returned by [`TlsStream::into_parts`].
///
/// A session can be resumed from these, but only if both ciphertext buffers are
/// honoured: write [`staged_ciphertext`](Self::staged_ciphertext) to the
/// transport before anything else, and feed
/// [`buffered_ciphertext`](Self::buffered_ciphertext) to
/// [`Connection::read_tls`] before reading the transport again. Either buffer
/// is routinely non-empty on a busy stream — a read stops as soon as rustls
/// accepts one batch of records, so the tail of a 16 KiB transport read is
/// normally still waiting.
///
/// Non-exhaustive so that a future buffer, should the implementation grow one,
/// does not have to be another silent loss.
#[non_exhaustive]
pub struct TlsParts<S> {
    /// The transport the session was running over.
    pub io: S,
    /// The rustls session state, including any plaintext it has already
    /// decrypted and not yet handed out.
    pub connection: Connection,
    /// Ciphertext read from `io` that `connection` has not consumed yet.
    ///
    /// These bytes are no longer in the transport. Reading `io` without
    /// replaying them into [`Connection::read_tls`] starts the record stream
    /// mid-record.
    pub buffered_ciphertext: Vec<u8>,
    /// Ciphertext `connection` produced that has not reached `io` yet.
    ///
    /// These bytes are no longer in the session. The peer never sees them
    /// unless they are written to `io` ahead of anything the resumed session
    /// produces.
    pub staged_ciphertext: Vec<u8>,
}

impl<S> fmt::Debug for TlsParts<S> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("TlsParts")
            .field("handshaking", &self.connection.is_handshaking())
            .field("staged_ciphertext", &self.staged_ciphertext.len())
            .field("buffered_ciphertext", &self.buffered_ciphertext.len())
            .finish_non_exhaustive()
    }
}

impl<S> fmt::Debug for TlsStream<S> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("TlsStream")
            .field("handshaking", &self.conn.is_handshaking())
            .field("staged_ciphertext", &self.staged())
            .field(
                "buffered_ciphertext",
                &(self.incoming_end - self.incoming_start),
            )
            .field("transport_eof", &self.transport_eof)
            .field("close_notify_sent", &self.close_notify_sent)
            .finish_non_exhaustive()
    }
}

impl<S> TlsStream<S>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    /// Moves the records rustls has queued into the staging buffer.
    ///
    /// Deliberately a no-op while staged ciphertext remains: the transport may
    /// still own the exact slice it was handed, and a completion-based backend
    /// identifies a re-polled write by that slice. Growing the buffer under it
    /// would present a different one and let the same bytes be submitted twice.
    /// The records stay queued in rustls until the staging buffer is empty,
    /// which costs nothing but a later copy.
    fn stage_records(&mut self) {
        if self.outgoing_start < self.outgoing.len() {
            return;
        }
        self.outgoing.clear();
        self.outgoing_start = 0;
        while self.conn.wants_write() {
            self.conn
                .write_tls(&mut self.outgoing)
                .expect("writing TLS records into a Vec cannot fail");
        }
    }

    /// Writes every record rustls has produced to the transport.
    ///
    /// `Ready(Ok(()))` means rustls has nothing left to send and the transport
    /// has accepted all of it — not that the transport has flushed it.
    fn poll_send(&mut self, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        loop {
            self.stage_records();
            let remaining = self.staged();
            if remaining == 0 {
                return Poll::Ready(Ok(()));
            }
            let written = ready!(
                Pin::new(&mut self.io).poll_write(cx, &self.outgoing[self.outgoing_start..])
            )?;
            // `S` is whatever transport the caller supplied. A count of zero or
            // one larger than the slice would desynchronize the record stream
            // beyond repair, so it fails here rather than corrupting the
            // session or panicking on the next slice.
            if written == 0 {
                return Poll::Ready(Err(io::Error::new(
                    io::ErrorKind::WriteZero,
                    "the transport accepted none of a TLS record",
                )));
            }
            if written > remaining {
                return Poll::Ready(Err(io::Error::other(
                    "the transport reported writing more of a TLS record than it was given",
                )));
            }
            self.outgoing_start += written;
        }
    }

    /// Fills the ciphertext buffer from the transport.
    ///
    /// Only called with the buffer empty, so a transport read never has to work
    /// around bytes rustls has not consumed yet.
    fn poll_refill(&mut self, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        debug_assert_eq!(self.incoming_start, self.incoming_end);
        let read = ready!(Pin::new(&mut self.io).poll_read(cx, &mut self.incoming))?;
        self.incoming_start = 0;
        self.incoming_end = read;
        if read == 0 {
            self.transport_eof = true;
        }
        Poll::Ready(Ok(()))
    }

    /// Gives rustls more ciphertext, reading from the transport as needed.
    ///
    /// Resolves once rustls has taken something — new bytes, or the transport's
    /// end of stream. `Ok(false)` means rustls will not take any more input
    /// because the peer's `close_notify` has already been processed, which the
    /// caller must treat as end of stream rather than polling again.
    fn poll_ingest(&mut self, cx: &mut Context<'_>) -> Poll<io::Result<bool>> {
        loop {
            if self.incoming_start == self.incoming_end {
                self.incoming_start = 0;
                self.incoming_end = 0;
                if self.transport_eof {
                    // rustls learns of a transport EOF from a reader that
                    // yields nothing. Without it, `Reader` cannot tell a clean
                    // `close_notify` from a stream someone truncated, which is
                    // the difference between an ordinary end of response and a
                    // truncation attack.
                    self.conn.read_tls(&mut io::empty())?;
                    return Poll::Ready(Ok(true));
                }
                ready!(self.poll_refill(cx))?;
                continue;
            }

            let Self {
                conn,
                incoming,
                incoming_start,
                incoming_end,
                ..
            } = self;
            let mut source = &incoming[*incoming_start..*incoming_end];
            let taken = conn.read_tls(&mut source)?;
            *incoming_start += taken;
            if taken == 0 {
                // Only reachable once the peer's `close_notify` has been
                // processed: rustls stops accepting input at that point.
                return Poll::Ready(Ok(false));
            }

            if let Err(error) = self.conn.process_new_packets() {
                // rustls has queued a fatal alert describing the rejection.
                // Give the transport one non-blocking chance to carry it to the
                // peer, but never wait for it: this connection is over either
                // way, and the caller is owed the error now.
                let _ = self.poll_send(cx);
                return Poll::Ready(Err(io::Error::new(io::ErrorKind::InvalidData, error)));
            }
            return Poll::Ready(Ok(true));
        }
    }

    /// Drives the handshake to completion.
    ///
    /// [`TlsConnector::connect`] and [`TlsAcceptor::accept`] resolve this before
    /// handing the stream over, so the read and write paths reach it only if a
    /// handshake is somehow still in progress — and they check that first,
    /// because a handshake is the one time a *read* is allowed to wait on the
    /// write direction.
    fn poll_handshake(&mut self, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        while self.conn.is_handshaking() {
            ready!(self.poll_send(cx))?;
            if !self.conn.is_handshaking() {
                break;
            }
            if self.transport_eof {
                return Poll::Ready(Err(io::Error::new(
                    io::ErrorKind::UnexpectedEof,
                    "the transport closed during the TLS handshake",
                )));
            }
            if !ready!(self.poll_ingest(cx))? {
                return Poll::Ready(Err(io::Error::new(
                    io::ErrorKind::UnexpectedEof,
                    "the peer closed the TLS session during the handshake",
                )));
            }
        }
        // The flight that completed the handshake is usually still queued.
        self.poll_send(cx)
    }

    /// Drives the session until rustls's plaintext buffer can answer a read.
    ///
    /// `Ready(Ok(()))` means the next [`Connection::reader`] call resolves
    /// without blocking — with plaintext, or with the end of the stream.
    /// Separating this from taking the bytes is what lets a caller read
    /// straight out of rustls's buffer: a borrow of the connection cannot
    /// survive the polling loop that produced it.
    fn poll_fill(&mut self, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        // Only a handshake justifies making a reader wait on the write
        // direction. Once the session is up, a peer that has stopped reading
        // must not be able to stop us reading what it already sent.
        if self.conn.is_handshaking() {
            ready!(self.poll_handshake(cx))?;
        }

        loop {
            match self.conn.reader().fill_buf() {
                // Plaintext, or an empty chunk for a clean `close_notify`. A
                // truncated stream comes back as `UnexpectedEof`, which is
                // precisely the distinction callers need, so it passes through
                // unchanged.
                Ok(_) => return Poll::Ready(Ok(())),
                Err(error) if error.kind() == io::ErrorKind::WouldBlock => {}
                Err(error) => return Poll::Ready(Err(error)),
            }

            if !ready!(self.poll_ingest(cx))? {
                return Poll::Ready(Ok(()));
            }

            // Processing may have queued records of its own — a TLS 1.3 key
            // update, or an alert. Offer them to the transport, but neither
            // wait on write backpressure nor fail the read if the write
            // direction is gone: plaintext the peer already sent may be sitting
            // in rustls right now, and losing it to a write-side error would
            // truncate a message that arrived intact. The records stay queued
            // and the error resurfaces on the next write, flush, or close.
            let _ = self.poll_send(cx);
        }
    }

    fn poll_read_plaintext(
        &mut self,
        cx: &mut Context<'_>,
        buf: &mut [u8],
    ) -> Poll<io::Result<usize>> {
        if buf.is_empty() {
            return Poll::Ready(Ok(0));
        }
        ready!(self.poll_fill(cx))?;
        Poll::Ready(self.conn.reader().read(buf))
    }

    fn poll_write_plaintext(
        &mut self,
        cx: &mut Context<'_>,
        bufs: &[IoSlice<'_>],
    ) -> Poll<io::Result<usize>> {
        if bufs.iter().all(|buf| buf.is_empty()) {
            return Poll::Ready(Ok(0));
        }
        if self.close_notify_sent {
            return Poll::Ready(Err(io::Error::new(
                io::ErrorKind::BrokenPipe,
                "the TLS session has been closed",
            )));
        }
        if self.conn.is_handshaking() {
            ready!(self.poll_handshake(cx))?;
        }

        // Nothing may be handed to rustls until the staged records are gone.
        // Accepting plaintext and then returning `Pending` would tell the
        // caller nothing was written while rustls had already encrypted it, and
        // the caller's next attempt would send those bytes a second time.
        ready!(self.poll_send(cx))?;

        let accepted = self.conn.writer().write_vectored(bufs)?;
        if accepted == 0 {
            return Poll::Ready(Err(io::Error::new(
                io::ErrorKind::WriteZero,
                "rustls accepted no plaintext",
            )));
        }

        // Best effort: get the record moving now. Ciphertext the transport will
        // not take yet stays staged, and `poll_flush` is what waits for it.
        if let Poll::Ready(Err(error)) = self.poll_send(cx) {
            return Poll::Ready(Err(error));
        }
        Poll::Ready(Ok(accepted))
    }
}

impl<S> AsyncRead for TlsStream<S>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut [u8],
    ) -> Poll<io::Result<usize>> {
        self.get_mut().poll_read_plaintext(cx, buf)
    }
}

impl<S> AsyncWrite for TlsStream<S>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        self.get_mut()
            .poll_write_plaintext(cx, &[IoSlice::new(buf)])
    }

    /// Encrypts every slice into as few records as rustls will use.
    ///
    /// Worth overriding rather than inheriting the scalar default: each record
    /// costs a header and an authentication tag, and a caller that already
    /// separated a header from a body should not pay for two of them.
    fn poll_write_vectored(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        bufs: &[IoSlice<'_>],
    ) -> Poll<io::Result<usize>> {
        self.get_mut().poll_write_plaintext(cx, bufs)
    }

    /// Ignores `generation` and writes as an ordinary caller would.
    ///
    /// The generation identifies one caller's logical write to a
    /// runtime-backed resource. This stream is not one: what reaches the
    /// transport is ciphertext from a buffer the *stream* owns, whose delivery
    /// must continue no matter which plaintext future is still alive. Passing a
    /// caller's generation down would attach a shared record to whichever
    /// future happened to start it, so cancellation safety here comes from
    /// re-presenting the identical staged slice instead.
    fn poll_write_operation(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
        _generation: u64,
    ) -> Poll<io::Result<usize>> {
        self.get_mut()
            .poll_write_plaintext(cx, &[IoSlice::new(buf)])
    }

    /// Ignores `generation`, for the reason given on
    /// [`poll_write_operation`](Self::poll_write_operation).
    fn poll_write_vectored_operation(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        bufs: &[IoSlice<'_>],
        _generation: u64,
    ) -> Poll<io::Result<usize>> {
        self.get_mut().poll_write_plaintext(cx, bufs)
    }

    /// Writes every pending record to the transport and flushes it.
    ///
    /// This is what makes accepted plaintext visible to the peer. It does not
    /// end the session: no `close_notify` is sent, and the stream stays
    /// writable.
    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        let this = self.get_mut();
        ready!(this.poll_send(cx))?;
        Pin::new(&mut this.io).poll_flush(cx)
    }

    /// Ends the session with `close_notify`, then closes the transport.
    ///
    /// A TLS close is not a transport shutdown. `close_notify` is a record: it
    /// tells the peer that the byte stream ended where it says it ended, which
    /// is what lets the peer distinguish a complete message from one an
    /// attacker truncated. Sending it therefore has to happen *before* the
    /// transport's write direction goes away, and it can fail on its own.
    ///
    /// This waits for the alert to reach the transport, then closes the
    /// transport's write direction — for a
    /// [`TcpStream`](crate::net::TcpStream), a `shutdown(SHUT_WR)`. It does not
    /// wait for the peer's own `close_notify`; read until end of stream if the
    /// application protocol requires that acknowledgement. Reads go on working
    /// afterwards, so a half-closed session is still usable in the other
    /// direction. Writing after it fails with
    /// [`io::ErrorKind::BrokenPipe`].
    ///
    /// Calling it again is safe and is how a `close` future abandoned partway
    /// is resumed: the alert is queued once and the retry picks up wherever the
    /// transport stopped taking it. That includes retrying after a failure —
    /// `close_notify` is not re-queued, but a transport that errored once will
    /// keep reporting that error rather than silently reporting success.
    fn poll_close(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        let this = self.get_mut();
        if !this.close_notify_sent {
            // A `close_notify` before the handshake finishes is not something
            // the peer can decrypt; skip straight to closing the transport.
            if !this.conn.is_handshaking() {
                this.conn.send_close_notify();
            }
            this.close_notify_sent = true;
        }
        ready!(this.poll_send(cx))?;
        Pin::new(&mut this.io).poll_close(cx)
    }
}
