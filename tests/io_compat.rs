#![cfg(feature = "futures-compat")]

//! Integration tests for futures-io compatibility adapters.

mod common;

use common::block_on;
use core::future::poll_fn;
use core::pin::Pin;
use core::task::{Context, Poll};
use runite::io::compat::{Compat, FuturesCompat};
use runite::io::{AsyncReadExt, AsyncWriteExt};
use runite::net::{TcpListener, TcpStream};
use std::io;

async fn futures_write_all<W: futures_io::AsyncWrite + Unpin>(
    writer: &mut W,
    mut buf: &[u8],
) -> io::Result<()> {
    while !buf.is_empty() {
        let written =
            poll_fn(|cx| futures_io::AsyncWrite::poll_write(Pin::new(&mut *writer), cx, buf))
                .await?;
        if written == 0 {
            return Err(io::Error::new(
                io::ErrorKind::WriteZero,
                "futures writer wrote zero bytes",
            ));
        }
        buf = &buf[written..];
    }
    Ok(())
}

async fn futures_read_exact<R: futures_io::AsyncRead + Unpin>(
    reader: &mut R,
    mut buf: &mut [u8],
) -> io::Result<()> {
    while !buf.is_empty() {
        let read =
            poll_fn(|cx| futures_io::AsyncRead::poll_read(Pin::new(&mut *reader), cx, buf)).await?;
        if read == 0 {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "futures reader reached eof",
            ));
        }
        let (_, rest) = buf.split_at_mut(read);
        buf = rest;
    }
    Ok(())
}

#[test]
fn compat_exposes_runite_tcp_stream_as_futures_io() {
    block_on(|| async {
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind listener");
        let addr = listener.local_addr().expect("listener address");

        let server = runite::spawn(async move {
            let (mut stream, _) = listener.accept().await.expect("accept client");
            let mut request = [0; 4];
            stream
                .read_exact(&mut request)
                .await
                .expect("server read request");
            assert_eq!(&request, b"ping");
            stream
                .write_all(b"pong")
                .await
                .expect("server write response");
        });

        let client = TcpStream::connect(addr).await.expect("connect client");
        let mut compat = Compat::new(client);
        assert!(compat.get_ref().peer_addr().is_ok());

        futures_write_all(&mut compat, b"ping")
            .await
            .expect("futures write request");
        poll_fn(|cx| futures_io::AsyncWrite::poll_flush(Pin::new(&mut compat), cx))
            .await
            .expect("futures flush");

        let mut response = [0; 4];
        futures_read_exact(&mut compat, &mut response)
            .await
            .expect("futures read response");
        assert_eq!(&response, b"pong");

        let _client = compat.into_inner();
        server.await.expect("server task should complete");
    });
}

struct FuturesMemory {
    read: &'static [u8],
    written: Vec<u8>,
    flushed: bool,
    closed: bool,
}

impl futures_io::AsyncRead for FuturesMemory {
    fn poll_read(
        mut self: Pin<&mut Self>,
        _cx: &mut Context<'_>,
        buf: &mut [u8],
    ) -> Poll<io::Result<usize>> {
        let read = buf.len().min(self.read.len());
        buf[..read].copy_from_slice(&self.read[..read]);
        self.read = &self.read[read..];
        Poll::Ready(Ok(read))
    }
}

impl futures_io::AsyncWrite for FuturesMemory {
    fn poll_write(
        mut self: Pin<&mut Self>,
        _cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        let written = buf.len().min(2);
        self.written.extend_from_slice(&buf[..written]);
        Poll::Ready(Ok(written))
    }

    fn poll_flush(mut self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        self.flushed = true;
        Poll::Ready(Ok(()))
    }

    fn poll_close(mut self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        self.closed = true;
        Poll::Ready(Ok(()))
    }
}

#[test]
fn futures_compat_exposes_futures_io_as_runite_traits() {
    block_on(|| async {
        let inner = FuturesMemory {
            read: b"from futures",
            written: Vec::new(),
            flushed: false,
            closed: false,
        };
        let mut compat = FuturesCompat::new(inner);

        assert_eq!(compat.get_ref().written, b"");
        compat.get_mut().written.extend_from_slice(b"pre:");

        let mut read = Vec::new();
        compat
            .read_to_end(&mut read)
            .await
            .expect("runite read from futures reader");
        assert_eq!(&read, b"from futures");

        compat
            .write_all(b"to runite")
            .await
            .expect("runite write to futures writer");
        compat.flush().await.expect("runite flush");
        compat.close().await.expect("runite close");

        let inner = compat.into_inner();
        assert_eq!(&inner.written, b"pre:to runite");
        assert!(inner.flushed);
        assert!(inner.closed);
    });
}
