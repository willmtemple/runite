#![cfg(feature = "futures-compat")]

//! Integration tests for futures-io compatibility adapters.

mod common;

use common::block_on;
use core::future::poll_fn;
use core::pin::Pin;
use core::task::{Context, Poll};
use runite::fs::{self, File};
use runite::io::compat::{Compat, FuturesCompat};
use runite::io::{
    AsyncBufRead, AsyncRead, AsyncReadExt, AsyncSeek, AsyncSeekExt, AsyncWrite, AsyncWriteExt,
    SeekFrom,
};
use runite::net::{TcpListener, TcpStream};
use std::collections::VecDeque;
use std::io::{self, IoSlice, IoSliceMut};

fn temp_path(label: &str) -> std::path::PathBuf {
    std::env::current_dir()
        .expect("current directory")
        .join("target")
        .join(format!(
            "runite-io-compat-{}-{}-{label}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .expect("clock after epoch")
                .as_nanos()
        ))
}

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

fn seek_position(len: usize, current: usize, position: SeekFrom) -> io::Result<usize> {
    let next = match position {
        SeekFrom::Start(position) => i128::from(position),
        SeekFrom::End(offset) => len as i128 + i128::from(offset),
        SeekFrom::Current(offset) => current as i128 + i128::from(offset),
    };
    usize::try_from(next)
        .ok()
        .filter(|position| *position <= len)
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "invalid memory seek"))
}

struct RuniteParity {
    data: &'static [u8],
    position: usize,
    written: Vec<u8>,
}

impl AsyncRead for RuniteParity {
    fn poll_read(
        mut self: Pin<&mut Self>,
        _cx: &mut Context<'_>,
        buf: &mut [u8],
    ) -> Poll<io::Result<usize>> {
        let read = buf.len().min(self.data.len() - self.position);
        buf[..read].copy_from_slice(&self.data[self.position..self.position + read]);
        self.position += read;
        Poll::Ready(Ok(read))
    }

    fn poll_read_vectored(
        mut self: Pin<&mut Self>,
        _cx: &mut Context<'_>,
        bufs: &mut [IoSliceMut<'_>],
    ) -> Poll<io::Result<usize>> {
        let mut total = 0;
        for buf in bufs {
            let read = buf.len().min(self.data.len() - self.position);
            buf[..read].copy_from_slice(&self.data[self.position..self.position + read]);
            self.position += read;
            total += read;
            if read < buf.len() {
                break;
            }
        }
        Poll::Ready(Ok(total))
    }
}

impl AsyncBufRead for RuniteParity {
    fn poll_fill_buf(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<&[u8]>> {
        let this = self.get_mut();
        Poll::Ready(Ok(&this.data[this.position..]))
    }

    fn consume(mut self: Pin<&mut Self>, amount: usize) {
        self.position = self.data.len().min(self.position.saturating_add(amount));
    }
}

impl AsyncWrite for RuniteParity {
    fn poll_write(
        mut self: Pin<&mut Self>,
        _cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        self.written.extend_from_slice(buf);
        Poll::Ready(Ok(buf.len()))
    }

    fn poll_write_vectored(
        mut self: Pin<&mut Self>,
        _cx: &mut Context<'_>,
        bufs: &[IoSlice<'_>],
    ) -> Poll<io::Result<usize>> {
        let mut total = 0;
        for buf in bufs {
            self.written.extend_from_slice(buf);
            total += buf.len();
        }
        Poll::Ready(Ok(total))
    }

    fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Poll::Ready(Ok(()))
    }

    fn poll_close(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Poll::Ready(Ok(()))
    }
}

impl AsyncSeek for RuniteParity {
    fn poll_seek(
        mut self: Pin<&mut Self>,
        _cx: &mut Context<'_>,
        position: SeekFrom,
    ) -> Poll<io::Result<u64>> {
        let next = seek_position(self.data.len(), self.position, position)?;
        self.position = next;
        Poll::Ready(Ok(next as u64))
    }
}

#[test]
fn compat_forwards_buffered_vectored_and_seek_semantics() {
    block_on(|| async {
        let mut compat = Compat::new(RuniteParity {
            data: b"abcdef",
            position: 0,
            written: Vec::new(),
        });

        let mut first = [0; 2];
        let mut second = [0; 2];
        let mut read_bufs = [IoSliceMut::new(&mut first), IoSliceMut::new(&mut second)];
        assert_eq!(
            poll_fn(|cx| futures_io::AsyncRead::poll_read_vectored(
                Pin::new(&mut compat),
                cx,
                &mut read_bufs,
            ))
            .await
            .expect("vectored read"),
            4
        );
        assert_eq!(&first, b"ab");
        assert_eq!(&second, b"cd");

        assert_eq!(
            poll_fn(|cx| futures_io::AsyncSeek::poll_seek(
                Pin::new(&mut compat),
                cx,
                SeekFrom::Start(1),
            ))
            .await
            .expect("seek"),
            1
        );
        let buffered = poll_fn(|cx| {
            match futures_io::AsyncBufRead::poll_fill_buf(Pin::new(&mut compat), cx) {
                Poll::Ready(Ok(buf)) => Poll::Ready(Ok(buf.to_vec())),
                Poll::Ready(Err(error)) => Poll::Ready(Err(error)),
                Poll::Pending => Poll::Pending,
            }
        })
        .await
        .expect("fill buffer");
        assert_eq!(buffered, b"bcdef");
        futures_io::AsyncBufRead::consume(Pin::new(&mut compat), 2);
        assert_eq!(compat.get_ref().position, 3);

        let write_bufs = [IoSlice::new(b"left"), IoSlice::new(b"right")];
        assert_eq!(
            poll_fn(|cx| futures_io::AsyncWrite::poll_write_vectored(
                Pin::new(&mut compat),
                cx,
                &write_bufs,
            ))
            .await
            .expect("vectored write"),
            9
        );
        assert_eq!(compat.into_inner().written, b"leftright");
    });
}

struct FuturesParity {
    data: &'static [u8],
    position: usize,
    written: Vec<u8>,
}

impl futures_io::AsyncRead for FuturesParity {
    fn poll_read(
        mut self: Pin<&mut Self>,
        _cx: &mut Context<'_>,
        buf: &mut [u8],
    ) -> Poll<io::Result<usize>> {
        let read = buf.len().min(self.data.len() - self.position);
        buf[..read].copy_from_slice(&self.data[self.position..self.position + read]);
        self.position += read;
        Poll::Ready(Ok(read))
    }

    fn poll_read_vectored(
        mut self: Pin<&mut Self>,
        _cx: &mut Context<'_>,
        bufs: &mut [IoSliceMut<'_>],
    ) -> Poll<io::Result<usize>> {
        let mut total = 0;
        for buf in bufs {
            let read = buf.len().min(self.data.len() - self.position);
            buf[..read].copy_from_slice(&self.data[self.position..self.position + read]);
            self.position += read;
            total += read;
            if read < buf.len() {
                break;
            }
        }
        Poll::Ready(Ok(total))
    }
}

impl futures_io::AsyncBufRead for FuturesParity {
    fn poll_fill_buf(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<&[u8]>> {
        let this = self.get_mut();
        Poll::Ready(Ok(&this.data[this.position..]))
    }

    fn consume(mut self: Pin<&mut Self>, amount: usize) {
        self.position = self.data.len().min(self.position.saturating_add(amount));
    }
}

impl futures_io::AsyncWrite for FuturesParity {
    fn poll_write(
        mut self: Pin<&mut Self>,
        _cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        self.written.extend_from_slice(buf);
        Poll::Ready(Ok(buf.len()))
    }

    fn poll_write_vectored(
        mut self: Pin<&mut Self>,
        _cx: &mut Context<'_>,
        bufs: &[IoSlice<'_>],
    ) -> Poll<io::Result<usize>> {
        let mut total = 0;
        for buf in bufs {
            self.written.extend_from_slice(buf);
            total += buf.len();
        }
        Poll::Ready(Ok(total))
    }

    fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Poll::Ready(Ok(()))
    }

    fn poll_close(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Poll::Ready(Ok(()))
    }
}

impl futures_io::AsyncSeek for FuturesParity {
    fn poll_seek(
        mut self: Pin<&mut Self>,
        _cx: &mut Context<'_>,
        position: SeekFrom,
    ) -> Poll<io::Result<u64>> {
        let next = seek_position(self.data.len(), self.position, position)?;
        self.position = next;
        Poll::Ready(Ok(next as u64))
    }
}

#[test]
fn futures_compat_forwards_buffered_vectored_and_seek_semantics() {
    block_on(|| async {
        let mut compat = FuturesCompat::new(FuturesParity {
            data: b"abcdef",
            position: 0,
            written: Vec::new(),
        });

        let mut first = [0; 2];
        let mut second = [0; 2];
        let mut read_bufs = [IoSliceMut::new(&mut first), IoSliceMut::new(&mut second)];
        assert_eq!(
            compat
                .read_vectored(&mut read_bufs)
                .await
                .expect("vectored read"),
            4
        );
        assert_eq!(&first, b"ab");
        assert_eq!(&second, b"cd");

        assert_eq!(compat.seek(SeekFrom::Start(1)).await.expect("seek"), 1);
        let buffered = poll_fn(
            |cx| match AsyncBufRead::poll_fill_buf(Pin::new(&mut compat), cx) {
                Poll::Ready(Ok(buf)) => Poll::Ready(Ok(buf.to_vec())),
                Poll::Ready(Err(error)) => Poll::Ready(Err(error)),
                Poll::Pending => Poll::Pending,
            },
        )
        .await
        .expect("fill buffer");
        assert_eq!(buffered, b"bcdef");
        AsyncBufRead::consume(Pin::new(&mut compat), 2);
        assert_eq!(compat.get_ref().position, 3);

        let write_bufs = [IoSlice::new(b"left"), IoSlice::new(b"right")];
        assert_eq!(
            compat
                .write_vectored(&write_bufs)
                .await
                .expect("vectored write"),
            9
        );
        assert_eq!(compat.into_inner().written, b"leftright");
    });
}

#[test]
fn compat_file_scalar_write_cancellation_cannot_credit_the_next_buffer() {
    let path = temp_path("scalar-write-cancel");
    block_on(move || async move {
        let file = File::create(&path).await.expect("create fixture");
        let mut compat = Compat::new(file);

        let first_poll = poll_fn(|cx| {
            Poll::Ready(futures_io::AsyncWrite::poll_write(
                Pin::new(&mut compat),
                cx,
                b"old",
            ))
        })
        .await;
        assert!(matches!(first_poll, Poll::Ready(Ok(3))));

        futures_write_all(&mut compat, b"new")
            .await
            .expect("new logical write");
        poll_fn(|cx| futures_io::AsyncWrite::poll_flush(Pin::new(&mut compat), cx))
            .await
            .expect("flush accepted adapter data");
        drop(compat.into_inner());

        let contents = fs::read(&path).await.expect("read fixture");
        assert_eq!(contents, b"oldnew");
        fs::remove_file(&path).await.expect("remove fixture");
    });
}

#[test]
fn compat_file_vectored_write_cancellation_cannot_credit_the_next_buffers() {
    let path = temp_path("vectored-write-cancel");
    block_on(move || async move {
        let file = File::create(&path).await.expect("create fixture");
        let mut compat = Compat::new(file);
        let old = [IoSlice::new(b"old")];

        let first_poll = poll_fn(|cx| {
            Poll::Ready(futures_io::AsyncWrite::poll_write_vectored(
                Pin::new(&mut compat),
                cx,
                &old,
            ))
        })
        .await;
        assert!(matches!(first_poll, Poll::Ready(Ok(3))));

        let new = [IoSlice::new(b"new")];
        assert_eq!(
            poll_fn(|cx| futures_io::AsyncWrite::poll_write_vectored(
                Pin::new(&mut compat),
                cx,
                &new,
            ))
            .await
            .expect("new logical vectored write"),
            3
        );
        poll_fn(|cx| futures_io::AsyncWrite::poll_flush(Pin::new(&mut compat), cx))
            .await
            .expect("flush accepted adapter data");
        drop(compat.into_inner());

        let contents = fs::read(&path).await.expect("read fixture");
        assert_eq!(contents, b"oldnew");
        fs::remove_file(&path).await.expect("remove fixture");
    });
}

struct PendingShortCursor {
    data: Vec<u8>,
    position: usize,
    park_next: bool,
}

impl AsyncWrite for PendingShortCursor {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        if buf.is_empty() {
            return Poll::Ready(Ok(0));
        }
        if self.park_next {
            self.park_next = false;
            cx.waker().wake_by_ref();
            return Poll::Pending;
        }

        let written = buf.len().min(2);
        let start = self.position;
        let end = start + written;
        if self.data.len() < end {
            self.data.resize(end, 0);
        }
        self.data[start..end].copy_from_slice(&buf[..written]);
        self.position = end;
        self.park_next = true;
        Poll::Ready(Ok(written))
    }

    fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Poll::Ready(Ok(()))
    }

    fn poll_close(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Poll::Ready(Ok(()))
    }
}

impl AsyncSeek for PendingShortCursor {
    fn poll_seek(
        mut self: Pin<&mut Self>,
        _cx: &mut Context<'_>,
        position: SeekFrom,
    ) -> Poll<io::Result<u64>> {
        let next = seek_position(self.data.len(), self.position, position)?;
        self.position = next;
        Poll::Ready(Ok(next as u64))
    }
}

#[test]
fn compat_seek_requires_accepted_short_write_to_drain_before_moving_cursor() {
    block_on(|| async {
        let mut compat = Compat::new(PendingShortCursor {
            data: Vec::new(),
            position: 0,
            park_next: false,
        });

        assert_eq!(
            poll_fn(|cx| futures_io::AsyncWrite::poll_write(Pin::new(&mut compat), cx, b"abcdef",))
                .await
                .expect("accept compatibility buffer"),
            6
        );
        poll_fn(|cx| {
            assert!(
                futures_io::AsyncSeek::poll_seek(Pin::new(&mut compat), cx, SeekFrom::Start(0),)
                    .is_pending()
            );
            Poll::Ready(())
        })
        .await;
        assert_eq!(
            poll_fn(|cx| futures_io::AsyncSeek::poll_seek(
                Pin::new(&mut compat),
                cx,
                SeekFrom::Start(0),
            ))
            .await
            .expect("seek must drain accepted write"),
            0
        );
        poll_fn(|cx| futures_io::AsyncWrite::poll_flush(Pin::new(&mut compat), cx))
            .await
            .expect("flush after seek");

        let inner = compat.into_inner();
        assert_eq!(inner.data, b"abcdef");
        assert_eq!(inner.position, 0);
    });
}

#[test]
fn compat_file_seek_drains_accepted_write_before_repositioning() {
    let path = temp_path("seek-before-flush");
    block_on(move || async move {
        let file = File::create(&path).await.expect("create fixture");
        let mut compat = Compat::new(file);
        assert_eq!(
            poll_fn(|cx| futures_io::AsyncWrite::poll_write(Pin::new(&mut compat), cx, b"abcdef",))
                .await
                .expect("accept file write"),
            6
        );

        assert_eq!(
            poll_fn(|cx| futures_io::AsyncSeek::poll_seek(
                Pin::new(&mut compat),
                cx,
                SeekFrom::Start(0),
            ))
            .await
            .expect("seek must drain accepted file write"),
            0
        );
        futures_write_all(&mut compat, b"Z")
            .await
            .expect("overwrite after seek");
        poll_fn(|cx| futures_io::AsyncWrite::poll_flush(Pin::new(&mut compat), cx))
            .await
            .expect("flush overwrite");
        drop(compat.into_inner());

        let contents = fs::read(&path).await.expect("read fixture");
        assert_eq!(contents, b"Zbcdef");
        fs::remove_file(&path).await.expect("remove fixture");
    });
}

enum ScriptedWriteStep {
    Write(usize),
    Error,
    Zero,
}

struct ScriptedWriter {
    data: Vec<u8>,
    steps: VecDeque<ScriptedWriteStep>,
}

impl AsyncWrite for ScriptedWriter {
    fn poll_write(
        mut self: Pin<&mut Self>,
        _cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        match self.steps.pop_front().expect("scripted write step") {
            ScriptedWriteStep::Write(limit) => {
                let written = limit.min(buf.len());
                self.data.extend_from_slice(&buf[..written]);
                Poll::Ready(Ok(written))
            }
            ScriptedWriteStep::Error => {
                Poll::Ready(Err(io::Error::other("scripted drain failure")))
            }
            ScriptedWriteStep::Zero => Poll::Ready(Ok(0)),
        }
    }

    fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Poll::Ready(Ok(()))
    }

    fn poll_close(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Poll::Ready(Ok(()))
    }
}

#[test]
fn compat_preserves_error_after_acknowledged_short_prefix() {
    block_on(|| async {
        let mut compat = Compat::new(ScriptedWriter {
            data: Vec::new(),
            steps: VecDeque::from([ScriptedWriteStep::Write(2), ScriptedWriteStep::Error]),
        });
        assert_eq!(
            poll_fn(|cx| futures_io::AsyncWrite::poll_write(Pin::new(&mut compat), cx, b"abcdef",))
                .await
                .expect("short prefix must acknowledge owned adapter buffer"),
            6
        );

        let error = poll_fn(|cx| futures_io::AsyncWrite::poll_flush(Pin::new(&mut compat), cx))
            .await
            .expect_err("later drain failure must surface on flush");
        assert_eq!(error.kind(), io::ErrorKind::Other);
        assert_eq!(compat.into_inner().data, b"ab");
    });
}

#[test]
fn compat_preserves_write_zero_after_acknowledged_short_prefix() {
    block_on(|| async {
        let mut compat = Compat::new(ScriptedWriter {
            data: Vec::new(),
            steps: VecDeque::from([ScriptedWriteStep::Write(2), ScriptedWriteStep::Zero]),
        });
        assert_eq!(
            poll_fn(|cx| futures_io::AsyncWrite::poll_write(Pin::new(&mut compat), cx, b"abcdef",))
                .await
                .expect("short prefix must acknowledge owned adapter buffer"),
            6
        );

        let error =
            poll_fn(|cx| futures_io::AsyncWrite::poll_write(Pin::new(&mut compat), cx, b"next"))
                .await
                .expect_err("later zero write must surface before accepting the next buffer");
        assert_eq!(error.kind(), io::ErrorKind::WriteZero);
        assert_eq!(compat.into_inner().data, b"ab");
    });
}

#[derive(Default)]
struct EmptyVectoredProbe {
    read_calls: usize,
    write_calls: usize,
}

impl futures_io::AsyncRead for EmptyVectoredProbe {
    fn poll_read(
        self: Pin<&mut Self>,
        _cx: &mut Context<'_>,
        _buf: &mut [u8],
    ) -> Poll<io::Result<usize>> {
        panic!("empty vectored reads must not reach the scalar foreign reader")
    }

    fn poll_read_vectored(
        mut self: Pin<&mut Self>,
        _cx: &mut Context<'_>,
        _bufs: &mut [IoSliceMut<'_>],
    ) -> Poll<io::Result<usize>> {
        self.read_calls += 1;
        Poll::Ready(Ok(99))
    }
}

impl futures_io::AsyncWrite for EmptyVectoredProbe {
    fn poll_write(
        self: Pin<&mut Self>,
        _cx: &mut Context<'_>,
        _buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        panic!("empty vectored writes must not reach the scalar foreign writer")
    }

    fn poll_write_vectored(
        mut self: Pin<&mut Self>,
        _cx: &mut Context<'_>,
        _bufs: &[IoSlice<'_>],
    ) -> Poll<io::Result<usize>> {
        self.write_calls += 1;
        Poll::Ready(Ok(99))
    }

    fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Poll::Ready(Ok(()))
    }

    fn poll_close(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Poll::Ready(Ok(()))
    }
}

#[test]
fn futures_compat_short_circuits_empty_vectored_io() {
    block_on(|| async {
        let mut compat = FuturesCompat::new(EmptyVectoredProbe::default());
        let mut no_reads: [IoSliceMut<'_>; 0] = [];
        assert_eq!(
            poll_fn(|cx| AsyncRead::poll_read_vectored(Pin::new(&mut compat), cx, &mut no_reads,))
                .await
                .expect("empty read list"),
            0
        );

        let (mut empty_a, mut empty_b) = ([], []);
        let mut empty_reads = [IoSliceMut::new(&mut empty_a), IoSliceMut::new(&mut empty_b)];
        assert_eq!(
            poll_fn(|cx| AsyncRead::poll_read_vectored(
                Pin::new(&mut compat),
                cx,
                &mut empty_reads,
            ))
            .await
            .expect("all-empty read list"),
            0
        );

        let no_writes: [IoSlice<'_>; 0] = [];
        assert_eq!(
            poll_fn(|cx| AsyncWrite::poll_write_vectored(Pin::new(&mut compat), cx, &no_writes,))
                .await
                .expect("empty write list"),
            0
        );
        let empty_writes = [IoSlice::new(b""), IoSlice::new(b"")];
        assert_eq!(
            compat
                .write_vectored(&empty_writes)
                .await
                .expect("all-empty write list"),
            0
        );
        assert_eq!(compat.get_ref().read_calls, 0);
        assert_eq!(compat.get_ref().write_calls, 0);
    });
}
