use core::future::Future;
use core::pin::Pin;
use core::task::{Context, Poll};
use std::io;

use super::{AsyncRead, AsyncWrite, Stream};

const READ_TO_END_CHUNK: usize = 8192;

pub trait AsyncReadExt: AsyncRead {
    fn read<'a>(&'a mut self, buf: &'a mut [u8]) -> Read<'a, Self>
    where
        Self: Unpin,
    {
        Read { reader: self, buf }
    }

    fn read_exact<'a>(&'a mut self, buf: &'a mut [u8]) -> ReadExact<'a, Self>
    where
        Self: Unpin,
    {
        ReadExact {
            reader: self,
            buf,
            filled: 0,
        }
    }

    fn read_to_end<'a>(&'a mut self, buf: &'a mut Vec<u8>) -> ReadToEnd<'a, Self>
    where
        Self: Unpin,
    {
        ReadToEnd {
            reader: self,
            buf,
            chunk: vec![0; READ_TO_END_CHUNK],
            start_len: 0,
            initialized: false,
        }
    }

    fn lines(self) -> Lines<Self>
    where
        Self: Sized,
    {
        Lines {
            reader: self,
            buf: Vec::new(),
            chunk: vec![0; READ_TO_END_CHUNK],
            eof: false,
        }
    }
}

impl<R: AsyncRead + ?Sized> AsyncReadExt for R {}

pub trait AsyncWriteExt: AsyncWrite {
    fn write<'a>(&'a mut self, buf: &'a [u8]) -> Write<'a, Self>
    where
        Self: Unpin,
    {
        Write { writer: self, buf }
    }

    fn write_all<'a>(&'a mut self, buf: &'a [u8]) -> WriteAll<'a, Self>
    where
        Self: Unpin,
    {
        WriteAll {
            writer: self,
            buf,
            written: 0,
        }
    }

    fn flush(&mut self) -> Flush<'_, Self>
    where
        Self: Unpin,
    {
        Flush { writer: self }
    }

    fn close(&mut self) -> Close<'_, Self>
    where
        Self: Unpin,
    {
        Close { writer: self }
    }
}

impl<W: AsyncWrite + ?Sized> AsyncWriteExt for W {}

pub struct Read<'a, R: ?Sized> {
    reader: &'a mut R,
    buf: &'a mut [u8],
}

impl<R: AsyncRead + Unpin + ?Sized> Future for Read<'_, R> {
    type Output = io::Result<usize>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = &mut *self;
        Pin::new(&mut *this.reader).poll_read(cx, this.buf)
    }
}

pub struct ReadExact<'a, R: ?Sized> {
    reader: &'a mut R,
    buf: &'a mut [u8],
    filled: usize,
}

impl<R: AsyncRead + Unpin + ?Sized> Future for ReadExact<'_, R> {
    type Output = io::Result<()>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = &mut *self;
        while this.filled < this.buf.len() {
            let read = match Pin::new(&mut *this.reader).poll_read(cx, &mut this.buf[this.filled..])
            {
                Poll::Ready(result) => result?,
                Poll::Pending => return Poll::Pending,
            };
            if read == 0 {
                return Poll::Ready(Err(io::Error::new(
                    io::ErrorKind::UnexpectedEof,
                    "failed to fill whole buffer",
                )));
            }
            this.filled += read;
        }
        Poll::Ready(Ok(()))
    }
}

pub struct ReadToEnd<'a, R: ?Sized> {
    reader: &'a mut R,
    buf: &'a mut Vec<u8>,
    chunk: Vec<u8>,
    start_len: usize,
    initialized: bool,
}

impl<R: AsyncRead + Unpin + ?Sized> Future for ReadToEnd<'_, R> {
    type Output = io::Result<usize>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = &mut *self;
        if !this.initialized {
            this.start_len = this.buf.len();
            this.initialized = true;
        }

        loop {
            let read = match Pin::new(&mut *this.reader).poll_read(cx, &mut this.chunk) {
                Poll::Ready(result) => result?,
                Poll::Pending => return Poll::Pending,
            };
            if read == 0 {
                return Poll::Ready(Ok(this.buf.len() - this.start_len));
            }
            this.buf.extend_from_slice(&this.chunk[..read]);
        }
    }
}

pub struct Write<'a, W: ?Sized> {
    writer: &'a mut W,
    buf: &'a [u8],
}

impl<W: AsyncWrite + Unpin + ?Sized> Future for Write<'_, W> {
    type Output = io::Result<usize>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = &mut *self;
        Pin::new(&mut *this.writer).poll_write(cx, this.buf)
    }
}

pub struct WriteAll<'a, W: ?Sized> {
    writer: &'a mut W,
    buf: &'a [u8],
    written: usize,
}

impl<W: AsyncWrite + Unpin + ?Sized> Future for WriteAll<'_, W> {
    type Output = io::Result<()>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = &mut *self;
        while this.written < this.buf.len() {
            let written =
                match Pin::new(&mut *this.writer).poll_write(cx, &this.buf[this.written..]) {
                    Poll::Ready(result) => result?,
                    Poll::Pending => return Poll::Pending,
                };
            if written == 0 {
                return Poll::Ready(Err(io::Error::new(
                    io::ErrorKind::WriteZero,
                    "failed to write whole buffer",
                )));
            }
            this.written += written;
        }
        Poll::Ready(Ok(()))
    }
}

pub struct Flush<'a, W: ?Sized> {
    writer: &'a mut W,
}

impl<W: AsyncWrite + Unpin + ?Sized> Future for Flush<'_, W> {
    type Output = io::Result<()>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        Pin::new(&mut *self.writer).poll_flush(cx)
    }
}

pub struct Close<'a, W: ?Sized> {
    writer: &'a mut W,
}

impl<W: AsyncWrite + Unpin + ?Sized> Future for Close<'_, W> {
    type Output = io::Result<()>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        Pin::new(&mut *self.writer).poll_close(cx)
    }
}

pub struct Lines<R> {
    reader: R,
    buf: Vec<u8>,
    chunk: Vec<u8>,
    eof: bool,
}

impl<R> Unpin for Lines<R> {}

impl<R: AsyncRead + Unpin> Stream for Lines<R> {
    type Item = io::Result<String>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();
        loop {
            if let Some(newline) = this.buf.iter().position(|byte| *byte == b'\n') {
                let mut line = this.buf.drain(..=newline).collect::<Vec<_>>();
                let _ = line.pop();
                return Poll::Ready(Some(string_from_line(line)));
            }

            if this.eof {
                if this.buf.is_empty() {
                    return Poll::Ready(None);
                }
                let line = core::mem::take(&mut this.buf);
                return Poll::Ready(Some(string_from_line(line)));
            }

            match Pin::new(&mut this.reader).poll_read(cx, &mut this.chunk) {
                Poll::Ready(Ok(0)) => this.eof = true,
                Poll::Ready(Ok(read)) => this.buf.extend_from_slice(&this.chunk[..read]),
                Poll::Ready(Err(error)) => return Poll::Ready(Some(Err(error))),
                Poll::Pending => return Poll::Pending,
            }
        }
    }
}

fn string_from_line(line: Vec<u8>) -> io::Result<String> {
    String::from_utf8(line).map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error))
}
