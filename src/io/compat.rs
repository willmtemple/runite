use core::pin::Pin;
use core::task::{Context, Poll};
use std::io;

use super::{AsyncRead, AsyncWrite};

pub struct Compat<T> {
    inner: T,
}

impl<T> Compat<T> {
    pub fn new(inner: T) -> Self {
        Self { inner }
    }

    pub fn into_inner(self) -> T {
        self.inner
    }

    pub fn get_ref(&self) -> &T {
        &self.inner
    }

    pub fn get_mut(&mut self) -> &mut T {
        &mut self.inner
    }
}

impl<T: AsyncRead + Unpin> futures_io::AsyncRead for Compat<T> {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut [u8],
    ) -> Poll<io::Result<usize>> {
        AsyncRead::poll_read(Pin::new(&mut self.inner), cx, buf)
    }
}

impl<T: AsyncWrite + Unpin> futures_io::AsyncWrite for Compat<T> {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        AsyncWrite::poll_write(Pin::new(&mut self.inner), cx, buf)
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        AsyncWrite::poll_flush(Pin::new(&mut self.inner), cx)
    }

    fn poll_close(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        AsyncWrite::poll_close(Pin::new(&mut self.inner), cx)
    }
}

pub struct FuturesCompat<T> {
    inner: T,
}

impl<T> FuturesCompat<T> {
    pub fn new(inner: T) -> Self {
        Self { inner }
    }

    pub fn into_inner(self) -> T {
        self.inner
    }

    pub fn get_ref(&self) -> &T {
        &self.inner
    }

    pub fn get_mut(&mut self) -> &mut T {
        &mut self.inner
    }
}

impl<T: futures_io::AsyncRead + Unpin> AsyncRead for FuturesCompat<T> {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut [u8],
    ) -> Poll<io::Result<usize>> {
        futures_io::AsyncRead::poll_read(Pin::new(&mut self.inner), cx, buf)
    }
}

impl<T: futures_io::AsyncWrite + Unpin> AsyncWrite for FuturesCompat<T> {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        futures_io::AsyncWrite::poll_write(Pin::new(&mut self.inner), cx, buf)
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        futures_io::AsyncWrite::poll_flush(Pin::new(&mut self.inner), cx)
    }

    fn poll_close(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        futures_io::AsyncWrite::poll_close(Pin::new(&mut self.inner), cx)
    }
}
