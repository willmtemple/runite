//! Compatibility adapters between runite's I/O traits and [`futures-io`].
//!
//! Available with the `futures-compat` feature. These adapters wrap a runite
//! [`AsyncRead`]/[`AsyncWrite`] so it can be used where the `futures-io` traits
//! are expected, and vice versa, easing interop with the broader `futures`
//! ecosystem.
//!
//! # Examples
//!
//! ```
//! use core::pin::Pin;
//! use core::task::{Context, Poll};
//! use std::io;
//!
//! use runite::io::compat::FuturesCompat;
//! use runite::io::AsyncReadExt;
//!
//! struct FuturesBytes(&'static [u8]);
//!
//! impl futures_io::AsyncRead for FuturesBytes {
//!     fn poll_read(
//!         mut self: Pin<&mut Self>,
//!         _cx: &mut Context<'_>,
//!         buf: &mut [u8],
//!     ) -> Poll<io::Result<usize>> {
//!         let read = buf.len().min(self.0.len());
//!         buf[..read].copy_from_slice(&self.0[..read]);
//!         self.0 = &self.0[read..];
//!         Poll::Ready(Ok(read))
//!     }
//! }
//!
//! runite::spawn(async {
//!     let mut reader = FuturesCompat::new(FuturesBytes(b"compat"));
//!     let mut out = Vec::new();
//!     reader.read_to_end(&mut out).await.unwrap();
//!     assert_eq!(&out, b"compat");
//! });
//! runite::run();
//! ```
//!
//! [`futures-io`]: https://docs.rs/futures-io

use core::pin::Pin;
use core::task::{Context, Poll};
use std::io;

use super::{AsyncRead, AsyncWrite};

/// Adapts a runite reader or writer to the `futures-io` traits.
///
/// This type is available with the `futures-compat` feature. It wraps a value
/// that implements runite's [`AsyncRead`] and/or [`AsyncWrite`] and exposes the
/// corresponding `futures_io` traits for integration with libraries that accept
/// those traits.
///
/// # Examples
///
/// This example is ignored by default because the module only exists when the
/// crate is built with `--features futures-compat`.
///
/// ```ignore
/// use runite::io::compat::Compat;
///
/// # let runite_reader = unimplemented!();
/// let futures_reader = Compat::new(runite_reader);
/// ```
pub struct Compat<T> {
    inner: T,
}

impl<T> Compat<T> {
    /// Wraps `inner` for use through `futures_io` traits.
    pub fn new(inner: T) -> Self {
        Self { inner }
    }

    /// Consumes the adapter and returns the wrapped value.
    pub fn into_inner(self) -> T {
        self.inner
    }

    /// Returns a shared reference to the wrapped value.
    pub fn get_ref(&self) -> &T {
        &self.inner
    }

    /// Returns a mutable reference to the wrapped value.
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

/// Adapts a `futures-io` reader or writer to runite's I/O traits.
///
/// This type is available with the `futures-compat` feature. It wraps a value
/// that implements `futures_io::AsyncRead` and/or `futures_io::AsyncWrite` and
/// exposes runite's [`AsyncRead`] and [`AsyncWrite`] traits.
///
/// # Examples
///
/// This example is ignored by default because the module only exists when the
/// crate is built with `--features futures-compat`.
///
/// ```ignore
/// use runite::io::compat::FuturesCompat;
///
/// # let futures_reader = unimplemented!();
/// let runite_reader = FuturesCompat::new(futures_reader);
/// ```
pub struct FuturesCompat<T> {
    inner: T,
}

impl<T> FuturesCompat<T> {
    /// Wraps `inner` for use through runite's async I/O traits.
    pub fn new(inner: T) -> Self {
        Self { inner }
    }

    /// Consumes the adapter and returns the wrapped value.
    pub fn into_inner(self) -> T {
        self.inner
    }

    /// Returns a shared reference to the wrapped value.
    pub fn get_ref(&self) -> &T {
        &self.inner
    }

    /// Returns a mutable reference to the wrapped value.
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
