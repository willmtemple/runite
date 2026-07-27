//! Hyper runtime trait implementations for [`TcpStream`].
//!
//! Gated behind the `hyper` Cargo feature so consumers that do not need the HTTP
//! transport integration are not forced to pull in the `hyper` dependency. The
//! implementation inherits [`TcpStream`]'s current-thread, effectively `!Send`
//! transport model; it is a Hyper I/O adapter, not a Tokio socket or executor.

use core::pin::Pin;
use core::task::{Context, Poll};

use std::io;
use std::net::Shutdown;

use hyper::rt::{Read as HyperRead, ReadBufCursor, Write as HyperWrite};

use super::TcpStream;
use crate::io::IoFuture;

impl HyperRead for TcpStream {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        mut buf: ReadBufCursor<'_>,
    ) -> Poll<Result<(), io::Error>> {
        let this = self.get_mut();
        if buf.remaining() == 0 {
            return Poll::Ready(Ok(()));
        }

        let capacity = buf.remaining();
        let timeout = this.read_timeout_value();
        let fd = this.raw_fd();
        this.read_state
            .get_mut()
            .poll_with(
                cx,
                capacity,
                move |len| match timeout {
                    Some(timeout) => {
                        Box::pin(crate::sys::current::net::recv_timeout(fd, len, 0, timeout))
                    }
                    None => crate::sys::current::net::recv_future(fd, len),
                },
                |bytes| buf.put_slice(bytes),
            )
            .map(|result| result.map(|_| ()))
    }
}

impl HyperWrite for TcpStream {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<Result<usize, io::Error>> {
        let this = self.get_mut();
        if buf.is_empty() {
            return Poll::Ready(Ok(0));
        }

        let timeout = this.write_timeout_value();
        let fd = this.raw_fd();
        this.write_state
            .get_mut()
            .poll_buffered_write(cx, buf, move |data| send_all_future(fd, data, timeout))
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), io::Error>> {
        self.get_mut().write_state.get_mut().poll_flush(cx)
    }

    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), io::Error>> {
        let this = self.get_mut();
        let fd = this.raw_fd();
        this.write_state.get_mut().poll_shutdown(cx, move || {
            crate::sys::current::net::shutdown_future(fd, Shutdown::Write)
        })
    }
}

fn send_all_future(
    fd: crate::sys::handle::RawSock,
    data: Vec<u8>,
    timeout: Option<core::time::Duration>,
) -> IoFuture<usize> {
    Box::pin(async move {
        let mut written = 0;
        while written < data.len() {
            let count = match timeout {
                Some(timeout) => {
                    crate::sys::current::net::send_timeout(
                        crate::sys::handle::clone_raw_sock(&fd),
                        data[written..].to_vec(),
                        0,
                        timeout,
                    )
                    .await?
                }
                None => {
                    crate::sys::current::net::send_future(
                        crate::sys::handle::clone_raw_sock(&fd),
                        data[written..].to_vec(),
                    )
                    .await?
                }
            };
            if count == 0 {
                return Err(io::Error::new(
                    io::ErrorKind::WriteZero,
                    "failed to write buffered Hyper data",
                ));
            }
            written += count;
        }
        Ok(written)
    })
}

#[cfg(test)]
mod tests {
    use core::future::Future;
    use core::pin::Pin;
    use core::task::{Context, Poll};
    use std::future::poll_fn;
    use std::io;
    use std::net::SocketAddr;
    use std::sync::{Arc, Mutex};

    use hyper::rt::Write as HyperWrite;

    use crate::net::{TcpListener, TcpStream};
    use crate::{queue_macrotask, run, spawn};

    struct PendingOnce<T> {
        pending: bool,
        result: Option<io::Result<T>>,
    }

    impl<T: Unpin> Future for PendingOnce<T> {
        type Output = io::Result<T>;

        fn poll(mut self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Self::Output> {
            if self.pending {
                self.pending = false;
                Poll::Pending
            } else {
                Poll::Ready(self.result.take().expect("polled after completion"))
            }
        }
    }

    #[test]
    fn write_does_not_reuse_an_abandoned_write_count() {
        let passed = Arc::new(Mutex::new(false));
        let passed_for_task = Arc::clone(&passed);
        queue_macrotask(move || {
            spawn(async move {
                let listener = TcpListener::bind(SocketAddr::from(([127, 0, 0, 1], 0)))
                    .await
                    .expect("listener should bind");
                let addr = listener.local_addr().expect("listener address");
                let accepted = spawn(async move { listener.accept().await.expect("accept").0 });
                let mut client = TcpStream::connect(addr).await.expect("connect");
                let mut peer = accepted.await.expect("accept task");
                let old = b"old".to_vec();

                poll_fn(|cx| {
                    assert!(
                        client
                            .write_state
                            .get_mut()
                            .poll_write(cx, 1, &old, |_| {
                                Box::pin(PendingOnce {
                                    pending: true,
                                    result: Some(Ok(old.len())),
                                })
                            })
                            .is_pending()
                    );
                    Poll::Ready(())
                })
                .await;

                let new = b"hyper bytes".to_vec();
                let written = poll_fn(|cx| HyperWrite::poll_write(Pin::new(&mut client), cx, &new))
                    .await
                    .expect("hyper write");
                assert_eq!(written, new.len());
                let mut received = vec![0; new.len()];
                peer.read_exact(&mut received)
                    .await
                    .expect("peer reads hyper write");
                assert_eq!(received, new);
                *passed_for_task.lock().unwrap() = true;
            });
        });
        run();

        assert!(*passed.lock().unwrap());
    }
}
