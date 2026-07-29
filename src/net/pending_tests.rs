use core::future::Future;
use core::pin::Pin;
use core::task::{Context, Poll};
use std::future::poll_fn;
use std::io;
use std::net::{Shutdown, SocketAddr};
use std::sync::{Arc, Mutex};

use crate::io::{AsyncReadExt, AsyncWriteExt, next_operation_id};
use crate::{queue_macrotask, run, spawn};

use super::{TcpListener, TcpStream};

struct PendingOnce<T> {
    pending: bool,
    result: Option<io::Result<T>>,
}

impl<T> PendingOnce<T> {
    fn new(result: io::Result<T>) -> Self {
        Self {
            pending: true,
            result: Some(result),
        }
    }
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
fn split_and_reunite_preserve_pending_directional_state() {
    let preserved = Arc::new(Mutex::new(false));
    let preserved_for_task = Arc::clone(&preserved);
    queue_macrotask(move || {
        spawn(async move {
            let listener = TcpListener::bind(SocketAddr::from(([127, 0, 0, 1], 0)))
                .await
                .expect("listener should bind");
            let addr = listener.local_addr().expect("listener address");
            let accepted = spawn(async move { listener.accept().await.expect("accept").0 });
            let mut client = TcpStream::connect(addr).await.expect("connect");
            let mut peer = accepted.await.expect("accept task");
            let write_buf = b"tag".to_vec();
            let write_generation = next_operation_id();
            let mut discarded = [0; 8];

            poll_fn(|cx| {
                assert!(
                    client
                        .read_state
                        .borrow_mut()
                        .poll_slice(cx, &mut discarded, |_| {
                            Box::pin(PendingOnce::new(Ok(b"read".to_vec())))
                        })
                        .is_pending()
                );
                assert!(
                    client
                        .write_state
                        .get_mut()
                        .poll_write(cx, write_generation, &write_buf, |_| {
                            Box::pin(PendingOnce::new(Ok(write_buf.len())))
                        })
                        .is_pending()
                );
                Poll::Ready(())
            })
            .await;

            let (read, write) = client.into_split();
            let mut client = TcpStream::reunite(read, write).expect("reunite");
            let mut read_buf = [0; 4];
            assert_eq!(client.read(&mut read_buf).await.expect("finish read"), 4);
            assert_eq!(&read_buf, b"read");
            assert_eq!(
                poll_fn(|cx| client.write_state.get_mut().poll_write(
                    cx,
                    write_generation,
                    &write_buf,
                    |_| panic!("write already started")
                ))
                .await
                .expect("finish write"),
                write_buf.len()
            );

            let old_buf = b"old".to_vec();
            let old_generation = next_operation_id();
            poll_fn(|cx| {
                assert!(
                    client
                        .write_state
                        .get_mut()
                        .poll_write(cx, old_generation, &old_buf, |_| {
                            Box::pin(PendingOnce::new(Err(io::Error::new(
                                io::ErrorKind::TimedOut,
                                "abandoned timeout",
                            ))))
                        })
                        .is_pending()
                );
                Poll::Ready(())
            })
            .await;
            let new_buf = b"new bytes".to_vec();
            assert_eq!(
                client.write(&new_buf).await.expect("new write"),
                new_buf.len()
            );
            let mut received = vec![0; new_buf.len()];
            peer.read_exact(&mut received)
                .await
                .expect("peer reads new write");
            assert_eq!(received, new_buf);

            let before_shutdown = b"before shutdown".to_vec();
            let shutdown_write_generation = next_operation_id();
            poll_fn(|cx| {
                assert!(
                    client
                        .write_state
                        .get_mut()
                        .poll_write(cx, shutdown_write_generation, &before_shutdown, |_| {
                            Box::pin(PendingOnce::new(Ok(before_shutdown.len())))
                        })
                        .is_pending()
                );
                Poll::Ready(())
            })
            .await;
            client
                .shutdown(Shutdown::Write)
                .await
                .expect("ordered shutdown");
            let mut eof = [0];
            assert_eq!(peer.read(&mut eof).await.expect("peer EOF"), 0);
            drop(client);
            drop(peer);

            let listener = TcpListener::bind(SocketAddr::from(([127, 0, 0, 1], 0)))
                .await
                .expect("shutdown listener should bind");
            let addr = listener.local_addr().expect("shutdown listener address");
            let accepted = spawn(async move { listener.accept().await.expect("accept").0 });
            let mut client = TcpStream::connect(addr).await.expect("shutdown connect");
            let peer = accepted.await.expect("shutdown accept task");
            poll_fn(|cx| {
                assert!(
                    client
                        .write_state
                        .get_mut()
                        .poll_shutdown(cx, || { Box::pin(PendingOnce::new(Ok(()))) })
                        .is_pending()
                );
                Poll::Ready(())
            })
            .await;
            let (read, write) = client.into_split();
            let client = TcpStream::reunite(read, write).expect("reunite shutdown");
            client.close_descriptor().await.expect("finish shutdown");
            drop(peer);
            *preserved_for_task.lock().unwrap() = true;
        });
    });
    run();

    assert!(*preserved.lock().unwrap());
}

#[test]
fn shutdown_is_directional_and_concurrent_callers_share_completion() {
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
            let peer = accepted.await.expect("accept task");

            let mut write = Some(Box::pin(PendingOnce::new(Ok(3))));
            let write_generation = next_operation_id();
            poll_fn(|cx| {
                assert!(
                    client
                        .write_state
                        .get_mut()
                        .poll_write(cx, write_generation, b"old", |_| write.take().unwrap())
                        .is_pending()
                );
                Poll::Ready(())
            })
            .await;
            client
                .shutdown(Shutdown::Read)
                .await
                .expect("read shutdown");
            assert_eq!(
                poll_fn(|cx| client.write_state.get_mut().poll_write(
                    cx,
                    write_generation,
                    b"old",
                    |_| panic!("read shutdown consumed pending write")
                ))
                .await
                .expect("original write completion"),
                3
            );

            let mut before_write_shutdown = Some(Box::pin(PendingOnce::new(Ok(4))));
            let shutdown_generation = next_operation_id();
            poll_fn(|cx| {
                assert!(
                    client
                        .write_state
                        .get_mut()
                        .poll_write(cx, shutdown_generation, b"next", |_| {
                            before_write_shutdown.take().unwrap()
                        })
                        .is_pending()
                );
                Poll::Ready(())
            })
            .await;
            let (first, second) = crate::join!(
                client.shutdown(Shutdown::Write),
                client.shutdown(Shutdown::Write)
            );
            first.expect("first write shutdown");
            second.expect("second write shutdown");
            drop(peer);

            let listener = TcpListener::bind(SocketAddr::from(([127, 0, 0, 1], 0)))
                .await
                .expect("second listener should bind");
            let addr = listener.local_addr().expect("second listener address");
            let accepted = spawn(async move { listener.accept().await.expect("accept").0 });
            let client = TcpStream::connect(addr).await.expect("second connect");
            let peer = accepted.await.expect("second accept task");
            let mut discarded = [0; 4];
            let mut read = Some(Box::pin(PendingOnce::new(Ok(b"read".to_vec()))));
            poll_fn(|cx| {
                assert!(
                    client
                        .read_state
                        .borrow_mut()
                        .poll_slice(cx, &mut discarded, |_| read.take().unwrap())
                        .is_pending()
                );
                Poll::Ready(())
            })
            .await;
            client
                .shutdown(Shutdown::Write)
                .await
                .expect("write shutdown");
            let mut observed = [0; 4];
            assert_eq!(
                poll_fn(|cx| client.read_state.borrow_mut().poll_slice(
                    cx,
                    &mut observed,
                    |_| panic!("write shutdown consumed pending read")
                ))
                .await
                .expect("pending read completion"),
                4
            );
            assert_eq!(&observed, b"read");
            drop(peer);
            *passed_for_task.lock().unwrap() = true;
        });
    });
    run();

    assert!(*passed.lock().unwrap());
}
