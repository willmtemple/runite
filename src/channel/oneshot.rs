//! Single-use channels for handing one value from a sender to a receiver.
//!
//! Use a oneshot channel when one task needs to complete a single request, reply
//! to another task, or transfer ownership of one value exactly once. The sender
//! is consumed by [`Sender::send`], and the receiver resolves to an error if the
//! sender is dropped before sending. Async receives register a waiter with the
//! current runite event loop; completing the channel wakes that owning loop by a
//! local microtask or a platform-specific remote wake as needed.
//!
//! # Examples
//!
//! ```
//! runite::spawn(async {
//!     let (sender, mut receiver) = runite::channel::oneshot::channel();
//!     sender.send("ready").unwrap();
//!     assert_eq!(receiver.recv().await.unwrap(), "ready");
//! });
//!
//! runite::run();
//! ```

use std::future::poll_fn;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};

use crate::op::completion::{CompletionFuture, CompletionHandle};
use crate::sys::current::channel::runtime_waiter;

/// Creates a single-use channel for transferring one value from a [`Sender`] to a [`Receiver`].
///
/// # Examples
///
/// ```
/// let (sender, mut receiver) = runite::channel::oneshot::channel::<usize>();
/// sender.send(7).unwrap();
/// assert_eq!(receiver.try_recv(), Ok(7));
/// ```
pub fn channel<T: Send + 'static>() -> (Sender<T>, Receiver<T>) {
    let shared = Arc::new(Mutex::new(State {
        value: None,
        sender_alive: true,
        receiver_closed: false,
        waiter: None,
    }));
    (
        Sender {
            shared: Some(Arc::clone(&shared)),
        },
        Receiver {
            shared,
            consumed: false,
            wait: None,
        },
    )
}

/// Sending half of a oneshot channel.
///
/// A sender can either send one value with [`send`](Self::send) or be dropped to
/// close the channel without a value.
pub struct Sender<T: Send + 'static> {
    shared: Option<Arc<Mutex<State<T>>>>,
}

/// Receiving half of a oneshot channel.
///
/// A receiver can wait asynchronously with [`recv`](Self::recv) or poll
/// synchronously with [`try_recv`](Self::try_recv).
pub struct Receiver<T: Send + 'static> {
    shared: Arc<Mutex<State<T>>>,
    consumed: bool,
    /// Persistent wait slot shared across `recv` calls. Keeping the completion
    /// on the receiver (rather than in each `recv` future) makes `recv`
    /// cancel-safe: a value delivered to a `recv` future that is dropped before
    /// being polled ready is retained here and returned by the next `recv`.
    wait: Option<CompletionFuture<Result<T, RecvError>>>,
}

struct State<T: Send + 'static> {
    value: Option<T>,
    sender_alive: bool,
    receiver_closed: bool,
    waiter: Option<CompletionHandle<Result<T, RecvError>>>,
}

#[derive(Debug, Eq, PartialEq)]
/// Error returned when a oneshot send fails because the receiver is gone or closed.
pub struct SendError<T>(pub T);

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
/// Error returned when a oneshot receive observes a closed channel with no value.
pub struct RecvError;

#[derive(Debug, Eq, PartialEq)]
/// Non-blocking receive errors for [`Receiver::try_recv`].
pub enum TryRecvError {
    /// No value has been sent yet, and the sender is still alive.
    Empty,
    /// The channel can never yield a value.
    Closed,
}

impl<T: Send + 'static> Sender<T> {
    /// Sends `value` into the channel.
    ///
    /// This consumes the sender. If the receiver is already waiting, `send`
    /// completes that registered runtime waiter. The wake is a local microtask
    /// when `send` runs on the receiver's runtime thread, or a platform-specific
    /// remote wake when it runs from another thread.
    ///
    /// # Examples
    ///
    /// ```
    /// runite::spawn(async {
    ///     let (sender, mut receiver) = runite::channel::oneshot::channel();
    ///     sender.send(7).unwrap();
    ///     assert_eq!(receiver.recv().await.unwrap(), 7);
    /// });
    ///
    /// runite::run();
    /// ```
    pub fn send(mut self, value: T) -> Result<(), SendError<T>> {
        let Some(shared) = self.shared.take() else {
            return Err(SendError(value));
        };

        let waiter = {
            let mut state = shared.lock().expect("oneshot state should not be poisoned");
            state.sender_alive = false;
            if state.receiver_closed {
                return Err(SendError(value));
            }

            state.waiter.take()
        };

        if let Some(waiter) = waiter {
            waiter.complete(Ok(value));
        } else {
            shared
                .lock()
                .expect("oneshot state should not be poisoned")
                .value = Some(value);
        }

        Ok(())
    }

    /// Returns `true` if the receiver has been closed or dropped.
    ///
    /// # Examples
    ///
    /// ```
    /// let (sender, mut receiver) = runite::channel::oneshot::channel::<usize>();
    /// assert!(!sender.is_closed());
    /// receiver.close();
    /// assert!(sender.is_closed());
    /// ```
    pub fn is_closed(&self) -> bool {
        self.shared.as_ref().is_none_or(|shared| {
            shared
                .lock()
                .expect("oneshot state should not be poisoned")
                .receiver_closed
        })
    }
}

impl<T: Send + 'static> Receiver<T> {
    /// Waits for the channel's value.
    ///
    /// # Cancel safety
    ///
    /// This method is cancel-safe. The receive completion lives on the receiver,
    /// so a value the sender delivered to a `recv` future that is dropped before
    /// being polled ready is retained and returned by the next `recv` rather than
    /// lost.
    ///
    /// # Examples
    ///
    /// ```
    /// runite::spawn(async {
    ///     let (sender, mut receiver) = runite::channel::oneshot::channel();
    ///     sender.send("done").unwrap();
    ///     assert_eq!(receiver.recv().await.unwrap(), "done");
    /// });
    ///
    /// runite::run();
    /// ```
    ///
    /// # Panics
    ///
    /// Panics if this future is first polled outside a runtime-managed thread.
    /// Async channel waiting registers with the current runtime thread so it can
    /// be woken by a local microtask or the platform-specific remote wake path.
    pub async fn recv(&mut self) -> Result<T, RecvError> {
        // Route through the receiver's persistent wait slot so a delivered value
        // survives a cancelled `recv` future. `consumed` and `wait` are disjoint
        // fields, borrowed independently of the cloned `shared` handle.
        let shared = Arc::clone(&self.shared);
        let consumed = &mut self.consumed;
        let wait = &mut self.wait;
        poll_fn(move |cx| Self::poll_recv(&shared, consumed, cx, wait)).await
    }

    /// Attempts to receive the value without waiting.
    ///
    /// # Examples
    ///
    /// ```
    /// use runite::channel::oneshot::{self, TryRecvError};
    ///
    /// let (sender, mut receiver) = oneshot::channel();
    /// assert_eq!(receiver.try_recv(), Err(TryRecvError::Empty));
    /// sender.send(3).unwrap();
    /// assert_eq!(receiver.try_recv(), Ok(3));
    /// ```
    pub fn try_recv(&mut self) -> Result<T, TryRecvError> {
        if self.consumed {
            return Err(TryRecvError::Closed);
        }

        let mut state = self
            .shared
            .lock()
            .expect("oneshot state should not be poisoned");
        if let Some(value) = state.value.take() {
            self.consumed = true;
            return Ok(value);
        }

        if state.receiver_closed || !state.sender_alive {
            self.consumed = true;
            Err(TryRecvError::Closed)
        } else {
            Err(TryRecvError::Empty)
        }
    }

    /// Closes the receiver.
    ///
    /// Closing prevents future sends from succeeding. If a value has already been sent, it can
    /// still be retrieved.
    ///
    /// # Examples
    ///
    /// ```
    /// use runite::channel::oneshot::{self, SendError};
    ///
    /// let (sender, mut receiver) = oneshot::channel();
    /// receiver.close();
    /// assert_eq!(sender.send(9), Err(SendError(9)));
    /// ```
    pub fn close(&mut self) {
        let mut state = self
            .shared
            .lock()
            .expect("oneshot state should not be poisoned");
        state.receiver_closed = true;
    }

    /// Returns `true` if the channel is closed to future sends.
    ///
    /// # Examples
    ///
    /// ```
    /// let (sender, receiver) = runite::channel::oneshot::channel::<usize>();
    /// assert!(!receiver.is_closed());
    /// drop(sender);
    /// assert!(receiver.is_closed());
    /// ```
    pub fn is_closed(&self) -> bool {
        let state = self
            .shared
            .lock()
            .expect("oneshot state should not be poisoned");
        state.receiver_closed || !state.sender_alive
    }

    fn poll_recv(
        shared: &Arc<Mutex<State<T>>>,
        consumed: &mut bool,
        cx: &mut Context<'_>,
        wait: &mut Option<CompletionFuture<Result<T, RecvError>>>,
    ) -> Poll<Result<T, RecvError>> {
        if *consumed {
            return Poll::Ready(Err(RecvError));
        }

        if let Some(future) = wait.as_mut() {
            match Pin::new(future).poll(cx) {
                Poll::Ready(result) => {
                    wait.take();
                    *consumed = true;
                    Poll::Ready(result)
                }
                Poll::Pending => Poll::Pending,
            }
        } else {
            let (future, handle) = runtime_waiter::<Result<T, RecvError>>();
            let cancel_shared = Arc::clone(shared);
            let cancel_handle = handle.clone();
            handle.set_cancel(move || {
                let mut state = cancel_shared
                    .lock()
                    .expect("oneshot state should not be poisoned");
                let _ = state.waiter.take();
                drop(state);
                cancel_handle.finish(None);
            });

            let mut immediate = None;
            {
                let mut state = shared.lock().expect("oneshot state should not be poisoned");
                if let Some(value) = state.value.take() {
                    immediate = Some(Ok(value));
                } else if state.receiver_closed || !state.sender_alive {
                    immediate = Some(Err(RecvError));
                } else {
                    assert!(
                        state.waiter.is_none(),
                        "only one oneshot receive operation may wait at a time"
                    );
                    state.waiter = Some(handle.clone());
                }
            }

            if let Some(result) = immediate {
                handle.complete(result);
            }

            *wait = Some(future);
            Self::poll_recv(shared, consumed, cx, wait)
        }
    }
}

impl<T: Send + 'static> Drop for Sender<T> {
    fn drop(&mut self) {
        let Some(shared) = self.shared.take() else {
            return;
        };

        let waiter = {
            let mut state = shared.lock().expect("oneshot state should not be poisoned");
            if !state.sender_alive {
                return;
            }

            state.sender_alive = false;
            if state.value.is_none() {
                state.waiter.take()
            } else {
                None
            }
        };

        if let Some(waiter) = waiter {
            waiter.complete(Err(RecvError));
        }
    }
}

impl<T: Send + 'static> Drop for Receiver<T> {
    fn drop(&mut self) {
        let mut state = self
            .shared
            .lock()
            .expect("oneshot state should not be poisoned");
        state.receiver_closed = true;
        let _ = state.waiter.take();
    }
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Mutex};

    use crate::{queue_macrotask, run, spawn, spawn_worker};

    use super::{TryRecvError, channel};

    #[test]
    fn oneshot_cross_thread_round_trip() {
        let result = Arc::new(Mutex::new(None::<usize>));
        let result_for_task = Arc::clone(&result);

        queue_macrotask(move || {
            let (sender, mut receiver) = channel();
            let result_for_task = Arc::clone(&result_for_task);

            let _worker = spawn_worker(
                move || {
                    queue_macrotask(move || {
                        sender.send(42usize).expect("oneshot send should succeed");
                    });
                },
                || {},
            );

            spawn(async move {
                let value = receiver.recv().await.expect("oneshot recv should succeed");
                *result_for_task.lock().unwrap() = Some(value);
            });
        });
        run();

        assert_eq!(*result.lock().unwrap(), Some(42));
    }

    /// A value delivered to a `recv` future that is dropped before being polled
    /// ready must be retained on the receiver and returned by the next `recv`.
    #[test]
    fn recv_is_cancel_safe() {
        use std::future::Future;
        use std::task::{Context, Waker};

        let observed = Arc::new(Mutex::new(None::<Result<u32, super::RecvError>>));
        let observed_for_task = Arc::clone(&observed);

        queue_macrotask(move || {
            let (sender, mut receiver) = channel::<u32>();

            // Register a recv waiter, deliver the value, then abandon the recv
            // future without polling it ready.
            {
                let mut cx = Context::from_waker(Waker::noop());
                let mut fut = std::pin::pin!(receiver.recv());
                assert!(fut.as_mut().poll(&mut cx).is_pending());
                sender.send(1).expect("receiver is alive");
            }

            spawn(async move {
                *observed_for_task.lock().unwrap() = Some(receiver.recv().await);
            });
        });

        run();

        assert_eq!(*observed.lock().unwrap(), Some(Ok(1)));
    }

    #[test]
    fn oneshot_try_recv_and_close() {
        let (sender, mut receiver) = channel::<usize>();
        assert_eq!(receiver.try_recv(), Err(TryRecvError::Empty));
        receiver.close();
        assert!(
            sender.send(7).is_err(),
            "closed receiver should reject send"
        );
        assert_eq!(receiver.try_recv(), Err(TryRecvError::Closed));
    }
}
