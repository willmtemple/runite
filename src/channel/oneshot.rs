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
        #[cfg(test)]
        send_transition_gate: None,
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

impl<T: Send + 'static> std::fmt::Debug for Sender<T> {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.debug_struct("Sender").finish_non_exhaustive()
    }
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
    /// cancel-safe. The completion is only a readiness signal; the value stays
    /// in `State` until a receive operation consumes it.
    wait: Option<CompletionFuture<()>>,
}

impl<T: Send + 'static> std::fmt::Debug for Receiver<T> {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.debug_struct("Receiver").finish_non_exhaustive()
    }
}

struct State<T: Send + 'static> {
    value: Option<T>,
    sender_alive: bool,
    receiver_closed: bool,
    waiter: Option<CompletionHandle<()>>,
    #[cfg(test)]
    send_transition_gate: Option<crate::platform::runtime_shared::test_support::ExecutionGate>,
}

#[derive(Debug, Eq, PartialEq)]
/// Error returned when a oneshot send fails because the receiver is gone or closed.
pub struct SendError<T>(pub T);

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
/// Error returned when a oneshot receive observes a closed channel with no value.
pub struct RecvError;

#[derive(Debug, Eq, PartialEq)]
/// Non-blocking receive errors for [`Receiver::try_recv`].
#[non_exhaustive]
pub enum TryRecvError {
    /// No value has been sent yet, and the sender is still alive.
    Empty,
    /// The channel can never yield a value.
    Closed,
}

impl<T> std::fmt::Display for SendError<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("sending on a closed oneshot channel")
    }
}

impl<T: std::fmt::Debug> std::error::Error for SendError<T> {}

impl std::fmt::Display for RecvError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("receiving on a closed oneshot channel")
    }
}

impl std::error::Error for RecvError {}

impl std::fmt::Display for TryRecvError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            TryRecvError::Empty => f.write_str("oneshot channel is empty"),
            TryRecvError::Closed => f.write_str("oneshot channel is closed"),
        }
    }
}

impl std::error::Error for TryRecvError {}

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

        #[cfg(test)]
        let transition_gate;
        let waiter = {
            let mut state = shared.lock().expect("oneshot state should not be poisoned");
            state.sender_alive = false;
            if state.receiver_closed {
                return Err(SendError(value));
            }

            state.value = Some(value);
            #[cfg(test)]
            {
                transition_gate = state.send_transition_gate.take();
            }
            state.waiter.take()
        };

        #[cfg(test)]
        if let Some(gate) = transition_gate {
            gate.arrive_and_wait();
        }

        if let Some(waiter) = waiter {
            waiter.complete(());
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
    /// This method is cancel-safe. The receive completion lives on the receiver
    /// and only signals readiness; the value remains in the channel state until
    /// `recv` or [`try_recv`](Self::try_recv) consumes it.
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
        // Route through the receiver's persistent readiness slot so abandoning
        // this method cannot discard a value stored in the channel state.
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

        let result = {
            let mut state = self
                .shared
                .lock()
                .expect("oneshot state should not be poisoned");
            if let Some(value) = state.value.take() {
                Ok(value)
            } else if state.receiver_closed || !state.sender_alive {
                Err(TryRecvError::Closed)
            } else {
                return Err(TryRecvError::Empty);
            }
        };
        self.wait.take();
        self.consumed = true;
        result
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
        let waiter = {
            let mut state = self
                .shared
                .lock()
                .expect("oneshot state should not be poisoned");
            state.receiver_closed = true;
            state.waiter.take()
        };
        if let Some(waiter) = waiter {
            waiter.complete(());
        }
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
        wait: &mut Option<CompletionFuture<()>>,
    ) -> Poll<Result<T, RecvError>> {
        if *consumed {
            return Poll::Ready(Err(RecvError));
        }

        if let Some(future) = wait.as_mut() {
            match Pin::new(future).poll(cx) {
                Poll::Ready(()) => {
                    wait.take();
                }
                Poll::Pending => return Poll::Pending,
            }
        }

        {
            let mut state = shared.lock().expect("oneshot state should not be poisoned");
            if let Some(value) = state.value.take() {
                *consumed = true;
                return Poll::Ready(Ok(value));
            }
            if state.receiver_closed || !state.sender_alive {
                *consumed = true;
                return Poll::Ready(Err(RecvError));
            }
        }

        let (future, handle) = runtime_waiter::<()>();
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

        let immediate = {
            let mut state = shared.lock().expect("oneshot state should not be poisoned");
            if state.value.is_some() || state.receiver_closed || !state.sender_alive {
                true
            } else {
                assert!(
                    state.waiter.is_none(),
                    "only one oneshot receive operation may wait at a time"
                );
                state.waiter = Some(handle.clone());
                false
            }
        };

        if immediate {
            handle.complete(());
        }

        *wait = Some(future);
        Self::poll_recv(shared, consumed, cx, wait)
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
            waiter.complete(());
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
    use std::time::Duration;

    use crate::platform::runtime_shared::test_support::{ExecutionGate, TrackedThread};
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

    /// A value sent after a `recv` future is abandoned remains in the channel
    /// state and is returned by the next `recv`.
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
    fn send_publishes_value_and_closed_state_atomically() {
        let (sender, mut receiver) = channel();
        let gate = ExecutionGate::default();
        receiver.shared.lock().unwrap().send_transition_gate = Some(gate.clone());

        let sender_thread = TrackedThread::new(std::thread::spawn(move || sender.send(17)));
        let release = gate.release_on_drop();
        assert!(
            gate.wait_until_arrived(Duration::from_secs(5)),
            "sender should reach its publication gate"
        );

        assert_eq!(receiver.try_recv(), Ok(17));

        release.release();
        assert_eq!(sender_thread.join().unwrap(), Ok(()));
    }

    #[test]
    fn abandoned_recv_observes_channel_visible_send_and_close() {
        use std::future::Future;
        use std::task::{Context, Poll, Waker};

        type Observation = (
            bool,
            Result<(), super::SendError<i32>>,
            Result<i32, TryRecvError>,
            bool,
            Result<(), super::SendError<i32>>,
            Poll<Result<i32, super::RecvError>>,
            bool,
            Result<(), super::SendError<i32>>,
            Poll<Result<i32, super::RecvError>>,
        );

        let observed = Arc::new(Mutex::new(None::<Observation>));
        let observed_for_task = Arc::clone(&observed);
        queue_macrotask(move || {
            let mut cx = Context::from_waker(Waker::noop());

            let (sender, mut receiver) = channel();
            let first_pending = {
                let mut recv = std::pin::pin!(receiver.recv());
                recv.as_mut().poll(&mut cx).is_pending()
            };
            let first_send = sender.send(1);
            let first_received = receiver.try_recv();

            let (sender, mut receiver) = channel();
            let close_pending = {
                let mut recv = std::pin::pin!(receiver.recv());
                recv.as_mut().poll(&mut cx).is_pending()
            };
            receiver.close();
            let close_send = sender.send(2);
            let mut recv = std::pin::pin!(receiver.recv());
            let close_received = recv.as_mut().poll(&mut cx);

            let (sender, mut receiver) = channel();
            let sent_close_pending = {
                let mut recv = std::pin::pin!(receiver.recv());
                recv.as_mut().poll(&mut cx).is_pending()
            };
            let sent_close_send = sender.send(3);
            receiver.close();
            let mut recv = std::pin::pin!(receiver.recv());
            let sent_close_received = recv.as_mut().poll(&mut cx);

            *observed_for_task.lock().unwrap() = Some((
                first_pending,
                first_send,
                first_received,
                close_pending,
                close_send,
                close_received,
                sent_close_pending,
                sent_close_send,
                sent_close_received,
            ));
        });

        run();
        assert_eq!(
            observed.lock().unwrap().take(),
            Some((
                true,
                Ok(()),
                Ok(1),
                true,
                Err(super::SendError(2)),
                Poll::Ready(Err(super::RecvError)),
                true,
                Ok(()),
                Poll::Ready(Ok(3)),
            ))
        );
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
