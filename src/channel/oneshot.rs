//! Single-use channels for handing one value from a sender to a receiver.

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
        },
    )
}

/// Sending half of a oneshot channel.
pub struct Sender<T: Send + 'static> {
    shared: Option<Arc<Mutex<State<T>>>>,
}

/// Receiving half of a oneshot channel.
pub struct Receiver<T: Send + 'static> {
    shared: Arc<Mutex<State<T>>>,
    consumed: bool,
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
    /// This consumes the sender. If the receiver is already waiting on a runtime thread, the
    /// receive future is woken through the runtime's cross-thread notifier path.
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
    /// # Panics
    ///
    /// Panics if this future is first polled outside a runtime-managed thread. Async channel
    /// waiting registers with the current runtime thread so it can be woken through the driver's
    /// notification path.
    pub async fn recv(&mut self) -> Result<T, RecvError> {
        let mut wait = None;
        poll_fn(|cx| self.poll_recv(cx, &mut wait)).await
    }

    /// Attempts to receive the value without waiting.
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
    pub fn close(&mut self) {
        let mut state = self
            .shared
            .lock()
            .expect("oneshot state should not be poisoned");
        state.receiver_closed = true;
    }

    /// Returns `true` if the channel is closed to future sends.
    pub fn is_closed(&self) -> bool {
        let state = self
            .shared
            .lock()
            .expect("oneshot state should not be poisoned");
        state.receiver_closed || !state.sender_alive
    }

    fn poll_recv(
        &mut self,
        cx: &mut Context<'_>,
        wait: &mut Option<CompletionFuture<Result<T, RecvError>>>,
    ) -> Poll<Result<T, RecvError>> {
        if self.consumed {
            return Poll::Ready(Err(RecvError));
        }

        if let Some(future) = wait.as_mut() {
            match Pin::new(future).poll(cx) {
                Poll::Ready(result) => {
                    wait.take();
                    self.consumed = true;
                    Poll::Ready(result)
                }
                Poll::Pending => Poll::Pending,
            }
        } else {
            let (future, handle) = runtime_waiter::<Result<T, RecvError>>();
            let cancel_shared = Arc::clone(&self.shared);
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
                let mut state = self
                    .shared
                    .lock()
                    .expect("oneshot state should not be poisoned");
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
            self.poll_recv(cx, wait)
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

    use crate::{queue_future, queue_task, run, spawn_worker};

    use super::{TryRecvError, channel};

    #[test]
    fn oneshot_cross_thread_round_trip() {
        let result = Arc::new(Mutex::new(None::<usize>));
        let result_for_task = Arc::clone(&result);

        queue_task(move || {
            let (sender, mut receiver) = channel();
            let result_for_task = Arc::clone(&result_for_task);

            let _worker = spawn_worker(
                move || {
                    queue_task(move || {
                        sender.send(42usize).expect("oneshot send should succeed");
                    });
                },
                || {},
            );

            queue_future(async move {
                let value = receiver.recv().await.expect("oneshot recv should succeed");
                *result_for_task.lock().unwrap() = Some(value);
            });
        });
        run();

        assert_eq!(*result.lock().unwrap(), Some(42));
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
