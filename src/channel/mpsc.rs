//! Multi-producer, single-consumer channels.
//!
//! # Examples
//!
//! Send from one task and receive from another. Once every sender is dropped,
//! [`Receiver::recv`] returns `None`.
//!
//! ```
//! let (sender, mut receiver) = runite::channel::mpsc::channel(2);
//!
//! runite::queue_future(async move {
//!     sender.send("hello").await.unwrap();
//! });
//!
//! runite::queue_future(async move {
//!     assert_eq!(receiver.recv().await, Some("hello"));
//!     assert_eq!(receiver.recv().await, None);
//! });
//!
//! runite::run();
//! ```

use std::collections::VecDeque;
use std::future::poll_fn;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};

use crate::io::Stream;
use crate::op::completion::{CompletionFuture, CompletionHandle};
use crate::sys::current::channel::runtime_waiter;

/// Creates a bounded channel with room for at most `capacity` queued messages.
///
/// Bounded senders provide both [`Sender::try_send`] and async [`Sender::send`] backpressure.
///
/// # Panics
///
/// Panics if `capacity == 0`.
pub fn channel<T: Send + 'static>(capacity: usize) -> (Sender<T>, Receiver<T>) {
    assert!(capacity > 0, "bounded channels require capacity > 0");
    let shared = Arc::new(Mutex::new(State::new(Some(capacity))));
    (
        Sender {
            shared: Arc::clone(&shared),
        },
        Receiver {
            shared,
            stream_wait: None,
        },
    )
}

/// Creates an unbounded channel.
///
/// Unbounded senders never wait for capacity, but the single receiver is still asynchronous.
pub fn unbounded_channel<T: Send + 'static>() -> (UnboundedSender<T>, Receiver<T>) {
    let shared = Arc::new(Mutex::new(State::new(None)));
    (
        UnboundedSender {
            shared: Arc::clone(&shared),
        },
        Receiver {
            shared,
            stream_wait: None,
        },
    )
}

/// Bounded multi-producer sender.
pub struct Sender<T: Send + 'static> {
    shared: Arc<Mutex<State<T>>>,
}

/// Unbounded multi-producer sender.
pub struct UnboundedSender<T: Send + 'static> {
    shared: Arc<Mutex<State<T>>>,
}

/// Single consumer for both bounded and unbounded MPSC channels.
pub struct Receiver<T: Send + 'static> {
    shared: Arc<Mutex<State<T>>>,
    stream_wait: Option<CompletionFuture<Option<T>>>,
}

struct State<T: Send + 'static> {
    queue: VecDeque<T>,
    capacity: Option<usize>,
    sender_count: usize,
    receiver_closed: bool,
    recv_waiter: Option<CompletionHandle<Option<T>>>,
    send_waiters: VecDeque<SendWaiter<T>>,
    next_waiter_id: usize,
}

struct SendWaiter<T: Send + 'static> {
    id: usize,
    value: T,
    handle: CompletionHandle<Result<(), SendError<T>>>,
}

#[derive(Debug, Eq, PartialEq)]
/// Error returned when sending fails because the receiver has been closed or dropped.
pub struct SendError<T>(pub T);

#[derive(Debug, Eq, PartialEq)]
/// Error returned by [`Sender::try_send`] when a message cannot be queued immediately.
pub enum TrySendError<T> {
    /// The bounded queue is currently full.
    Full(T),
    /// The receiver has been closed or dropped.
    Closed(T),
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
/// Error returned by [`Receiver::try_recv`] when no message is available immediately.
pub enum TryRecvError {
    /// The channel is still open, but currently empty.
    Empty,
    /// The channel is closed and no more messages can arrive.
    Disconnected,
}

/// A wakeup deferred until the channel mutex has been released.
///
/// Waking a waiter while holding the channel lock can be expensive (cross-thread
/// wakeups go through the io_uring ring notification path) and risks priority
/// inversion.  All `State` methods collect these instead of calling
/// `CompletionHandle::complete` directly; the caller fires them after dropping
/// the `MutexGuard`.
enum PendingCompletion<T: Send + 'static> {
    RecvSome(CompletionHandle<Option<T>>, T),
    RecvNone(CompletionHandle<Option<T>>),
    SendOk(CompletionHandle<Result<(), SendError<T>>>),
    SendErr(CompletionHandle<Result<(), SendError<T>>>, T),
}

fn fire_completions<T: Send + 'static>(completions: Vec<PendingCompletion<T>>) {
    for c in completions {
        match c {
            PendingCompletion::RecvSome(h, v) => h.complete(Some(v)),
            PendingCompletion::RecvNone(h) => h.complete(None),
            PendingCompletion::SendOk(h) => h.complete(Ok(())),
            PendingCompletion::SendErr(h, v) => h.complete(Err(SendError(v))),
        }
    }
}

impl<T: Send + 'static> State<T> {
    fn new(capacity: Option<usize>) -> Self {
        Self {
            queue: VecDeque::new(),
            capacity,
            sender_count: 1,
            receiver_closed: false,
            recv_waiter: None,
            send_waiters: VecDeque::new(),
            next_waiter_id: 1,
        }
    }

    fn try_send_now(
        &mut self,
        value: T,
        completions: &mut Vec<PendingCompletion<T>>,
    ) -> Result<(), TrySendError<T>> {
        if self.receiver_closed {
            return Err(TrySendError::Closed(value));
        }

        if let Some(waiter) = self.recv_waiter.take() {
            completions.push(PendingCompletion::RecvSome(waiter, value));
            return Ok(());
        }

        if self
            .capacity
            .is_some_and(|capacity| self.queue.len() >= capacity)
        {
            return Err(TrySendError::Full(value));
        }

        self.queue.push_back(value);
        Ok(())
    }

    fn enqueue_send_waiter(
        &mut self,
        value: T,
        handle: CompletionHandle<Result<(), SendError<T>>>,
    ) -> usize {
        let id = self.next_waiter_id;
        self.next_waiter_id = self.next_waiter_id.wrapping_add(1);
        self.send_waiters
            .push_back(SendWaiter { id, value, handle });
        id
    }

    fn remove_send_waiter(&mut self, waiter_id: usize) -> bool {
        let Some(index) = self
            .send_waiters
            .iter()
            .position(|waiter| waiter.id == waiter_id)
        else {
            return false;
        };
        self.send_waiters.remove(index);
        true
    }

    fn pump_senders(&mut self, completions: &mut Vec<PendingCompletion<T>>) {
        loop {
            if self.receiver_closed {
                self.fail_pending_senders(completions);
                break;
            }

            let has_capacity = self
                .capacity
                .is_none_or(|capacity| self.queue.len() < capacity);
            if !has_capacity {
                break;
            }

            let Some(waiter) = self.send_waiters.pop_front() else {
                break;
            };

            if let Some(receiver) = self.recv_waiter.take() {
                completions.push(PendingCompletion::RecvSome(receiver, waiter.value));
            } else {
                self.queue.push_back(waiter.value);
            }
            completions.push(PendingCompletion::SendOk(waiter.handle));
        }

        if self.queue.is_empty()
            && self.sender_count == 0
            && let Some(waiter) = self.recv_waiter.take()
        {
            completions.push(PendingCompletion::RecvNone(waiter));
        }
    }

    fn fail_pending_senders(&mut self, completions: &mut Vec<PendingCompletion<T>>) {
        while let Some(waiter) = self.send_waiters.pop_front() {
            completions.push(PendingCompletion::SendErr(waiter.handle, waiter.value));
        }
    }

    fn close_receiver(&mut self, completions: &mut Vec<PendingCompletion<T>>) {
        self.receiver_closed = true;
        self.fail_pending_senders(completions);
        if self.queue.is_empty()
            && let Some(waiter) = self.recv_waiter.take()
        {
            completions.push(PendingCompletion::RecvNone(waiter));
        }
    }

    fn drop_sender(&mut self, completions: &mut Vec<PendingCompletion<T>>) {
        self.sender_count = self
            .sender_count
            .checked_sub(1)
            .expect("sender count underflow: more drops than creates");
        if self.sender_count == 0
            && self.queue.is_empty()
            && let Some(waiter) = self.recv_waiter.take()
        {
            completions.push(PendingCompletion::RecvNone(waiter));
        }
    }
}

impl<T: Send + 'static> Clone for Sender<T> {
    fn clone(&self) -> Self {
        self.shared
            .lock()
            .expect("mpsc state should not be poisoned")
            .sender_count += 1;
        Self {
            shared: Arc::clone(&self.shared),
        }
    }
}

impl<T: Send + 'static> Clone for UnboundedSender<T> {
    fn clone(&self) -> Self {
        self.shared
            .lock()
            .expect("mpsc state should not be poisoned")
            .sender_count += 1;
        Self {
            shared: Arc::clone(&self.shared),
        }
    }
}

impl<T: Send + 'static> Sender<T> {
    /// Waits until the message can be queued.
    ///
    /// When the bounded channel is full, this future waits until the receiver frees capacity.
    ///
    /// # Panics
    ///
    /// Panics if this future is first polled outside a runtime-managed thread.
    pub async fn send(&self, value: T) -> Result<(), SendError<T>> {
        let mut value = Some(value);
        let mut wait = None;
        poll_fn(|cx| self.poll_send(cx, &mut value, &mut wait)).await
    }

    /// Attempts to queue a message immediately.
    pub fn try_send(&self, value: T) -> Result<(), TrySendError<T>> {
        let mut completions = Vec::new();
        let result = {
            let mut state = self
                .shared
                .lock()
                .expect("mpsc state should not be poisoned");
            state.try_send_now(value, &mut completions)
        };
        fire_completions(completions);
        result
    }

    /// Returns `true` if the receiver has been closed or dropped.
    pub fn is_closed(&self) -> bool {
        self.shared
            .lock()
            .expect("mpsc state should not be poisoned")
            .receiver_closed
    }

    fn poll_send(
        &self,
        cx: &mut Context<'_>,
        value_slot: &mut Option<T>,
        wait: &mut Option<CompletionFuture<Result<(), SendError<T>>>>,
    ) -> Poll<Result<(), SendError<T>>> {
        if let Some(future) = wait.as_mut() {
            match Pin::new(future).poll(cx) {
                Poll::Ready(result) => {
                    wait.take();
                    Poll::Ready(result)
                }
                Poll::Pending => Poll::Pending,
            }
        } else {
            let mut completions = Vec::new();
            let first_result = {
                let mut state = self
                    .shared
                    .lock()
                    .expect("mpsc state should not be poisoned");
                state.try_send_now(
                    value_slot.take().expect("send value should be present"),
                    &mut completions,
                )
            };
            fire_completions(completions);
            match first_result {
                Ok(()) => Poll::Ready(Ok(())),
                Err(TrySendError::Closed(value)) => Poll::Ready(Err(SendError(value))),
                Err(TrySendError::Full(returned)) => {
                    let (future, handle) = runtime_waiter::<Result<(), SendError<T>>>();
                    let state_shared = Arc::clone(&self.shared);
                    let mut completions = Vec::new();
                    let registration = {
                        let mut state = state_shared
                            .lock()
                            .expect("mpsc state should not be poisoned");
                        match state.try_send_now(returned, &mut completions) {
                            Ok(()) => Ok(None),
                            Err(TrySendError::Closed(value)) => Err(SendError(value)),
                            Err(TrySendError::Full(value)) => {
                                Ok(Some(state.enqueue_send_waiter(value, handle.clone())))
                            }
                        }
                    };
                    fire_completions(completions);
                    match registration {
                        Ok(None) => {
                            handle.complete(Ok(()));
                            *wait = Some(future);
                            self.poll_send(cx, value_slot, wait)
                        }
                        Err(error) => {
                            handle.complete(Err(error));
                            *wait = Some(future);
                            self.poll_send(cx, value_slot, wait)
                        }
                        Ok(Some(waiter_id)) => {
                            let cancel_shared = Arc::clone(&self.shared);
                            let cancel_handle = handle.clone();
                            handle.set_cancel(move || {
                                let mut state = cancel_shared
                                    .lock()
                                    .expect("mpsc state should not be poisoned");
                                let _ = state.remove_send_waiter(waiter_id);
                                drop(state);
                                cancel_handle.finish(None);
                            });
                            *wait = Some(future);
                            self.poll_send(cx, value_slot, wait)
                        }
                    }
                }
            }
        }
    }
}

impl<T: Send + 'static> UnboundedSender<T> {
    /// Queues a message immediately.
    pub fn send(&self, value: T) -> Result<(), SendError<T>> {
        let mut completions = Vec::new();
        let result = {
            let mut state = self
                .shared
                .lock()
                .expect("mpsc state should not be poisoned");
            state.try_send_now(value, &mut completions)
        };
        fire_completions(completions);
        result.map_err(|error| match error {
            TrySendError::Full(value) | TrySendError::Closed(value) => SendError(value),
        })
    }

    /// Returns `true` if the receiver has been closed or dropped.
    pub fn is_closed(&self) -> bool {
        self.shared
            .lock()
            .expect("mpsc state should not be poisoned")
            .receiver_closed
    }
}

impl<T: Send + 'static> Receiver<T> {
    /// Waits for the next message.
    ///
    /// Returns `None` when the channel is closed and all buffered messages have been drained.
    ///
    /// # Panics
    ///
    /// Panics if this future is first polled outside a runtime-managed thread.
    pub async fn recv(&mut self) -> Option<T> {
        let mut wait = None;
        let shared = Arc::clone(&self.shared);
        poll_fn(|cx| Self::poll_recv(&shared, cx, &mut wait)).await
    }

    /// Attempts to receive a message immediately.
    pub fn try_recv(&mut self) -> Result<T, TryRecvError> {
        let mut completions = Vec::new();
        let result = {
            let mut state = self
                .shared
                .lock()
                .expect("mpsc state should not be poisoned");
            if let Some(value) = state.queue.pop_front() {
                state.pump_senders(&mut completions);
                Ok(value)
            } else if state.sender_count == 0 || state.receiver_closed {
                Err(TryRecvError::Disconnected)
            } else {
                Err(TryRecvError::Empty)
            }
        };
        fire_completions(completions);
        result
    }

    /// Closes the channel to future sends.
    ///
    /// Already-buffered messages remain available to [`recv`](Self::recv) and
    /// [`try_recv`](Self::try_recv).
    pub fn close(&mut self) {
        let mut completions = Vec::new();
        {
            let mut state = self
                .shared
                .lock()
                .expect("mpsc state should not be poisoned");
            state.close_receiver(&mut completions);
        }
        fire_completions(completions);
    }

    /// Returns `true` if the channel is closed or all senders have been dropped.
    pub fn is_closed(&self) -> bool {
        let state = self
            .shared
            .lock()
            .expect("mpsc state should not be poisoned");
        state.receiver_closed || state.sender_count == 0
    }

    fn poll_recv(
        shared: &Arc<Mutex<State<T>>>,
        cx: &mut Context<'_>,
        wait: &mut Option<CompletionFuture<Option<T>>>,
    ) -> Poll<Option<T>> {
        if let Some(future) = wait.as_mut() {
            match Pin::new(future).poll(cx) {
                Poll::Ready(result) => {
                    wait.take();
                    Poll::Ready(result)
                }
                Poll::Pending => Poll::Pending,
            }
        } else {
            let (future, handle) = runtime_waiter::<Option<T>>();
            let cancel_shared = Arc::clone(shared);
            let cancel_handle = handle.clone();
            handle.set_cancel(move || {
                let mut state = cancel_shared
                    .lock()
                    .expect("mpsc state should not be poisoned");
                let _ = state.recv_waiter.take();
                drop(state);
                cancel_handle.finish(None);
            });

            let mut completions = Vec::new();
            {
                let mut state = shared.lock().expect("mpsc state should not be poisoned");
                if let Some(value) = state.queue.pop_front() {
                    state.pump_senders(&mut completions);
                    completions.push(PendingCompletion::RecvSome(handle.clone(), value));
                } else if state.receiver_closed || state.sender_count == 0 {
                    completions.push(PendingCompletion::RecvNone(handle.clone()));
                } else {
                    assert!(
                        state.recv_waiter.is_none(),
                        "only one mpsc receive operation may wait at a time"
                    );
                    state.recv_waiter = Some(handle.clone());
                }
            }
            fire_completions(completions);

            *wait = Some(future);
            Self::poll_recv(shared, cx, wait)
        }
    }
}

impl<T: Send + 'static> Stream for Receiver<T> {
    type Item = T;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();
        Self::poll_recv(&this.shared, cx, &mut this.stream_wait)
    }
}

impl<T: Send + 'static> Drop for Sender<T> {
    fn drop(&mut self) {
        let mut completions = Vec::new();
        {
            let mut state = self
                .shared
                .lock()
                .expect("mpsc state should not be poisoned");
            state.drop_sender(&mut completions);
        }
        fire_completions(completions);
    }
}

impl<T: Send + 'static> Drop for UnboundedSender<T> {
    fn drop(&mut self) {
        let mut completions = Vec::new();
        {
            let mut state = self
                .shared
                .lock()
                .expect("mpsc state should not be poisoned");
            state.drop_sender(&mut completions);
        }
        fire_completions(completions);
    }
}

impl<T: Send + 'static> Drop for Receiver<T> {
    fn drop(&mut self) {
        let mut completions = Vec::new();
        {
            let mut state = self
                .shared
                .lock()
                .expect("mpsc state should not be poisoned");
            state.close_receiver(&mut completions);
        }
        fire_completions(completions);
    }
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Mutex};
    use std::time::Duration;

    use crate::io::StreamExt;
    use crate::time::sleep;
    use crate::{queue_future, queue_task, run, spawn_worker};

    use super::{TryRecvError, TrySendError, channel, unbounded_channel};

    #[test]
    fn mpsc_receiver_is_stream() {
        let observed = Arc::new(Mutex::new(None::<(Vec<i32>, Option<i32>)>));
        let observed_for_task = Arc::clone(&observed);

        queue_task(move || {
            queue_future(async move {
                let (sender, mut receiver) = channel(5);
                for value in 0..5 {
                    sender
                        .send(value)
                        .await
                        .expect("send should succeed while receiver is live");
                }
                drop(sender);

                let values = (&mut receiver).take(5).collect::<Vec<_>>().await;
                let end = receiver.next().await;
                *observed_for_task.lock().unwrap() = Some((values, end));
            });
        });

        run();

        assert_eq!(*observed.lock().unwrap(), Some((vec![0, 1, 2, 3, 4], None)));
    }

    #[test]
    fn bounded_channel_applies_backpressure() {
        let log = Arc::new(Mutex::new(Vec::<String>::new()));
        let log_for_task = Arc::clone(&log);

        queue_task(move || {
            let (sender, mut receiver) = channel(1);
            let log_for_sender = Arc::clone(&log_for_task);
            let log_for_receiver = Arc::clone(&log_for_task);

            queue_future(async move {
                sender
                    .send("first")
                    .await
                    .expect("first send should succeed");
                log_for_sender
                    .lock()
                    .unwrap()
                    .push("sent first".to_string());
                sender
                    .send("second")
                    .await
                    .expect("second send should succeed");
                log_for_sender
                    .lock()
                    .unwrap()
                    .push("sent second".to_string());
            });

            queue_future(async move {
                sleep(Duration::from_millis(5)).await;
                let first = receiver.recv().await.expect("first recv should succeed");
                log_for_receiver
                    .lock()
                    .unwrap()
                    .push(format!("received {first}"));
                let second = receiver.recv().await.expect("second recv should succeed");
                log_for_receiver
                    .lock()
                    .unwrap()
                    .push(format!("received {second}"));
            });
        });
        run();

        let log = log.lock().unwrap();
        let sent_first = log.iter().position(|entry| entry == "sent first").unwrap();
        let received_first = log
            .iter()
            .position(|entry| entry == "received first")
            .unwrap();
        let sent_second = log.iter().position(|entry| entry == "sent second").unwrap();
        let received_second = log
            .iter()
            .position(|entry| entry == "received second")
            .unwrap();

        assert!(
            sent_first < received_first,
            "first send should happen before first recv"
        );
        assert!(
            received_first < sent_second,
            "second send should not complete before capacity is freed"
        );
        assert!(
            received_first < received_second,
            "receiver should observe messages in FIFO order"
        );
    }

    #[test]
    fn unbounded_channel_moves_messages_across_worker_threads() {
        let log = Arc::new(Mutex::new(Vec::new()));
        let log_for_task = Arc::clone(&log);

        queue_task(move || {
            let (sender, mut receiver) = unbounded_channel::<String>();
            let worker_sender = sender.clone();
            let log_for_receiver = Arc::clone(&log_for_task);

            let _worker = spawn_worker(
                move || {
                    queue_task(move || {
                        worker_sender
                            .send("worker boot".into())
                            .expect("worker boot send should succeed");
                        worker_sender
                            .send("worker done".into())
                            .expect("worker done send should succeed");
                    });
                },
                || {},
            );
            drop(sender);

            queue_future(async move {
                while let Some(message) = receiver.recv().await {
                    log_for_receiver.lock().unwrap().push(message);
                }
            });
        });
        run();

        assert_eq!(
            log.lock().unwrap().as_slice(),
            ["worker boot", "worker done"]
        );
    }

    #[test]
    fn try_send_try_recv_and_close_semantics_work() {
        let (sender, mut receiver) = channel(1);
        sender
            .try_send(1usize)
            .expect("initial send should succeed");
        assert_eq!(sender.try_send(2usize), Err(TrySendError::Full(2)));
        assert_eq!(receiver.try_recv(), Ok(1));
        assert_eq!(receiver.try_recv(), Err(TryRecvError::Empty));
        receiver.close();
        assert!(sender.is_closed(), "sender should observe closed receiver");
        assert_eq!(sender.try_send(3usize), Err(TrySendError::Closed(3)));
        assert_eq!(receiver.try_recv(), Err(TryRecvError::Disconnected));
    }
}
