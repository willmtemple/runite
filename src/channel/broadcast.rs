//! Multi-producer, multi-consumer broadcast channels.

use std::collections::VecDeque;
use std::fmt;
use std::future::poll_fn;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};

use crate::op::completion::{CompletionFuture, CompletionHandle};
use crate::sys::current::channel::runtime_waiter;

/// Creates a bounded broadcast channel with room for `capacity` messages.
///
/// Each receiver observes every message sent after it subscribes. Slow receivers
/// report [`RecvError::Lagged`] when the ring buffer overwrites messages they
/// have not yet received.
///
/// # Panics
///
/// Panics if `capacity == 0`.
pub fn channel<T: Clone + Send + 'static>(capacity: usize) -> (Sender<T>, Receiver<T>) {
    assert!(capacity > 0, "broadcast channels require capacity > 0");
    let shared = Arc::new(Mutex::new(State::new(capacity)));
    (
        Sender {
            shared: Arc::clone(&shared),
        },
        Receiver {
            shared,
            next_seq: 0,
            wait: None,
        },
    )
}

/// Broadcast sending half.
pub struct Sender<T: Clone + Send + 'static> {
    shared: Arc<Mutex<State<T>>>,
}

/// Broadcast receiving half.
pub struct Receiver<T: Clone + Send + 'static> {
    shared: Arc<Mutex<State<T>>>,
    next_seq: u64,
    wait: Option<CompletionFuture<RecvOutcome<T>>>,
}

struct State<T: Clone + Send + 'static> {
    buffer: VecDeque<Slot<T>>,
    capacity: usize,
    next_seq: u64,
    sender_count: usize,
    receiver_count: usize,
    recv_waiters: Vec<RecvWaiter<T>>,
    next_waiter_id: usize,
}

struct Slot<T> {
    seq: u64,
    value: T,
}

struct RecvWaiter<T: Clone + Send + 'static> {
    id: usize,
    next_seq: u64,
    handle: CompletionHandle<RecvOutcome<T>>,
}

enum RecvOutcome<T> {
    Value(T, u64),
    Lagged(u64, u64),
    Closed,
}

#[derive(Debug, Eq, PartialEq)]
/// Error returned when sending fails because there are no receivers.
pub struct SendError<T>(pub T);

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
/// Error returned when receiving from a broadcast channel fails.
pub enum RecvError {
    /// The receiver missed this many messages.
    Lagged(u64),
    /// All senders have been dropped and all buffered messages have been read.
    Closed,
}

impl<T: Clone + Send + 'static> State<T> {
    fn new(capacity: usize) -> Self {
        Self {
            buffer: VecDeque::new(),
            capacity,
            next_seq: 0,
            sender_count: 1,
            receiver_count: 1,
            recv_waiters: Vec::new(),
            next_waiter_id: 1,
        }
    }

    fn oldest_seq(&self) -> u64 {
        self.buffer.front().map_or(self.next_seq, |slot| slot.seq)
    }

    fn recv_outcome(&self, next_seq: u64) -> Option<RecvOutcome<T>> {
        let oldest = self.oldest_seq();
        if next_seq < oldest {
            return Some(RecvOutcome::Lagged(oldest - next_seq, oldest));
        }

        if next_seq < self.next_seq {
            let index =
                usize::try_from(next_seq - oldest).expect("buffer index should fit into usize");
            let slot = self
                .buffer
                .get(index)
                .expect("sequence should be present in broadcast buffer");
            return Some(RecvOutcome::Value(slot.value.clone(), slot.seq + 1));
        }

        if self.sender_count == 0 {
            Some(RecvOutcome::Closed)
        } else {
            None
        }
    }

    fn push_value(&mut self, value: T) {
        if self.buffer.len() == self.capacity {
            let _ = self.buffer.pop_front();
        }
        self.buffer.push_back(Slot {
            seq: self.next_seq,
            value,
        });
        self.next_seq = self.next_seq.wrapping_add(1);
    }

    fn enqueue_waiter(&mut self, next_seq: u64, handle: CompletionHandle<RecvOutcome<T>>) -> usize {
        let id = self.next_waiter_id;
        self.next_waiter_id = self.next_waiter_id.wrapping_add(1);
        self.recv_waiters.push(RecvWaiter {
            id,
            next_seq,
            handle,
        });
        id
    }

    fn remove_waiter(&mut self, waiter_id: usize) {
        if let Some(index) = self
            .recv_waiters
            .iter()
            .position(|waiter| waiter.id == waiter_id)
        {
            self.recv_waiters.swap_remove(index);
        }
    }

    fn wake_ready_receivers(&mut self) -> Vec<(CompletionHandle<RecvOutcome<T>>, RecvOutcome<T>)> {
        let mut ready = Vec::new();
        let mut index = 0;
        while index < self.recv_waiters.len() {
            if let Some(outcome) = self.recv_outcome(self.recv_waiters[index].next_seq) {
                ready.push((self.recv_waiters.swap_remove(index).handle, outcome));
            } else {
                index += 1;
            }
        }
        ready
    }

    fn drop_sender(&mut self) -> Vec<(CompletionHandle<RecvOutcome<T>>, RecvOutcome<T>)> {
        self.sender_count = self
            .sender_count
            .checked_sub(1)
            .expect("sender count underflow: more drops than creates");
        if self.sender_count == 0 {
            self.wake_ready_receivers()
        } else {
            Vec::new()
        }
    }
}

impl<T: Clone + Send + 'static> Clone for Sender<T> {
    fn clone(&self) -> Self {
        self.shared
            .lock()
            .expect("broadcast state should not be poisoned")
            .sender_count += 1;
        Self {
            shared: Arc::clone(&self.shared),
        }
    }
}

impl<T: Clone + Send + 'static> Sender<T> {
    /// Sends a value to all active receivers.
    ///
    /// Returns the number of receivers that were active when the value was sent.
    pub fn send(&self, value: T) -> Result<usize, SendError<T>> {
        let (receiver_count, waiters) = {
            let mut state = self
                .shared
                .lock()
                .expect("broadcast state should not be poisoned");
            if state.receiver_count == 0 {
                return Err(SendError(value));
            }
            state.push_value(value);
            (state.receiver_count, state.wake_ready_receivers())
        };
        self.complete_waiters(waiters);
        Ok(receiver_count)
    }

    /// Creates a new receiver for values sent after this call.
    pub fn subscribe(&self) -> Receiver<T> {
        let next_seq = {
            let mut state = self
                .shared
                .lock()
                .expect("broadcast state should not be poisoned");
            state.receiver_count += 1;
            state.next_seq
        };
        Receiver {
            shared: Arc::clone(&self.shared),
            next_seq,
            wait: None,
        }
    }

    /// Returns the number of active receivers.
    pub fn receiver_count(&self) -> usize {
        self.shared
            .lock()
            .expect("broadcast state should not be poisoned")
            .receiver_count
    }

    fn complete_waiters(&self, waiters: Vec<(CompletionHandle<RecvOutcome<T>>, RecvOutcome<T>)>) {
        for (waiter, outcome) in waiters {
            waiter.complete(outcome);
        }
    }
}

impl<T: Clone + Send + 'static> Receiver<T> {
    /// Waits for the next value.
    ///
    /// # Panics
    ///
    /// Panics if this future is first polled outside a runtime-managed thread.
    pub async fn recv(&mut self) -> Result<T, RecvError> {
        poll_fn(|cx| self.poll_recv(cx)).await
    }

    /// Returns the number of messages currently available to this receiver.
    pub fn len(&self) -> usize {
        let state = self
            .shared
            .lock()
            .expect("broadcast state should not be poisoned");
        let oldest = state.oldest_seq();
        let pending = if self.next_seq < oldest {
            state.next_seq - oldest
        } else {
            state.next_seq.saturating_sub(self.next_seq)
        };
        usize::try_from(pending).unwrap_or(usize::MAX)
    }

    /// Returns `true` if no messages are currently available to this receiver.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Creates a new receiver for values sent after this call.
    pub fn resubscribe(&self) -> Receiver<T> {
        let next_seq = {
            let mut state = self
                .shared
                .lock()
                .expect("broadcast state should not be poisoned");
            state.receiver_count += 1;
            state.next_seq
        };
        Receiver {
            shared: Arc::clone(&self.shared),
            next_seq,
            wait: None,
        }
    }

    fn poll_recv(&mut self, cx: &mut Context<'_>) -> Poll<Result<T, RecvError>> {
        if let Some(future) = self.wait.as_mut() {
            match Pin::new(future).poll(cx) {
                Poll::Ready(outcome) => {
                    self.wait.take();
                    Poll::Ready(self.apply_outcome(outcome))
                }
                Poll::Pending => Poll::Pending,
            }
        } else {
            let (future, handle) = runtime_waiter::<RecvOutcome<T>>();
            let immediate = {
                let mut state = self
                    .shared
                    .lock()
                    .expect("broadcast state should not be poisoned");
                if let Some(outcome) = state.recv_outcome(self.next_seq) {
                    Some(outcome)
                } else {
                    let waiter_id = state.enqueue_waiter(self.next_seq, handle.clone());
                    set_cancel_waiter(&handle, &self.shared, waiter_id);
                    None
                }
            };

            if let Some(outcome) = immediate {
                handle.complete(outcome);
            }

            self.wait = Some(future);
            self.poll_recv(cx)
        }
    }

    fn apply_outcome(&mut self, outcome: RecvOutcome<T>) -> Result<T, RecvError> {
        match outcome {
            RecvOutcome::Value(value, next_seq) => {
                self.next_seq = next_seq;
                Ok(value)
            }
            RecvOutcome::Lagged(skipped, next_seq) => {
                self.next_seq = next_seq;
                Err(RecvError::Lagged(skipped))
            }
            RecvOutcome::Closed => Err(RecvError::Closed),
        }
    }
}

fn set_cancel_waiter<T: Clone + Send + 'static>(
    handle: &CompletionHandle<RecvOutcome<T>>,
    shared: &Arc<Mutex<State<T>>>,
    waiter_id: usize,
) {
    let cancel_shared = Arc::clone(shared);
    let cancel_handle = handle.clone();
    handle.set_cancel(move || {
        let mut state = cancel_shared
            .lock()
            .expect("broadcast state should not be poisoned");
        state.remove_waiter(waiter_id);
        drop(state);
        cancel_handle.finish(None);
    });
}

impl<T: Clone + Send + 'static> Drop for Sender<T> {
    fn drop(&mut self) {
        let waiters = {
            let mut state = self
                .shared
                .lock()
                .expect("broadcast state should not be poisoned");
            state.drop_sender()
        };
        self.complete_waiters(waiters);
    }
}

impl<T: Clone + Send + 'static> Drop for Receiver<T> {
    fn drop(&mut self) {
        let mut state = self
            .shared
            .lock()
            .expect("broadcast state should not be poisoned");
        state.receiver_count = state
            .receiver_count
            .checked_sub(1)
            .expect("receiver count underflow: more drops than creates");
    }
}

impl<T> fmt::Display for SendError<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "channel closed")
    }
}

impl<T: fmt::Debug> std::error::Error for SendError<T> {}

impl fmt::Display for RecvError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Lagged(skipped) => write!(f, "receiver lagged by {skipped} messages"),
            Self::Closed => write!(f, "channel closed"),
        }
    }
}

impl std::error::Error for RecvError {}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Mutex};

    use crate::{queue_future, queue_task, run};

    use super::{RecvError, channel};

    #[test]
    fn fan_out_to_multiple_receivers() {
        let observed = Arc::new(Mutex::new(None::<(Vec<i32>, Vec<i32>)>));
        let observed_for_task = Arc::clone(&observed);

        queue_task(move || {
            let (sender, mut first) = channel(8);
            let mut second = sender.subscribe();
            queue_future(async move {
                assert_eq!(sender.send(1), Ok(2));
                assert_eq!(sender.send(2), Ok(2));
                let first_values = vec![first.recv().await.unwrap(), first.recv().await.unwrap()];
                let second_values =
                    vec![second.recv().await.unwrap(), second.recv().await.unwrap()];
                *observed_for_task.lock().unwrap() = Some((first_values, second_values));
            });
        });
        run();

        assert_eq!(*observed.lock().unwrap(), Some((vec![1, 2], vec![1, 2])));
    }

    #[test]
    fn slow_receiver_lags_then_resumes_at_oldest_value() {
        let observed = Arc::new(Mutex::new(None::<(RecvError, i32)>));
        let observed_for_task = Arc::clone(&observed);

        queue_task(move || {
            let (sender, mut receiver) = channel(2);
            queue_future(async move {
                sender.send(1).unwrap();
                sender.send(2).unwrap();
                sender.send(3).unwrap();
                let lag = receiver.recv().await.unwrap_err();
                let next = receiver.recv().await.unwrap();
                *observed_for_task.lock().unwrap() = Some((lag, next));
            });
        });
        run();

        assert_eq!(*observed.lock().unwrap(), Some((RecvError::Lagged(1), 2)));
    }

    #[test]
    fn closed_after_all_senders_drop_and_buffer_drains() {
        let observed = Arc::new(Mutex::new(None::<(i32, RecvError)>));
        let observed_for_task = Arc::clone(&observed);

        queue_task(move || {
            let (sender, mut receiver) = channel(2);
            queue_future(async move {
                sender.send(7).unwrap();
                drop(sender);
                let value = receiver.recv().await.unwrap();
                let closed = receiver.recv().await.unwrap_err();
                *observed_for_task.lock().unwrap() = Some((value, closed));
            });
        });
        run();

        assert_eq!(*observed.lock().unwrap(), Some((7, RecvError::Closed)));
    }

    #[test]
    fn subscribe_sees_only_future_values() {
        let observed = Arc::new(Mutex::new(None::<Vec<i32>>));
        let observed_for_task = Arc::clone(&observed);

        queue_task(move || {
            let (sender, original) = channel(4);
            queue_future(async move {
                sender.send(1).unwrap();
                let mut receiver = sender.subscribe();
                sender.send(2).unwrap();
                sender.send(3).unwrap();
                drop(original);
                *observed_for_task.lock().unwrap() = Some(vec![
                    receiver.recv().await.unwrap(),
                    receiver.recv().await.unwrap(),
                ]);
            });
        });
        run();

        assert_eq!(*observed.lock().unwrap(), Some(vec![2, 3]));
    }
}
