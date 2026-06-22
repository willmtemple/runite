//! Single-value watch channels.
//!
//! # Examples
//!
//! Borrow the latest value immediately, then wait for a later change.
//!
//! ```
//! let (sender, mut receiver) = runite::channel::watch::channel("initial");
//! assert_eq!(*receiver.borrow(), "initial");
//!
//! runite::queue_future(async move {
//!     sender.send("updated").unwrap();
//! });
//!
//! runite::queue_future(async move {
//!     receiver.changed().await.unwrap();
//!     assert_eq!(*receiver.borrow(), "updated");
//! });
//!
//! runite::run();
//! ```

use std::fmt;
use std::future::poll_fn;
use std::ops::Deref;
use std::pin::Pin;
use std::sync::{Arc, Mutex, MutexGuard};
use std::task::{Context, Poll};

use crate::op::completion::{CompletionFuture, CompletionHandle};
use crate::sys::current::channel::runtime_waiter;

/// Creates a watch channel initialized with `initial`.
///
/// A watch channel stores a single latest value. Receivers can borrow the
/// current value at any time and await notification when a newer version is
/// published.
pub fn channel<T: Send + 'static>(initial: T) -> (Sender<T>, Receiver<T>) {
    let shared = Arc::new(Mutex::new(State {
        value: initial,
        version: 0,
        sender_count: 1,
        receiver_count: 1,
        waiters: Vec::new(),
        next_waiter_id: 1,
    }));
    (
        Sender {
            shared: Arc::clone(&shared),
        },
        Receiver {
            shared,
            version: 0,
            wait: None,
        },
    )
}

/// Sending half of a watch channel.
///
/// Cloning a sender creates another producer for the same latest-value slot.
/// Sending replaces the stored value and wakes receivers that are waiting for a
/// newer version.
pub struct Sender<T: Send + 'static> {
    shared: Arc<Mutex<State<T>>>,
}

/// Receiving half of a watch channel.
///
/// A receiver tracks the last version it observed. Use [`changed`](Self::changed)
/// to wait for a newer value, then [`borrow_and_update`](Self::borrow_and_update)
/// to access it and mark that version as seen.
pub struct Receiver<T: Send + 'static> {
    shared: Arc<Mutex<State<T>>>,
    version: u64,
    wait: Option<CompletionFuture<Result<u64, RecvError>>>,
}

/// Borrowed watch value.
///
/// This guard dereferences to the current value and holds the channel lock while
/// it is alive, so keep borrows short and avoid holding them across `.await`.
pub struct Ref<'a, T: Send + 'static> {
    guard: MutexGuard<'a, State<T>>,
}

struct State<T: Send + 'static> {
    value: T,
    version: u64,
    sender_count: usize,
    receiver_count: usize,
    waiters: Vec<WatchWaiter>,
    next_waiter_id: usize,
}

struct WatchWaiter {
    id: usize,
    version: u64,
    handle: CompletionHandle<Result<u64, RecvError>>,
}

#[derive(Debug, Eq, PartialEq)]
/// Error returned when sending fails because there are no receivers.
///
/// The unsent replacement value is returned to the caller.
pub struct SendError<T>(pub T);

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
/// Error returned when a watch channel closes before another change arrives.
///
/// Existing values can still be borrowed; this error means all senders were
/// dropped while the receiver was waiting for a newer version.
pub struct RecvError;

impl<T: Send + 'static> State<T> {
    fn enqueue_waiter(
        &mut self,
        version: u64,
        handle: CompletionHandle<Result<u64, RecvError>>,
    ) -> usize {
        let id = self.next_waiter_id;
        self.next_waiter_id = self.next_waiter_id.wrapping_add(1);
        self.waiters.push(WatchWaiter {
            id,
            version,
            handle,
        });
        id
    }

    fn remove_waiter(&mut self, waiter_id: usize) {
        if let Some(index) = self
            .waiters
            .iter()
            .position(|waiter| waiter.id == waiter_id)
        {
            self.waiters.swap_remove(index);
        }
    }

    fn wake_changed(&mut self) -> Vec<CompletionHandle<Result<u64, RecvError>>> {
        let mut ready = Vec::new();
        let mut index = 0;
        while index < self.waiters.len() {
            if self.waiters[index].version < self.version {
                ready.push(self.waiters.swap_remove(index).handle);
            } else {
                index += 1;
            }
        }
        ready
    }

    fn close_sender(&mut self) -> Vec<CompletionHandle<Result<u64, RecvError>>> {
        self.sender_count = self
            .sender_count
            .checked_sub(1)
            .expect("sender count underflow: more drops than creates");
        if self.sender_count == 0 {
            self.waiters.drain(..).map(|waiter| waiter.handle).collect()
        } else {
            Vec::new()
        }
    }
}

impl<T: Send + 'static> Clone for Sender<T> {
    fn clone(&self) -> Self {
        self.shared
            .lock()
            .expect("watch state should not be poisoned")
            .sender_count += 1;
        Self {
            shared: Arc::clone(&self.shared),
        }
    }
}

impl<T: Send + 'static> Clone for Receiver<T> {
    fn clone(&self) -> Self {
        self.shared
            .lock()
            .expect("watch state should not be poisoned")
            .receiver_count += 1;
        Self {
            shared: Arc::clone(&self.shared),
            version: self.version,
            wait: None,
        }
    }
}

impl<T: Send + 'static> Sender<T> {
    /// Replaces the watched value and notifies receivers.
    ///
    /// Returns [`SendError`] with the value if no receivers remain.
    pub fn send(&self, value: T) -> Result<(), SendError<T>> {
        let waiters = {
            let mut state = self
                .shared
                .lock()
                .expect("watch state should not be poisoned");
            if state.receiver_count == 0 {
                return Err(SendError(value));
            }
            state.value = value;
            state.version = state.version.wrapping_add(1);
            state.wake_changed()
        };
        self.complete_changed(waiters);
        Ok(())
    }

    /// Mutates the watched value and notifies receivers.
    ///
    /// This notifies even if the closure leaves the value unchanged; use
    /// [`send_if_modified`](Self::send_if_modified) to make notification
    /// conditional.
    pub fn send_modify(&self, f: impl FnOnce(&mut T)) {
        let waiters = {
            let mut state = self
                .shared
                .lock()
                .expect("watch state should not be poisoned");
            f(&mut state.value);
            state.version = state.version.wrapping_add(1);
            state.wake_changed()
        };
        self.complete_changed(waiters);
    }

    /// Mutates the watched value and notifies receivers if `f` returns `true`.
    pub fn send_if_modified(&self, f: impl FnOnce(&mut T) -> bool) -> bool {
        let waiters = {
            let mut state = self
                .shared
                .lock()
                .expect("watch state should not be poisoned");
            if !f(&mut state.value) {
                return false;
            }
            state.version = state.version.wrapping_add(1);
            state.wake_changed()
        };
        self.complete_changed(waiters);
        true
    }

    /// Borrows the current value from the sender side.
    ///
    /// The returned [`Ref`] holds the channel lock until dropped.
    pub fn borrow(&self) -> Ref<'_, T> {
        Ref {
            guard: self
                .shared
                .lock()
                .expect("watch state should not be poisoned"),
        }
    }

    /// Creates a new receiver that starts at the current version.
    ///
    /// The receiver considers the current value already observed and waits for
    /// subsequent calls to [`send`](Self::send) or mutation methods.
    pub fn subscribe(&self) -> Receiver<T> {
        let version = {
            let mut state = self
                .shared
                .lock()
                .expect("watch state should not be poisoned");
            state.receiver_count += 1;
            state.version
        };
        Receiver {
            shared: Arc::clone(&self.shared),
            version,
            wait: None,
        }
    }

    /// Returns the number of active receivers.
    pub fn receiver_count(&self) -> usize {
        self.shared
            .lock()
            .expect("watch state should not be poisoned")
            .receiver_count
    }

    fn complete_changed(&self, waiters: Vec<CompletionHandle<Result<u64, RecvError>>>) {
        let version = self
            .shared
            .lock()
            .expect("watch state should not be poisoned")
            .version;
        for waiter in waiters {
            waiter.complete(Ok(version));
        }
    }
}

impl<T: Send + 'static> Receiver<T> {
    /// Waits until the watched value changes.
    ///
    /// Returns [`RecvError`] if all senders are dropped before a newer version
    /// becomes available.
    ///
    /// # Panics
    ///
    /// Panics if this future is first polled outside a runtime-managed thread.
    pub async fn changed(&mut self) -> Result<(), RecvError> {
        poll_fn(|cx| self.poll_changed(cx)).await
    }

    /// Borrows the current value without marking it observed.
    ///
    /// A later [`changed`](Self::changed) call can still complete immediately if
    /// this value is newer than the receiver's recorded version.
    pub fn borrow(&self) -> Ref<'_, T> {
        Ref {
            guard: self
                .shared
                .lock()
                .expect("watch state should not be poisoned"),
        }
    }

    /// Borrows the current value and marks it observed.
    ///
    /// The returned [`Ref`] holds the channel lock until dropped.
    pub fn borrow_and_update(&mut self) -> Ref<'_, T> {
        let guard = self
            .shared
            .lock()
            .expect("watch state should not be poisoned");
        self.version = guard.version;
        Ref { guard }
    }

    fn poll_changed(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), RecvError>> {
        if let Some(future) = self.wait.as_mut() {
            match Pin::new(future).poll(cx) {
                Poll::Ready(result) => {
                    self.wait.take();
                    if let Ok(version) = result {
                        self.version = version;
                        Poll::Ready(Ok(()))
                    } else {
                        Poll::Ready(Err(RecvError))
                    }
                }
                Poll::Pending => Poll::Pending,
            }
        } else {
            let (future, handle) = runtime_waiter::<Result<u64, RecvError>>();
            let immediate = {
                let mut state = self
                    .shared
                    .lock()
                    .expect("watch state should not be poisoned");
                if state.version > self.version {
                    Some(Ok(state.version))
                } else if state.sender_count == 0 {
                    Some(Err(RecvError))
                } else {
                    let waiter_id = state.enqueue_waiter(self.version, handle.clone());
                    set_cancel_waiter(&handle, &self.shared, waiter_id);
                    None
                }
            };

            if let Some(result) = immediate {
                handle.complete(result);
            }

            self.wait = Some(future);
            self.poll_changed(cx)
        }
    }
}

impl<'a, T: Send + 'static> Deref for Ref<'a, T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        &self.guard.value
    }
}

fn set_cancel_waiter<T: Send + 'static>(
    handle: &CompletionHandle<Result<u64, RecvError>>,
    shared: &Arc<Mutex<State<T>>>,
    waiter_id: usize,
) {
    let cancel_shared = Arc::clone(shared);
    let cancel_handle = handle.clone();
    handle.set_cancel(move || {
        let mut state = cancel_shared
            .lock()
            .expect("watch state should not be poisoned");
        state.remove_waiter(waiter_id);
        drop(state);
        cancel_handle.finish(None);
    });
}

impl<T: Send + 'static> Drop for Sender<T> {
    fn drop(&mut self) {
        let waiters = {
            let mut state = self
                .shared
                .lock()
                .expect("watch state should not be poisoned");
            state.close_sender()
        };
        for waiter in waiters {
            waiter.complete(Err(RecvError));
        }
    }
}

impl<T: Send + 'static> Drop for Receiver<T> {
    fn drop(&mut self) {
        let mut state = self
            .shared
            .lock()
            .expect("watch state should not be poisoned");
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
        write!(f, "channel closed")
    }
}

impl std::error::Error for RecvError {}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Mutex};

    use crate::{queue_future, queue_task, run};

    use super::{RecvError, channel};

    #[test]
    fn receiver_borrows_initial_value() {
        let (_sender, receiver) = channel(5usize);
        assert_eq!(*receiver.borrow(), 5);
    }

    #[test]
    fn changed_fires_after_send() {
        let observed = Arc::new(Mutex::new(None::<usize>));
        let observed_for_task = Arc::clone(&observed);

        queue_task(move || {
            let (sender, mut receiver) = channel(1usize);
            queue_future(async move {
                sender.send(2).unwrap();
                receiver.changed().await.unwrap();
                *observed_for_task.lock().unwrap() = Some(*receiver.borrow());
            });
        });
        run();

        assert_eq!(*observed.lock().unwrap(), Some(2));
    }

    #[test]
    fn rapid_sends_coalesce_to_latest_value() {
        let observed = Arc::new(Mutex::new(None::<usize>));
        let observed_for_task = Arc::clone(&observed);

        queue_task(move || {
            let (sender, mut receiver) = channel(0usize);
            queue_future(async move {
                sender.send(1).unwrap();
                sender.send(2).unwrap();
                sender.send(3).unwrap();
                receiver.changed().await.unwrap();
                *observed_for_task.lock().unwrap() = Some(*receiver.borrow_and_update());
            });
        });
        run();

        assert_eq!(*observed.lock().unwrap(), Some(3));
    }

    #[test]
    fn changed_errors_after_all_senders_drop() {
        let observed = Arc::new(Mutex::new(None::<RecvError>));
        let observed_for_task = Arc::clone(&observed);

        queue_task(move || {
            let (sender, mut receiver) = channel(0usize);
            queue_future(async move {
                drop(sender);
                *observed_for_task.lock().unwrap() = Some(receiver.changed().await.unwrap_err());
            });
        });
        run();

        assert_eq!(*observed.lock().unwrap(), Some(RecvError));
    }

    #[test]
    fn send_modify_updates_value() {
        let observed = Arc::new(Mutex::new(None::<usize>));
        let observed_for_task = Arc::clone(&observed);

        queue_task(move || {
            let (sender, mut receiver) = channel(1usize);
            queue_future(async move {
                sender.send_modify(|value| *value += 41);
                receiver.changed().await.unwrap();
                *observed_for_task.lock().unwrap() = Some(*receiver.borrow());
            });
        });
        run();

        assert_eq!(*observed.lock().unwrap(), Some(42));
    }
}
