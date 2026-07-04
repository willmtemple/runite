//! Single-value watch channels.
//!
//! A watch channel stores the latest value and notifies receivers when a newer
//! version is published. It is best for sharing state snapshots, configuration,
//! or status values where receivers do not need every intermediate update.
//! Waiters are tied to the runite runtime thread that first polls them; multiple
//! sends coalesce, so a receiver that waits once observes that the version
//! changed and then borrows the latest value.
//!
//! # Examples
//!
//! Borrow the latest value immediately, then wait for a later change.
//!
//! ```
//! let (sender, mut receiver) = runite::channel::watch::channel("initial");
//! assert_eq!(*receiver.borrow(), "initial");
//!
//! runite::spawn(async move {
//!     sender.send("updated").unwrap();
//! });
//!
//! runite::spawn(async move {
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
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, RwLock, RwLockReadGuard};
use std::task::{Context, Poll};

use crate::op::completion::{CompletionFuture, CompletionHandle};
use crate::sys::current::channel::runtime_waiter;

/// Creates a watch channel initialized with `initial`.
///
/// A watch channel stores a single latest value. Receivers can borrow the
/// current value at any time and await notification when a newer version is
/// published.
///
/// # Examples
///
/// ```
/// let (sender, receiver) = runite::channel::watch::channel(1);
/// assert_eq!(*sender.borrow(), 1);
/// assert_eq!(*receiver.borrow(), 1);
/// ```
pub fn channel<T: Send + 'static>(initial: T) -> (Sender<T>, Receiver<T>) {
    let shared = Arc::new(Shared {
        value: RwLock::new(initial),
        version: AtomicU64::new(0),
        book: Arc::new(Mutex::new(Book {
            sender_count: 1,
            receiver_count: 1,
            waiters: Vec::new(),
            next_waiter_id: 1,
        })),
    });
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
/// newer version on their owning runtime threads. Intermediate values coalesce:
/// receivers observe that the version changed, then borrow the latest value.
pub struct Sender<T: Send + 'static> {
    shared: Arc<Shared<T>>,
}

/// Receiving half of a watch channel.
///
/// A receiver tracks the last version it observed. Use [`changed`](Self::changed)
/// to wait for a newer value, then [`borrow_and_update`](Self::borrow_and_update)
/// to access it and mark that version as seen.
pub struct Receiver<T: Send + 'static> {
    shared: Arc<Shared<T>>,
    version: u64,
    wait: Option<CompletionFuture<Result<u64, RecvError>>>,
}

/// Borrowed watch value.
///
/// This guard dereferences to the current value and holds a **read** lock on the
/// value slot while it is alive. Multiple `Ref`s may be held at once — including
/// several on the same thread — because reads are shared. Holding a `Ref` across
/// a [`Sender::send`] (or any mutation) on the *same thread* will deadlock,
/// because the send needs the write lock; keep borrows short and never hold one
/// across `.await`.
pub struct Ref<'a, T: Send + 'static> {
    guard: RwLockReadGuard<'a, T>,
}

/// Shared channel storage.
///
/// The value and the bookkeeping live behind **separate** locks so that a
/// `Ref` (a value read lock) does not block, and is not blocked by, waiter
/// bookkeeping. This is what lets two `Ref`s coexist on one thread — the old
/// single-`Mutex` design deadlocked on the second borrow.
struct Shared<T: Send + 'static> {
    /// Current value, guarded by an `RwLock` so multiple `Ref` borrows can be
    /// held concurrently.
    value: RwLock<T>,
    /// Monotonic version, bumped **under the `value` write lock** so a borrower
    /// that reads the value and the version under the `value` read lock always
    /// observes a consistent pair.
    version: AtomicU64,
    /// Waiter registry and reference counts. Held in its own `Arc<Mutex<_>>` so
    /// the (`Send`) cancel closure can capture just the bookkeeping without
    /// capturing `T`, keeping the channel usable with `Send`-but-not-`Sync`
    /// values on a single thread.
    book: Arc<Mutex<Book>>,
}

struct Book {
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

impl Book {
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

    fn wake_changed(
        &mut self,
        current_version: u64,
    ) -> Vec<CompletionHandle<Result<u64, RecvError>>> {
        let mut ready = Vec::new();
        let mut index = 0;
        while index < self.waiters.len() {
            if self.waiters[index].version < current_version {
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

impl<T: Send + 'static> Shared<T> {
    fn lock_book(&self) -> std::sync::MutexGuard<'_, Book> {
        self.book
            .lock()
            .expect("watch state should not be poisoned")
    }

    /// Replaces the value via `mutate` and bumps the version, both under the
    /// value write lock so borrowers see a consistent (value, version) pair.
    /// Returns the new version.
    fn write_value(&self, mutate: impl FnOnce(&mut T)) -> u64 {
        let mut slot = self
            .value
            .write()
            .expect("watch state should not be poisoned");
        mutate(&mut slot);
        // Bump under the write lock: readers taking the value read lock cannot
        // observe the new value with the old version, or vice versa.
        self.version.fetch_add(1, Ordering::Release) + 1
    }
}

impl<T: Send + 'static> Clone for Sender<T> {
    fn clone(&self) -> Self {
        self.shared.lock_book().sender_count += 1;
        Self {
            shared: Arc::clone(&self.shared),
        }
    }
}

impl<T: Send + 'static> Clone for Receiver<T> {
    fn clone(&self) -> Self {
        self.shared.lock_book().receiver_count += 1;
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
    ///
    /// # Examples
    ///
    /// ```
    /// runite::spawn(async {
    ///     let (sender, mut receiver) = runite::channel::watch::channel("old");
    ///     sender.send("new").unwrap();
    ///     receiver.changed().await.unwrap();
    ///     assert_eq!(*receiver.borrow(), "new");
    /// });
    ///
    /// runite::run();
    /// ```
    pub fn send(&self, value: T) -> Result<(), SendError<T>> {
        let waiters = {
            let mut book = self.shared.lock_book();
            if book.receiver_count == 0 {
                return Err(SendError(value));
            }
            let version = self.shared.write_value(|slot| *slot = value);
            book.wake_changed(version)
        };
        self.complete_changed(waiters);
        Ok(())
    }

    /// Mutates the watched value and notifies receivers.
    ///
    /// This notifies even if the closure leaves the value unchanged; use
    /// [`send_if_modified`](Self::send_if_modified) to make notification
    /// conditional. Unlike [`send`](Self::send), this method still mutates the
    /// stored value and does not return [`SendError`] when no receivers remain.
    ///
    /// # Examples
    ///
    /// ```
    /// let (sender, receiver) = runite::channel::watch::channel(1);
    /// sender.send_modify(|value| *value += 1);
    /// assert_eq!(*receiver.borrow(), 2);
    /// ```
    pub fn send_modify(&self, f: impl FnOnce(&mut T)) {
        let waiters = {
            let mut book = self.shared.lock_book();
            let version = self.shared.write_value(f);
            book.wake_changed(version)
        };
        self.complete_changed(waiters);
    }

    /// Mutates the watched value and notifies receivers if `f` returns `true`.
    ///
    /// Unlike [`send`](Self::send), this method does not return [`SendError`] if
    /// no receivers remain. It returns whether `f` reported a modification; when
    /// it returns `true`, the version is advanced even with zero receivers.
    ///
    /// # Examples
    ///
    /// ```
    /// let (sender, receiver) = runite::channel::watch::channel(1);
    /// assert!(!sender.send_if_modified(|_| false));
    /// assert!(sender.send_if_modified(|value| {
    ///     *value = 3;
    ///     true
    /// }));
    /// assert_eq!(*receiver.borrow(), 3);
    /// ```
    pub fn send_if_modified(&self, f: impl FnOnce(&mut T) -> bool) -> bool {
        let waiters = {
            let mut book = self.shared.lock_book();
            // Run `f` under the value write lock; only bump the version and
            // collect waiters if it reports a modification, otherwise bail
            // (dropping both guards) without advancing the version.
            let version = {
                let mut slot = self
                    .shared
                    .value
                    .write()
                    .expect("watch state should not be poisoned");
                if !f(&mut slot) {
                    return false;
                }
                self.shared.version.fetch_add(1, Ordering::Release) + 1
            };
            book.wake_changed(version)
        };
        self.complete_changed(waiters);
        true
    }

    /// Borrows the current value from the sender side.
    ///
    /// The returned [`Ref`] holds a read lock on the value until dropped.
    ///
    /// # Examples
    ///
    /// ```
    /// let (sender, _receiver) = runite::channel::watch::channel("visible");
    /// assert_eq!(*sender.borrow(), "visible");
    /// ```
    pub fn borrow(&self) -> Ref<'_, T> {
        Ref {
            guard: self
                .shared
                .value
                .read()
                .expect("watch state should not be poisoned"),
        }
    }

    /// Creates a new receiver that starts at the current version.
    ///
    /// The receiver considers the current value already observed and waits for
    /// subsequent calls to [`send`](Self::send) or mutation methods.
    ///
    /// # Examples
    ///
    /// ```
    /// let (sender, _receiver) = runite::channel::watch::channel(1);
    /// let second = sender.subscribe();
    /// assert_eq!(*second.borrow(), 1);
    /// ```
    pub fn subscribe(&self) -> Receiver<T> {
        let version = {
            let mut book = self.shared.lock_book();
            book.receiver_count += 1;
            self.shared.version.load(Ordering::Acquire)
        };
        Receiver {
            shared: Arc::clone(&self.shared),
            version,
            wait: None,
        }
    }

    /// Returns the number of active receivers.
    ///
    /// # Examples
    ///
    /// ```
    /// let (sender, receiver) = runite::channel::watch::channel(1);
    /// assert_eq!(sender.receiver_count(), 1);
    /// drop(receiver);
    /// assert_eq!(sender.receiver_count(), 0);
    /// ```
    pub fn receiver_count(&self) -> usize {
        self.shared.lock_book().receiver_count
    }

    fn complete_changed(&self, waiters: Vec<CompletionHandle<Result<u64, RecvError>>>) {
        let version = self.shared.version.load(Ordering::Acquire);
        for waiter in waiters {
            waiter.complete(Ok(version));
        }
    }
}

impl<T: Send + 'static> Receiver<T> {
    /// Waits until the watched value changes.
    ///
    /// Multiple sends before this receiver borrows and updates coalesce into one
    /// observed version change; use [`borrow_and_update`](Self::borrow_and_update)
    /// to mark the latest value as seen.
    ///
    /// Returns [`RecvError`] if all senders are dropped before a newer version
    /// becomes available.
    ///
    /// # Examples
    ///
    /// ```
    /// runite::spawn(async {
    ///     let (sender, mut receiver) = runite::channel::watch::channel(0);
    ///     sender.send(1).unwrap();
    ///     receiver.changed().await.unwrap();
    ///     assert_eq!(*receiver.borrow(), 1);
    /// });
    ///
    /// runite::run();
    /// ```
    ///
    /// # Cancel safety
    ///
    /// Cancel-safe: dropping the returned future before it resolves does not
    /// advance the receiver's observed version, so a later `changed` still
    /// reports the pending change.
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
    ///
    /// # Examples
    ///
    /// ```
    /// let (sender, receiver) = runite::channel::watch::channel(1);
    /// sender.send(2).unwrap();
    /// assert_eq!(*receiver.borrow(), 2);
    /// ```
    pub fn borrow(&self) -> Ref<'_, T> {
        Ref {
            guard: self
                .shared
                .value
                .read()
                .expect("watch state should not be poisoned"),
        }
    }

    /// Borrows the current value and marks it observed.
    ///
    /// The returned [`Ref`] holds a read lock on the value until dropped.
    ///
    /// # Examples
    ///
    /// ```
    /// let (sender, mut receiver) = runite::channel::watch::channel(1);
    /// sender.send(2).unwrap();
    /// assert_eq!(*receiver.borrow_and_update(), 2);
    /// ```
    pub fn borrow_and_update(&mut self) -> Ref<'_, T> {
        // Read the value and the version under the same value read lock so the
        // recorded version matches the value being returned.
        let guard = self
            .shared
            .value
            .read()
            .expect("watch state should not be poisoned");
        self.version = self.shared.version.load(Ordering::Acquire);
        Ref { guard }
    }

    fn poll_changed(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), RecvError>> {
        if let Some(future) = self.wait.as_mut() {
            match Pin::new(future).poll(cx) {
                Poll::Ready(result) => {
                    self.wait.take();
                    match result {
                        Ok(version) if version > self.version => {
                            self.version = version;
                            Poll::Ready(Ok(()))
                        }
                        Ok(_) => {
                            // Stale completion: a waiter registered at an older
                            // version fired, but `self.version` has since caught
                            // up or passed it (e.g. via `borrow_and_update`).
                            // Accepting it would regress `self.version` and
                            // report a spurious change, so discard it and
                            // re-register instead of moving the version backward.
                            self.poll_changed(cx)
                        }
                        Err(_) => Poll::Ready(Err(RecvError)),
                    }
                }
                Poll::Pending => Poll::Pending,
            }
        } else {
            let (future, handle) = runtime_waiter::<Result<u64, RecvError>>();
            let immediate = {
                let mut book = self.shared.lock_book();
                // Read the version under the book lock: `send` bumps the version
                // (under the value lock) and then wakes waiters under this same
                // book lock, so checking the version and enqueueing here cannot
                // race with a send in a way that loses the wakeup.
                let current = self.shared.version.load(Ordering::Acquire);
                if current > self.version {
                    Some(Ok(current))
                } else if book.sender_count == 0 {
                    Some(Err(RecvError))
                } else {
                    let waiter_id = book.enqueue_waiter(self.version, handle.clone());
                    set_cancel_waiter(&handle, &self.shared.book, waiter_id);
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
        &self.guard
    }
}

fn set_cancel_waiter(
    handle: &CompletionHandle<Result<u64, RecvError>>,
    book: &Arc<Mutex<Book>>,
    waiter_id: usize,
) {
    let cancel_book = Arc::clone(book);
    let cancel_handle = handle.clone();
    handle.set_cancel(move || {
        let mut book = cancel_book
            .lock()
            .expect("watch state should not be poisoned");
        book.remove_waiter(waiter_id);
        drop(book);
        cancel_handle.finish(None);
    });
}

impl<T: Send + 'static> Drop for Sender<T> {
    fn drop(&mut self) {
        let waiters = {
            let mut book = self.shared.lock_book();
            book.close_sender()
        };
        for waiter in waiters {
            waiter.complete(Err(RecvError));
        }
    }
}

impl<T: Send + 'static> Drop for Receiver<T> {
    fn drop(&mut self) {
        let mut book = self.shared.lock_book();
        book.receiver_count = book
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

    use crate::{queue_macrotask, run, spawn};

    use super::{RecvError, channel};

    #[test]
    fn receiver_borrows_initial_value() {
        let (_sender, receiver) = channel(5usize);
        assert_eq!(*receiver.borrow(), 5);
    }

    #[test]
    fn two_borrows_on_same_thread_do_not_deadlock() {
        // Two live borrows on one thread must not deadlock: `Ref` holds a
        // shared read lock on the value slot, not an exclusive channel lock,
        // so any number of concurrent borrows coexist.
        let (sender, receiver) = channel(7usize);
        let a = receiver.borrow();
        let b = sender.borrow();
        let c = receiver.borrow();
        assert_eq!((*a, *b, *c), (7, 7, 7));
    }

    #[test]
    fn changed_fires_after_send() {
        let observed = Arc::new(Mutex::new(None::<usize>));
        let observed_for_task = Arc::clone(&observed);

        queue_macrotask(move || {
            let (sender, mut receiver) = channel(1usize);
            spawn(async move {
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

        queue_macrotask(move || {
            let (sender, mut receiver) = channel(0usize);
            spawn(async move {
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

        queue_macrotask(move || {
            let (sender, mut receiver) = channel(0usize);
            spawn(async move {
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

        queue_macrotask(move || {
            let (sender, mut receiver) = channel(1usize);
            spawn(async move {
                sender.send_modify(|value| *value += 41);
                receiver.changed().await.unwrap();
                *observed_for_task.lock().unwrap() = Some(*receiver.borrow());
            });
        });
        run();

        assert_eq!(*observed.lock().unwrap(), Some(42));
    }
}
