use std::cell::{Cell, RefCell, UnsafeCell};
use std::collections::VecDeque;
use std::future::Future;
use std::marker::PhantomData;
use std::ops::{Deref, DerefMut};
use std::pin::Pin;
use std::rc::Rc;
use std::task::{Context, Poll, Waker};

#[derive(Clone, Copy, Eq, PartialEq)]
enum WaiterKind {
    Read,
    Write,
}

struct Waiter {
    id: usize,
    kind: WaiterKind,
    selected: Rc<Cell<bool>>,
    waker: Waker,
}

/// A single-threaded async reader-writer lock.
///
/// Multiple readers may hold the lock at the same time, while writers require
/// exclusive access. Waiters are served in FIFO order from one shared queue: if
/// a writer is queued, later readers wait behind it instead of bypassing it.
///
/// This lock is intentionally `!Send` and `!Sync`: it is only for tasks that
/// remain on one runite runtime thread.
///
/// # Examples
///
/// ```
/// use std::cell::Cell;
/// use std::rc::Rc;
///
/// use runite::sync::RwLock;
///
/// let lock = Rc::new(RwLock::new(1));
/// let observed = Rc::new(Cell::new(0));
///
/// runite::spawn({
///     let lock = Rc::clone(&lock);
///     let observed = Rc::clone(&observed);
///     async move {
///         let mut value = lock.write().await;
///         *value += 41;
///         observed.set(*value);
///     }
/// });
///
/// runite::run();
///
/// assert_eq!(observed.get(), 42);
/// assert_eq!(*lock.try_read().expect("lock should be readable"), 42);
/// ```
pub struct RwLock<T: ?Sized> {
    readers: Cell<usize>,
    writer: Cell<bool>,
    next_waiter_id: Cell<usize>,
    waiters: RefCell<VecDeque<Waiter>>,
    _not_send_sync: PhantomData<Rc<()>>,
    value: UnsafeCell<T>,
}

/// Read guard returned by [`RwLock::read`] and [`RwLock::try_read`].
///
/// The guard dereferences to the protected value and releases its reader slot
/// when it is dropped.
pub struct RwLockReadGuard<'a, T: ?Sized> {
    lock: &'a RwLock<T>,
    _not_send_sync: PhantomData<Rc<()>>,
}

/// Write guard returned by [`RwLock::write`] and [`RwLock::try_write`].
///
/// The guard dereferences mutably to the protected value and releases exclusive
/// access when it is dropped.
pub struct RwLockWriteGuard<'a, T: ?Sized> {
    lock: &'a RwLock<T>,
    _not_send_sync: PhantomData<Rc<()>>,
}

/// Future returned by [`RwLock::read`].
pub struct RwLockReadFuture<'a, T: ?Sized> {
    lock: &'a RwLock<T>,
    waiter: Option<(usize, Rc<Cell<bool>>)>,
    acquired: bool,
}

/// Future returned by [`RwLock::write`].
pub struct RwLockWriteFuture<'a, T: ?Sized> {
    lock: &'a RwLock<T>,
    waiter: Option<(usize, Rc<Cell<bool>>)>,
    acquired: bool,
}

impl<T> RwLock<T> {
    /// Creates a reader-writer lock containing `value`.
    pub fn new(value: T) -> Self {
        Self {
            readers: Cell::new(0),
            writer: Cell::new(false),
            next_waiter_id: Cell::new(0),
            waiters: RefCell::new(VecDeque::new()),
            _not_send_sync: PhantomData,
            value: UnsafeCell::new(value),
        }
    }

    /// Consumes the lock and returns the protected value.
    pub fn into_inner(self) -> T {
        self.value.into_inner()
    }
}

impl<T: ?Sized> RwLock<T> {
    /// Waits until read access is available and returns a read guard.
    ///
    /// Readers are admitted immediately only when no writer holds the lock and
    /// no waiter is queued. Otherwise, the request waits in FIFO order.
    pub fn read(&self) -> RwLockReadFuture<'_, T> {
        RwLockReadFuture::new(self)
    }

    /// Waits until write access is available and returns a write guard.
    ///
    /// Writers require exclusive access and are served in FIFO order with
    /// readers from the same waiter queue.
    pub fn write(&self) -> RwLockWriteFuture<'_, T> {
        RwLockWriteFuture::new(self)
    }

    /// Attempts to acquire read access without waiting.
    ///
    /// Returns [`None`] if a writer holds the lock or if queued waiters should
    /// acquire it first.
    pub fn try_read(&self) -> Option<RwLockReadGuard<'_, T>> {
        if self.writer.get() || !self.waiters.borrow().is_empty() {
            return None;
        }

        self.readers.set(self.readers.get() + 1);
        Some(RwLockReadGuard {
            lock: self,
            _not_send_sync: PhantomData,
        })
    }

    /// Attempts to acquire write access without waiting.
    ///
    /// Returns [`None`] if the lock is currently held or if queued waiters
    /// should acquire it first.
    pub fn try_write(&self) -> Option<RwLockWriteGuard<'_, T>> {
        if self.writer.get() || self.readers.get() > 0 || !self.waiters.borrow().is_empty() {
            return None;
        }

        self.writer.set(true);
        Some(RwLockWriteGuard {
            lock: self,
            _not_send_sync: PhantomData,
        })
    }

    /// Returns a mutable reference to the protected value.
    pub fn get_mut(&mut self) -> &mut T {
        self.value.get_mut()
    }

    fn allocate_waiter_id(&self) -> usize {
        let id = self.next_waiter_id.get();
        self.next_waiter_id.set(id.wrapping_add(1));
        id
    }

    fn remove_waiter(&self, id: usize) {
        let mut waiters = self.waiters.borrow_mut();
        if let Some(index) = waiters.iter().position(|waiter| waiter.id == id) {
            waiters.remove(index);
        }
    }

    fn release_reader(&self) {
        let readers = self.readers.get() - 1;
        self.readers.set(readers);
        if readers == 0 {
            self.release_to_next_waiters();
        }
    }

    fn release_writer(&self) {
        self.writer.set(false);
        self.release_to_next_waiters();
    }

    fn release_to_next_waiters(&self) {
        if self.writer.get() || self.readers.get() > 0 {
            return;
        }

        let mut wake = Vec::new();
        {
            let mut waiters = self.waiters.borrow_mut();
            let Some(front) = waiters.front() else {
                return;
            };

            match front.kind {
                WaiterKind::Write => {
                    let waiter = waiters.pop_front().expect("front waiter should exist");
                    waiter.selected.set(true);
                    self.writer.set(true);
                    wake.push(waiter.waker);
                }
                WaiterKind::Read => {
                    while waiters
                        .front()
                        .is_some_and(|waiter| waiter.kind == WaiterKind::Read)
                    {
                        let waiter = waiters.pop_front().expect("front waiter should exist");
                        waiter.selected.set(true);
                        self.readers.set(self.readers.get() + 1);
                        wake.push(waiter.waker);
                    }
                }
            }
        }

        for waker in wake {
            waker.wake();
        }
    }
}

impl<T: ?Sized> Deref for RwLockReadGuard<'_, T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        // SAFETY: read guards are created only after a reader slot is acquired.
        // Writers are excluded while any reader slot is held. The lock is
        // single-threaded and `!Send`/`!Sync`, so no atomic synchronization is
        // required for shared access.
        unsafe { &*self.lock.value.get() }
    }
}

impl<T: ?Sized> Drop for RwLockReadGuard<'_, T> {
    fn drop(&mut self) {
        self.lock.release_reader();
    }
}

impl<T: ?Sized> Deref for RwLockWriteGuard<'_, T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        // SAFETY: write guards are created only after exclusive access is
        // acquired. No reader or other writer can be admitted until this guard
        // is dropped.
        unsafe { &*self.lock.value.get() }
    }
}

impl<T: ?Sized> DerefMut for RwLockWriteGuard<'_, T> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        // SAFETY: this write guard has exclusive access to the protected value.
        unsafe { &mut *self.lock.value.get() }
    }
}

impl<T: ?Sized> Drop for RwLockWriteGuard<'_, T> {
    fn drop(&mut self) {
        self.lock.release_writer();
    }
}

impl<'a, T: ?Sized> RwLockReadFuture<'a, T> {
    fn new(lock: &'a RwLock<T>) -> Self {
        Self {
            lock,
            waiter: None,
            acquired: false,
        }
    }
}

impl<'a, T: ?Sized> Future for RwLockReadFuture<'a, T> {
    type Output = RwLockReadGuard<'a, T>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        if self
            .waiter
            .as_ref()
            .is_some_and(|(_, selected)| selected.get())
        {
            self.acquired = true;
            return Poll::Ready(RwLockReadGuard {
                lock: self.lock,
                _not_send_sync: PhantomData,
            });
        }

        if self.waiter.is_none() && !self.lock.writer.get() && self.lock.waiters.borrow().is_empty()
        {
            self.lock.readers.set(self.lock.readers.get() + 1);
            self.acquired = true;
            return Poll::Ready(RwLockReadGuard {
                lock: self.lock,
                _not_send_sync: PhantomData,
            });
        }

        if let Some((id, _)) = &self.waiter {
            if let Some(waiter) = self
                .lock
                .waiters
                .borrow_mut()
                .iter_mut()
                .find(|waiter| waiter.id == *id)
            {
                waiter.waker = cx.waker().clone();
            }
        } else {
            let id = self.lock.allocate_waiter_id();
            let selected = Rc::new(Cell::new(false));
            self.lock.waiters.borrow_mut().push_back(Waiter {
                id,
                kind: WaiterKind::Read,
                selected: Rc::clone(&selected),
                waker: cx.waker().clone(),
            });
            self.waiter = Some((id, selected));
        }

        Poll::Pending
    }
}

impl<T: ?Sized> Drop for RwLockReadFuture<'_, T> {
    fn drop(&mut self) {
        let Some((id, selected)) = &self.waiter else {
            return;
        };

        if self.acquired {
            return;
        }

        if selected.get() {
            self.lock.release_reader();
        } else {
            self.lock.remove_waiter(*id);
        }
    }
}

impl<'a, T: ?Sized> RwLockWriteFuture<'a, T> {
    fn new(lock: &'a RwLock<T>) -> Self {
        Self {
            lock,
            waiter: None,
            acquired: false,
        }
    }
}

impl<'a, T: ?Sized> Future for RwLockWriteFuture<'a, T> {
    type Output = RwLockWriteGuard<'a, T>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        if self
            .waiter
            .as_ref()
            .is_some_and(|(_, selected)| selected.get())
        {
            self.acquired = true;
            return Poll::Ready(RwLockWriteGuard {
                lock: self.lock,
                _not_send_sync: PhantomData,
            });
        }

        if self.waiter.is_none()
            && !self.lock.writer.get()
            && self.lock.readers.get() == 0
            && self.lock.waiters.borrow().is_empty()
        {
            self.lock.writer.set(true);
            self.acquired = true;
            return Poll::Ready(RwLockWriteGuard {
                lock: self.lock,
                _not_send_sync: PhantomData,
            });
        }

        if let Some((id, _)) = &self.waiter {
            if let Some(waiter) = self
                .lock
                .waiters
                .borrow_mut()
                .iter_mut()
                .find(|waiter| waiter.id == *id)
            {
                waiter.waker = cx.waker().clone();
            }
        } else {
            let id = self.lock.allocate_waiter_id();
            let selected = Rc::new(Cell::new(false));
            self.lock.waiters.borrow_mut().push_back(Waiter {
                id,
                kind: WaiterKind::Write,
                selected: Rc::clone(&selected),
                waker: cx.waker().clone(),
            });
            self.waiter = Some((id, selected));
        }

        Poll::Pending
    }
}

impl<T: ?Sized> Drop for RwLockWriteFuture<'_, T> {
    fn drop(&mut self) {
        let Some((id, selected)) = &self.waiter else {
            return;
        };

        if self.acquired {
            return;
        }

        if selected.get() {
            self.lock.release_writer();
        } else {
            self.lock.remove_waiter(*id);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::cell::RefCell;

    use crate::{run, spawn, yield_now};

    #[test]
    fn multiple_concurrent_readers_are_allowed() {
        let lock = Rc::new(RwLock::new(7));
        let active = Rc::new(Cell::new(0));
        let max_active = Rc::new(Cell::new(0));
        let observed = Rc::new(Cell::new(0));

        for _ in 0..2 {
            spawn({
                let lock = Rc::clone(&lock);
                let active = Rc::clone(&active);
                let max_active = Rc::clone(&max_active);
                let observed = Rc::clone(&observed);
                async move {
                    let guard = lock.read().await;
                    active.set(active.get() + 1);
                    max_active.set(max_active.get().max(active.get()));
                    observed.set(observed.get() + *guard);
                    yield_now().await;
                    active.set(active.get() - 1);
                }
            });
        }

        run();

        assert_eq!(observed.get(), 14);
        assert_eq!(max_active.get(), 2);
    }

    #[test]
    fn writer_excludes_readers() {
        let lock = Rc::new(RwLock::new(0));
        let order = Rc::new(RefCell::new(Vec::new()));

        spawn({
            let lock = Rc::clone(&lock);
            let order = Rc::clone(&order);
            async move {
                let mut guard = lock.write().await;
                order.borrow_mut().push(1);
                *guard = 5;
                yield_now().await;
                order.borrow_mut().push(10);
            }
        });

        spawn({
            let lock = Rc::clone(&lock);
            let order = Rc::clone(&order);
            async move {
                let guard = lock.read().await;
                order.borrow_mut().push(*guard);
            }
        });

        run();

        assert_eq!(&*order.borrow(), &[1, 10, 5]);
    }

    #[test]
    fn fifo_writer_blocks_later_reader() {
        let lock = Rc::new(RwLock::new(()));
        let order = Rc::new(RefCell::new(Vec::new()));

        spawn({
            let lock = Rc::clone(&lock);
            let order = Rc::clone(&order);
            async move {
                let _guard = lock.read().await;
                order.borrow_mut().push(1);
                yield_now().await;
                order.borrow_mut().push(10);
            }
        });

        spawn({
            let lock = Rc::clone(&lock);
            let order = Rc::clone(&order);
            async move {
                let _guard = lock.write().await;
                order.borrow_mut().push(2);
                yield_now().await;
                order.borrow_mut().push(20);
            }
        });

        spawn({
            let lock = Rc::clone(&lock);
            let order = Rc::clone(&order);
            async move {
                let _guard = lock.read().await;
                order.borrow_mut().push(3);
            }
        });

        run();

        assert_eq!(&*order.borrow(), &[1, 10, 2, 20, 3]);
    }

    #[test]
    fn try_read_and_try_write_report_contention() {
        let mut lock = RwLock::new(1);
        *lock.get_mut() = 2;
        assert_eq!(lock.into_inner(), 2);

        let lock = RwLock::new(1);
        let read1 = lock.try_read().expect("first reader should acquire");
        let read2 = lock.try_read().expect("second reader should acquire");
        assert!(lock.try_write().is_none());
        drop(read1);
        drop(read2);

        let mut write = lock.try_write().expect("writer should acquire");
        *write = 3;
        assert!(lock.try_read().is_none());
        assert!(lock.try_write().is_none());
        drop(write);

        assert_eq!(
            *lock.try_read().expect("reader should acquire after drop"),
            3
        );
    }

    #[test]
    fn dropping_guard_hands_off_to_next_waiter() {
        let lock = Rc::new(RwLock::new(0));
        let observed = Rc::new(Cell::new(0));

        spawn({
            let lock = Rc::clone(&lock);
            async move {
                let mut guard = lock.write().await;
                *guard = 1;
                yield_now().await;
            }
        });

        spawn({
            let lock = Rc::clone(&lock);
            let observed = Rc::clone(&observed);
            async move {
                let guard = lock.read().await;
                observed.set(*guard);
            }
        });

        run();

        assert_eq!(observed.get(), 1);
    }

    #[test]
    fn dropping_writer_wakes_consecutive_readers() {
        let lock = Rc::new(RwLock::new(()));
        let active = Rc::new(Cell::new(0));
        let max_active = Rc::new(Cell::new(0));

        spawn({
            let lock = Rc::clone(&lock);
            async move {
                let _guard = lock.write().await;
                yield_now().await;
            }
        });

        for _ in 0..2 {
            spawn({
                let lock = Rc::clone(&lock);
                let active = Rc::clone(&active);
                let max_active = Rc::clone(&max_active);
                async move {
                    let _guard = lock.read().await;
                    active.set(active.get() + 1);
                    max_active.set(max_active.get().max(active.get()));
                    yield_now().await;
                    active.set(active.get() - 1);
                }
            });
        }

        run();

        assert_eq!(max_active.get(), 2);
    }

    #[test]
    fn queued_waiters_block_try_read_fast_path() {
        let lock = Rc::new(RwLock::new(()));
        let try_read_failed = Rc::new(Cell::new(false));

        spawn({
            let lock = Rc::clone(&lock);
            let try_read_failed = Rc::clone(&try_read_failed);
            async move {
                let _guard = lock.read().await;
                spawn({
                    let lock = Rc::clone(&lock);
                    async move {
                        let _guard = lock.write().await;
                    }
                });
                yield_now().await;
                try_read_failed.set(lock.try_read().is_none());
            }
        });

        run();

        assert!(try_read_failed.get());
    }

    #[test]
    fn lock_is_not_send_or_sync_by_design() {
        // `RwLock` contains `PhantomData<Rc<()>>`, matching the other sync
        // primitives and documenting the intended `!Send`/`!Sync` auto-traits.
        let _lock = RwLock::new(());
    }
}
