use std::cell::{Cell, RefCell, UnsafeCell};
use std::collections::VecDeque;
use std::future::Future;
use std::marker::PhantomData;
use std::ops::{Deref, DerefMut};
use std::pin::Pin;
use std::rc::Rc;
use std::task::{Context, Poll, Waker};

struct Waiter {
    id: usize,
    selected: Rc<Cell<bool>>,
    waker: Waker,
}

/// A single-threaded async mutex.
///
/// This mutex is intentionally `!Send` and `!Sync`: it is only for tasks that
/// remain on one runite runtime thread.
///
/// # Examples
///
/// ```
/// use std::cell::Cell;
/// use std::rc::Rc;
///
/// use runite::sync::Mutex;
///
/// let mutex = Rc::new(Mutex::new(1));
/// let observed = Rc::new(Cell::new(0));
///
/// runite::spawn({
///     let mutex = Rc::clone(&mutex);
///     let observed = Rc::clone(&observed);
///     async move {
///         let mut guard = mutex.lock().await;
///         *guard += 41;
///         observed.set(*guard);
///     }
/// });
///
/// runite::run();
///
/// assert_eq!(observed.get(), 42);
/// assert_eq!(*mutex.try_lock().expect("mutex should be unlocked"), 42);
/// ```
pub struct Mutex<T: ?Sized> {
    locked: Cell<bool>,
    next_waiter_id: Cell<usize>,
    waiters: RefCell<VecDeque<Waiter>>,
    _not_send_sync: PhantomData<Rc<()>>,
    value: UnsafeCell<T>,
}

/// Guard returned by [`Mutex::lock`] and [`Mutex::try_lock`].
///
/// The guard dereferences to the protected value and releases the mutex when it
/// is dropped.
pub struct MutexGuard<'a, T: ?Sized> {
    mutex: &'a Mutex<T>,
    _not_send_sync: PhantomData<Rc<()>>,
}

impl<T> Mutex<T> {
    /// Creates a mutex containing `value`.
    pub fn new(value: T) -> Self {
        Self {
            locked: Cell::new(false),
            next_waiter_id: Cell::new(0),
            waiters: RefCell::new(VecDeque::new()),
            _not_send_sync: PhantomData,
            value: UnsafeCell::new(value),
        }
    }
}

impl<T: ?Sized> Mutex<T> {
    /// Waits until the mutex is available and returns a guard.
    ///
    /// Waiters are woken in FIFO order. Dropping the returned [`MutexGuard`]
    /// releases the mutex to the next waiter, if any.
    pub async fn lock(&self) -> MutexGuard<'_, T> {
        LockFuture::new(self).await
    }

    /// Attempts to acquire the mutex without waiting.
    ///
    /// Returns [`None`] if the mutex is currently locked or if queued waiters
    /// should acquire it first.
    pub fn try_lock(&self) -> Option<MutexGuard<'_, T>> {
        if self.locked.get() || !self.waiters.borrow().is_empty() {
            return None;
        }

        self.locked.set(true);
        Some(MutexGuard {
            mutex: self,
            _not_send_sync: PhantomData,
        })
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

    fn release_to_next_waiter(&self) {
        if let Some(waiter) = self.waiters.borrow_mut().pop_front() {
            waiter.selected.set(true);
            self.locked.set(true);
            waiter.waker.wake();
        } else {
            self.locked.set(false);
        }
    }
}

impl<T: ?Sized> Deref for MutexGuard<'_, T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        // SAFETY: `Mutex` only exposes `value` through guards. A guard is
        // created only when the locked flag is acquired or when a dropped guard
        // hands ownership to exactly one queued waiter. The runtime is
        // single-threaded and the type is `!Send`/`!Sync`, so no atomic
        // synchronization is required to uphold shared-reference validity.
        unsafe { &*self.mutex.value.get() }
    }
}

impl<T: ?Sized> DerefMut for MutexGuard<'_, T> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        // SAFETY: while this guard exists, no other guard can be constructed for
        // this mutex. The locked flag is released only from this guard's `Drop`.
        unsafe { &mut *self.mutex.value.get() }
    }
}

impl<T: ?Sized> Drop for MutexGuard<'_, T> {
    fn drop(&mut self) {
        self.mutex.release_to_next_waiter();
    }
}

struct LockFuture<'a, T: ?Sized> {
    mutex: &'a Mutex<T>,
    waiter: Option<(usize, Rc<Cell<bool>>)>,
    acquired: bool,
}

impl<'a, T: ?Sized> LockFuture<'a, T> {
    fn new(mutex: &'a Mutex<T>) -> Self {
        Self {
            mutex,
            waiter: None,
            acquired: false,
        }
    }
}

impl<'a, T: ?Sized> Future for LockFuture<'a, T> {
    type Output = MutexGuard<'a, T>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        if self
            .waiter
            .as_ref()
            .is_some_and(|(_, selected)| selected.get())
        {
            self.acquired = true;
            return Poll::Ready(MutexGuard {
                mutex: self.mutex,
                _not_send_sync: PhantomData,
            });
        }

        if self.waiter.is_none()
            && !self.mutex.locked.get()
            && self.mutex.waiters.borrow().is_empty()
        {
            self.mutex.locked.set(true);
            self.acquired = true;
            return Poll::Ready(MutexGuard {
                mutex: self.mutex,
                _not_send_sync: PhantomData,
            });
        }

        if let Some((id, _)) = &self.waiter {
            if let Some(waiter) = self
                .mutex
                .waiters
                .borrow_mut()
                .iter_mut()
                .find(|waiter| waiter.id == *id)
            {
                waiter.waker = cx.waker().clone();
            }
        } else {
            let id = self.mutex.allocate_waiter_id();
            let selected = Rc::new(Cell::new(false));
            self.mutex.waiters.borrow_mut().push_back(Waiter {
                id,
                selected: Rc::clone(&selected),
                waker: cx.waker().clone(),
            });
            self.waiter = Some((id, selected));
        }

        Poll::Pending
    }
}

impl<T: ?Sized> Drop for LockFuture<'_, T> {
    fn drop(&mut self) {
        let Some((id, selected)) = &self.waiter else {
            return;
        };

        if self.acquired {
            return;
        }

        if selected.get() {
            self.mutex.release_to_next_waiter();
        } else {
            self.mutex.remove_waiter(*id);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::cell::RefCell;

    use crate::{run, spawn, yield_now};

    #[test]
    fn fast_path_acquires_and_releases() {
        let mutex = Rc::new(Mutex::new(1));
        let observed = Rc::new(Cell::new(0));

        spawn({
            let mutex = Rc::clone(&mutex);
            let observed = Rc::clone(&observed);
            async move {
                let mut guard = mutex.lock().await;
                *guard += 1;
                observed.set(*guard);
            }
        });

        run();

        assert_eq!(observed.get(), 2);
        assert_eq!(*mutex.try_lock().unwrap(), 2);
    }

    #[test]
    fn contention_is_fifo_and_exclusive() {
        let mutex = Rc::new(Mutex::new(()));
        let order = Rc::new(RefCell::new(Vec::new()));

        spawn({
            let mutex = Rc::clone(&mutex);
            let order = Rc::clone(&order);
            async move {
                let _guard = mutex.lock().await;
                order.borrow_mut().push(1);
                yield_now().await;
                order.borrow_mut().push(10);
            }
        });

        spawn({
            let mutex = Rc::clone(&mutex);
            let order = Rc::clone(&order);
            async move {
                let _guard = mutex.lock().await;
                order.borrow_mut().push(2);
            }
        });

        run();

        assert_eq!(&*order.borrow(), &[1, 10, 2]);
    }
}
