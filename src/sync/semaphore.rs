use std::cell::{Cell, RefCell};
use std::collections::VecDeque;
use std::future::Future;
use std::marker::PhantomData;
use std::pin::Pin;
use std::rc::Rc;
use std::task::{Context, Poll, Waker};

struct Waiter {
    id: usize,
    selected: Rc<Cell<bool>>,
    waker: Waker,
}

/// A single-threaded counting semaphore.
///
/// A semaphore holds a number of permits. Each successful acquire removes one
/// permit, and dropping the returned [`Permit`] releases it.
///
/// # Examples
///
/// ```
/// use std::cell::Cell;
/// use std::rc::Rc;
///
/// use runite::sync::Semaphore;
///
/// let semaphore = Rc::new(Semaphore::new(1));
/// let active = Rc::new(Cell::new(0));
/// let max_active = Rc::new(Cell::new(0));
/// let completed = Rc::new(Cell::new(0));
///
/// for _ in 0..2 {
///     runite::spawn({
///         let semaphore = Rc::clone(&semaphore);
///         let active = Rc::clone(&active);
///         let max_active = Rc::clone(&max_active);
///         let completed = Rc::clone(&completed);
///         async move {
///             let permit = semaphore.acquire().await;
///             active.set(active.get() + 1);
///             max_active.set(max_active.get().max(active.get()));
///
///             runite::yield_now().await;
///
///             active.set(active.get() - 1);
///             completed.set(completed.get() + 1);
///             drop(permit);
///         }
///     });
/// }
///
/// runite::run();
///
/// assert_eq!(completed.get(), 2);
/// assert_eq!(max_active.get(), 1);
/// assert!(semaphore.try_acquire().is_some());
/// ```
pub struct Semaphore {
    permits: Cell<usize>,
    next_waiter_id: Cell<usize>,
    waiters: RefCell<VecDeque<Waiter>>,
    _not_send_sync: PhantomData<Rc<()>>,
}

/// A permit returned by [`Semaphore::acquire`] and [`Semaphore::try_acquire`].
///
/// Dropping a permit releases it back to the semaphore or hands it directly to
/// the next queued waiter.
pub struct Permit<'a> {
    semaphore: &'a Semaphore,
    _not_send_sync: PhantomData<Rc<()>>,
}

impl Semaphore {
    /// Creates a semaphore with `permits` initially available.
    pub fn new(permits: usize) -> Self {
        Self {
            permits: Cell::new(permits),
            next_waiter_id: Cell::new(0),
            waiters: RefCell::new(VecDeque::new()),
            _not_send_sync: PhantomData,
        }
    }

    /// Waits until a permit is available and returns it.
    ///
    /// Waiters are woken in FIFO order. Dropping the returned [`Permit`]
    /// releases it.
    pub async fn acquire(&self) -> Permit<'_> {
        AcquireFuture::new(self).await
    }

    /// Attempts to acquire a permit without waiting.
    ///
    /// Returns [`None`] if no permit is available or if queued waiters should
    /// receive future permits first.
    pub fn try_acquire(&self) -> Option<Permit<'_>> {
        if !self.waiters.borrow().is_empty() {
            return None;
        }

        let permits = self.permits.get();
        if permits == 0 {
            return None;
        }

        self.permits.set(permits - 1);
        Some(Permit {
            semaphore: self,
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
            waiter.waker.wake();
        } else {
            self.permits.set(self.permits.get() + 1);
        }
    }
}

impl Drop for Permit<'_> {
    fn drop(&mut self) {
        self.semaphore.release_to_next_waiter();
    }
}

struct AcquireFuture<'a> {
    semaphore: &'a Semaphore,
    waiter: Option<(usize, Rc<Cell<bool>>)>,
    acquired: bool,
}

impl<'a> AcquireFuture<'a> {
    fn new(semaphore: &'a Semaphore) -> Self {
        Self {
            semaphore,
            waiter: None,
            acquired: false,
        }
    }
}

impl<'a> Future for AcquireFuture<'a> {
    type Output = Permit<'a>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        if self
            .waiter
            .as_ref()
            .is_some_and(|(_, selected)| selected.get())
        {
            self.acquired = true;
            return Poll::Ready(Permit {
                semaphore: self.semaphore,
                _not_send_sync: PhantomData,
            });
        }

        if self.waiter.is_none()
            && self.semaphore.waiters.borrow().is_empty()
            && self.semaphore.permits.get() > 0
        {
            self.semaphore.permits.set(self.semaphore.permits.get() - 1);
            self.acquired = true;
            return Poll::Ready(Permit {
                semaphore: self.semaphore,
                _not_send_sync: PhantomData,
            });
        }

        if let Some((id, _)) = &self.waiter {
            if let Some(waiter) = self
                .semaphore
                .waiters
                .borrow_mut()
                .iter_mut()
                .find(|waiter| waiter.id == *id)
            {
                waiter.waker = cx.waker().clone();
            }
        } else {
            let id = self.semaphore.allocate_waiter_id();
            let selected = Rc::new(Cell::new(false));
            self.semaphore.waiters.borrow_mut().push_back(Waiter {
                id,
                selected: Rc::clone(&selected),
                waker: cx.waker().clone(),
            });
            self.waiter = Some((id, selected));
        }

        Poll::Pending
    }
}

impl Drop for AcquireFuture<'_> {
    fn drop(&mut self) {
        let Some((id, selected)) = &self.waiter else {
            return;
        };

        if self.acquired {
            return;
        }

        if selected.get() {
            self.semaphore.release_to_next_waiter();
        } else {
            self.semaphore.remove_waiter(*id);
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
        let semaphore = Rc::new(Semaphore::new(1));
        assert!(semaphore.try_acquire().is_some());
        assert!(semaphore.try_acquire().is_some());

        let observed = Rc::new(Cell::new(false));
        spawn({
            let semaphore = Rc::clone(&semaphore);
            let observed = Rc::clone(&observed);
            async move {
                let _permit = semaphore.acquire().await;
                observed.set(true);
            }
        });

        run();

        assert!(observed.get());
    }

    #[test]
    fn contention_is_fifo() {
        let semaphore = Rc::new(Semaphore::new(1));
        let order = Rc::new(RefCell::new(Vec::new()));

        spawn({
            let semaphore = Rc::clone(&semaphore);
            let order = Rc::clone(&order);
            async move {
                let _permit = semaphore.acquire().await;
                order.borrow_mut().push(1);
                yield_now().await;
                order.borrow_mut().push(10);
            }
        });

        spawn({
            let semaphore = Rc::clone(&semaphore);
            let order = Rc::clone(&order);
            async move {
                let _permit = semaphore.acquire().await;
                order.borrow_mut().push(2);
            }
        });

        run();

        assert_eq!(&*order.borrow(), &[1, 10, 2]);
    }
}
