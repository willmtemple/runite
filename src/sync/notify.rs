use std::cell::{Cell, RefCell};
use std::collections::VecDeque;
use std::future::Future;
use std::marker::PhantomData;
use std::pin::Pin;
use std::rc::Rc;
use std::task::{Context, Poll, Waker};

struct Waiter {
    id: usize,
    selected: Rc<Cell<Selection>>,
    waker: Waker,
}

/// How a waiter was selected, which determines what happens if the waiter's
/// future is dropped before it observes the wake.
#[derive(Clone, Copy, PartialEq, Eq)]
enum Selection {
    /// Still waiting; no notification delivered yet.
    Pending,
    /// Selected by [`Notify::notify_one`]. This carries a single transferable
    /// permit: if the future is dropped before completing, the notification is
    /// forwarded to the next waiter (or stored as a permit).
    One,
    /// Selected by [`Notify::notify_waiters`]. This is a broadcast wake with no
    /// stored permit: if the future is dropped before completing, nothing is
    /// forwarded.
    Waiters,
}

/// A single-threaded async notification primitive.
///
/// `notify_one` stores one permit when no task is waiting; the next
/// [`Notify::notified`] call consumes it immediately. `notify_waiters` wakes all
/// current waiters and does not create a stored permit.
///
/// # Differences from Tokio
///
/// This primitive is local to one runtime thread and has no public named
/// `Notified` future type. Like Tokio's `Notify`, it stores at most one permit
/// for `notify_one`, but wakeups are only for local tasks; use channels or
/// thread handles for cross-thread notification.
///
/// # Examples
///
/// ```
/// use std::cell::Cell;
/// use std::rc::Rc;
///
/// use runite::sync::Notify;
///
/// let notify = Rc::new(Notify::new());
/// let woke = Rc::new(Cell::new(false));
///
/// runite::spawn({
///     let notify = Rc::clone(&notify);
///     let woke = Rc::clone(&woke);
///     async move {
///         notify.notified().await;
///         woke.set(true);
///     }
/// });
///
/// runite::spawn({
///     let notify = Rc::clone(&notify);
///     async move {
///         runite::yield_now().await;
///         notify.notify_one();
///     }
/// });
///
/// runite::run();
///
/// assert!(woke.get());
/// ```
pub struct Notify {
    permit: Cell<bool>,
    next_waiter_id: Cell<usize>,
    waiters: RefCell<VecDeque<Waiter>>,
    _not_send_sync: PhantomData<Rc<()>>,
}

impl Notify {
    /// Creates a notification primitive with no stored permit.
    pub fn new() -> Self {
        Self {
            permit: Cell::new(false),
            next_waiter_id: Cell::new(0),
            waiters: RefCell::new(VecDeque::new()),
            _not_send_sync: PhantomData,
        }
    }

    /// Waits for a notification.
    ///
    /// If [`notify_one`](Self::notify_one) has already stored a permit, this
    /// returns immediately and consumes that permit.
    ///
    /// `notify_one` wakes the oldest waiter first (FIFO). If a future returned by
    /// this method is selected by `notify_one` but dropped before it completes,
    /// the notification is forwarded to the next waiter (or stored as a permit)
    /// rather than being lost. A future woken by [`notify_waiters`](Self::notify_waiters)
    /// is a broadcast wake and is not forwarded when dropped.
    pub async fn notified(&self) {
        Notified::new(self).await
    }

    /// Wakes one waiting task or stores one permit for the next waiter.
    ///
    /// The oldest waiter is woken first (FIFO). If that selected future is
    /// dropped before completing, the notification is forwarded to the next
    /// waiter or stored as the single permit.
    ///
    /// At most one permit is stored; repeated calls before a waiter arrives
    /// still allow only one future [`notified`](Self::notified) call to complete
    /// immediately.
    pub fn notify_one(&self) {
        if let Some(waiter) = self.waiters.borrow_mut().pop_front() {
            waiter.selected.set(Selection::One);
            waiter.waker.wake();
        } else {
            self.permit.set(true);
        }
    }

    /// Wakes all tasks that are currently waiting.
    ///
    /// This does not store a permit for future waiters. Broadcast wakeups are
    /// not forwarded if a selected `notified()` future is dropped before it
    /// completes.
    pub fn notify_waiters(&self) {
        for waiter in self.waiters.borrow_mut().drain(..) {
            waiter.selected.set(Selection::Waiters);
            waiter.waker.wake();
        }
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
}

impl Default for Notify {
    fn default() -> Self {
        Self::new()
    }
}

struct Notified<'a> {
    notify: &'a Notify,
    waiter: Option<(usize, Rc<Cell<Selection>>)>,
    done: bool,
}

impl<'a> Notified<'a> {
    fn new(notify: &'a Notify) -> Self {
        Self {
            notify,
            waiter: None,
            done: false,
        }
    }
}

impl Future for Notified<'_> {
    type Output = ();

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        if self
            .waiter
            .as_ref()
            .is_some_and(|(_, selected)| selected.get() != Selection::Pending)
        {
            self.done = true;
            return Poll::Ready(());
        }

        if self.waiter.is_none() && self.notify.permit.replace(false) {
            self.done = true;
            return Poll::Ready(());
        }

        if let Some((id, _)) = &self.waiter {
            if let Some(waiter) = self
                .notify
                .waiters
                .borrow_mut()
                .iter_mut()
                .find(|waiter| waiter.id == *id)
            {
                waiter.waker = cx.waker().clone();
            }
        } else {
            let id = self.notify.allocate_waiter_id();
            let selected = Rc::new(Cell::new(Selection::Pending));
            self.notify.waiters.borrow_mut().push_back(Waiter {
                id,
                selected: Rc::clone(&selected),
                waker: cx.waker().clone(),
            });
            self.waiter = Some((id, selected));
        }

        Poll::Pending
    }
}

impl Drop for Notified<'_> {
    fn drop(&mut self) {
        if self.done {
            return;
        }

        if let Some((id, selected)) = &self.waiter {
            match selected.get() {
                // Never notified: just remove ourselves from the queue.
                Selection::Pending => self.notify.remove_waiter(*id),
                // We held a transferable permit from `notify_one` but never
                // consumed it. Forward it so it is not lost.
                Selection::One => self.notify.notify_one(),
                // Broadcast wake from `notify_waiters`: nothing to forward.
                Selection::Waiters => {}
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::cell::RefCell;

    use crate::{run, spawn, yield_now};

    #[test]
    fn fast_path_consumes_stored_permit() {
        let notify = Rc::new(Notify::new());
        let observed = Rc::new(Cell::new(false));

        notify.notify_one();
        spawn({
            let notify = Rc::clone(&notify);
            let observed = Rc::clone(&observed);
            async move {
                notify.notified().await;
                observed.set(true);
            }
        });

        run();

        assert!(observed.get());
    }

    #[test]
    fn contention_wakes_waiters_fifo() {
        let notify = Rc::new(Notify::new());
        let order = Rc::new(RefCell::new(Vec::new()));

        for id in [1, 2] {
            spawn({
                let notify = Rc::clone(&notify);
                let order = Rc::clone(&order);
                async move {
                    notify.notified().await;
                    order.borrow_mut().push(id);
                }
            });
        }

        spawn({
            let notify = Rc::clone(&notify);
            async move {
                yield_now().await;
                notify.notify_one();
                yield_now().await;
                notify.notify_one();
            }
        });

        run();

        assert_eq!(&*order.borrow(), &[1, 2]);
    }

    #[test]
    fn dropped_notify_one_recipient_forwards_to_next_waiter() {
        use std::task::{Context, Waker};

        let notify = Notify::new();
        let waker = Waker::noop();
        let mut cx = Context::from_waker(waker);

        let mut first = Box::pin(notify.notified());
        let mut second = Box::pin(notify.notified());
        assert!(first.as_mut().poll(&mut cx).is_pending());
        assert!(second.as_mut().poll(&mut cx).is_pending());

        // Selects `first` (FIFO). Dropping it before it completes must forward
        // the notification to `second` instead of losing it.
        notify.notify_one();
        drop(first);

        assert!(second.as_mut().poll(&mut cx).is_ready());
    }

    #[test]
    fn dropped_notify_one_recipient_with_no_waiter_stores_permit() {
        use std::task::{Context, Waker};

        let notify = Notify::new();
        let waker = Waker::noop();
        let mut cx = Context::from_waker(waker);

        let mut only = Box::pin(notify.notified());
        assert!(only.as_mut().poll(&mut cx).is_pending());

        notify.notify_one();
        drop(only);

        // With no other waiter, the forwarded notification becomes a stored
        // permit that the next waiter consumes immediately.
        let mut next = Box::pin(notify.notified());
        assert!(next.as_mut().poll(&mut cx).is_ready());
    }

    #[test]
    fn dropped_broadcast_recipient_does_not_store_permit() {
        use std::task::{Context, Waker};

        let notify = Notify::new();
        let waker = Waker::noop();
        let mut cx = Context::from_waker(waker);

        let mut waiter = Box::pin(notify.notified());
        assert!(waiter.as_mut().poll(&mut cx).is_pending());

        // Broadcast wake. Dropping the recipient must not leave a stored permit.
        notify.notify_waiters();
        drop(waiter);

        let mut next = Box::pin(notify.notified());
        assert!(next.as_mut().poll(&mut cx).is_pending());
    }
}
