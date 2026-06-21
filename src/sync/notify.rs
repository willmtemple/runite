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

/// A single-threaded async notification primitive.
///
/// `notify_one` stores one permit when no task is waiting; the next
/// [`Notify::notified`] call consumes it immediately. `notify_waiters` wakes all
/// current waiters and does not create a stored permit.
pub struct Notify {
    permit: Cell<bool>,
    next_waiter_id: Cell<usize>,
    waiters: RefCell<VecDeque<Waiter>>,
    _not_send_sync: PhantomData<Rc<()>>,
}

impl Notify {
    pub fn new() -> Self {
        Self {
            permit: Cell::new(false),
            next_waiter_id: Cell::new(0),
            waiters: RefCell::new(VecDeque::new()),
            _not_send_sync: PhantomData,
        }
    }

    pub async fn notified(&self) {
        Notified::new(self).await
    }

    pub fn notify_one(&self) {
        if let Some(waiter) = self.waiters.borrow_mut().pop_front() {
            waiter.selected.set(true);
            waiter.waker.wake();
        } else {
            self.permit.set(true);
        }
    }

    pub fn notify_waiters(&self) {
        for waiter in self.waiters.borrow_mut().drain(..) {
            waiter.selected.set(true);
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
    waiter: Option<(usize, Rc<Cell<bool>>)>,
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
            .is_some_and(|(_, selected)| selected.get())
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
            let selected = Rc::new(Cell::new(false));
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

        if let Some((id, selected)) = &self.waiter
            && !selected.get()
        {
            self.notify.remove_waiter(*id);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::cell::RefCell;

    use crate::platform::current::runtime::{queue_future, run, yield_now};

    #[test]
    fn fast_path_consumes_stored_permit() {
        let notify = Rc::new(Notify::new());
        let observed = Rc::new(Cell::new(false));

        notify.notify_one();
        queue_future({
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
            queue_future({
                let notify = Rc::clone(&notify);
                let order = Rc::clone(&order);
                async move {
                    notify.notified().await;
                    order.borrow_mut().push(id);
                }
            });
        }

        queue_future({
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
}
