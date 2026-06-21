//! Local future task plumbing: `FutureTask`, `JoinState`, and the waker
//! vtable used to schedule `queue_future` continuations.

use std::cell::{Cell, RefCell};
use std::rc::Rc;
use std::task::{Context, Poll, RawWaker, RawWakerVTable, Waker};

use super::LocalBoxFuture;
use super::state::with_installed_thread;

pub(crate) struct FutureTask {
    pub(crate) future: RefCell<Option<LocalBoxFuture>>,
    pub(crate) queued: Cell<bool>,
}

impl FutureTask {
    /// Schedules a microtask that polls the future, deduplicating against
    /// wakes that arrive while the task is already pending.
    pub(crate) fn schedule(self: &Rc<Self>) {
        if self.queued.replace(true) {
            return;
        }

        let task = Rc::clone(self);
        with_installed_thread(|state| {
            state
                .local_microtasks
                .borrow_mut()
                .push_back(Box::new(move || task.poll()));
        });
    }

    fn poll(self: Rc<Self>) {
        self.queued.set(false);

        let Some(mut future) = self.future.borrow_mut().take() else {
            return;
        };

        let waker = self.waker();
        let mut context = Context::from_waker(&waker);
        match future.as_mut().poll(&mut context) {
            Poll::Ready(()) => {}
            Poll::Pending => {
                *self.future.borrow_mut() = Some(future);
            }
        }
    }

    fn waker(self: &Rc<Self>) -> Waker {
        // SAFETY: the vtable below preserves the `Rc<FutureTask>` invariants
        // round-tripping through `Rc::into_raw` / `Rc::from_raw`.
        unsafe {
            Waker::from_raw(RawWaker::new(
                Rc::into_raw(Rc::clone(self)).cast::<()>(),
                &FUTURE_TASK_WAKER_VTABLE,
            ))
        }
    }
}

static FUTURE_TASK_WAKER_VTABLE: RawWakerVTable = RawWakerVTable::new(
    future_task_clone,
    future_task_wake,
    future_task_wake_by_ref,
    future_task_drop,
);

unsafe fn future_task_clone(data: *const ()) -> RawWaker {
    // SAFETY: the raw waker data is created only by `FutureTask::waker` from
    // `Rc::into_raw(Rc<FutureTask>)`, so it is non-null, correctly aligned, and
    // still owns one strong count while the vtable callback runs.
    let task = unsafe { Rc::<FutureTask>::from_raw(data.cast::<FutureTask>()) };
    let clone = Rc::clone(&task);
    let _ = Rc::into_raw(task);
    RawWaker::new(Rc::into_raw(clone).cast::<()>(), &FUTURE_TASK_WAKER_VTABLE)
}

unsafe fn future_task_wake(data: *const ()) {
    // SAFETY: the `wake` callback consumes exactly the strong count encoded in
    // this raw waker data, which was produced by `Rc::into_raw` for
    // `Rc<FutureTask>`.
    let task = unsafe { Rc::<FutureTask>::from_raw(data.cast::<FutureTask>()) };
    task.schedule();
}

unsafe fn future_task_wake_by_ref(data: *const ()) {
    // SAFETY: `wake_by_ref` borrows the raw waker's strong count temporarily by
    // reconstructing the `Rc`, then converts it back with `Rc::into_raw` before
    // returning so ownership remains with the waker.
    let task = unsafe { Rc::<FutureTask>::from_raw(data.cast::<FutureTask>()) };
    task.schedule();
    let _ = Rc::into_raw(task);
}

unsafe fn future_task_drop(data: *const ()) {
    // SAFETY: dropping the raw waker must release exactly the strong count that
    // `FutureTask::waker` or `future_task_clone` stored with `Rc::into_raw`.
    drop(unsafe { Rc::<FutureTask>::from_raw(data.cast::<FutureTask>()) });
}

pub(crate) struct JoinState<T> {
    result: RefCell<Option<T>>,
    waker: RefCell<Option<Waker>>,
    ready: Cell<bool>,
}

impl<T> JoinState<T> {
    pub(crate) fn new() -> Self {
        Self {
            result: RefCell::new(None),
            waker: RefCell::new(None),
            ready: Cell::new(false),
        }
    }

    pub(crate) fn complete(&self, value: T) {
        *self.result.borrow_mut() = Some(value);
        self.ready.set(true);

        if let Some(waker) = self.waker.borrow_mut().take() {
            waker.wake();
        }
    }

    pub(crate) fn poll(&self, cx: &mut Context<'_>) -> Poll<T> {
        if self.ready.get() {
            return Poll::Ready(
                self.result
                    .borrow_mut()
                    .take()
                    .expect("join handle polled after completion"),
            );
        }

        *self.waker.borrow_mut() = Some(cx.waker().clone());
        Poll::Pending
    }
}
