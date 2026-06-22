//! Local future task plumbing: `FutureTask`, `JoinState`, and the waker
//! vtable used to schedule `queue_future` continuations.

use std::cell::{Cell, RefCell};
use std::rc::{Rc, Weak};
use std::task::{Context, Poll, RawWaker, RawWakerVTable, Waker};

use super::LocalBoxFuture;
use super::state::with_installed_thread;
use crate::task::JoinError;

pub(crate) struct FutureTask {
    pub(crate) future: RefCell<Option<LocalBoxFuture>>,
    pub(crate) queued: Cell<bool>,
    pub(crate) shared: Rc<TaskShared>,
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

        // An abort that landed while this task sat in the microtask queue has
        // already taken the future; nothing left to poll.
        let Some(mut future) = self.future.borrow_mut().take() else {
            return;
        };

        let waker = self.waker();
        let mut context = Context::from_waker(&waker);
        match future.as_mut().poll(&mut context) {
            Poll::Ready(()) => {}
            Poll::Pending => {
                // If the task aborted itself during this poll (e.g. it holds
                // its own `AbortHandle`), drop the future instead of restoring
                // it so it is never polled again.
                if self.shared.is_aborted() {
                    drop(future);
                } else {
                    *self.future.borrow_mut() = Some(future);
                }
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

/// Tracks the lifecycle of a queued future independently of its output type so
/// that the non-generic [`AbortHandle`](super::handles::AbortHandle) can drive
/// cancellation without knowing `T`.
#[derive(Clone, Copy, PartialEq, Eq)]
enum TaskState {
    /// The task is still queued or running.
    Running,
    /// The task finished and its output is available to the joiner.
    Finished,
    /// The task was aborted before completion.
    Aborted,
}

/// Type-erased, shared cancellation state for a queued future.
///
/// Held by the [`FutureTask`], the [`JoinState`], and any
/// [`AbortHandle`](super::handles::AbortHandle)s. Aborting drops the task's
/// future (which cascades `Drop` into any in-flight driver operations, so they
/// are cancelled) and wakes the joiner with [`JoinError::Aborted`].
pub(crate) struct TaskShared {
    task: RefCell<Weak<FutureTask>>,
    join_waker: RefCell<Option<Waker>>,
    state: Cell<TaskState>,
}

impl TaskShared {
    pub(crate) fn new() -> Self {
        Self {
            task: RefCell::new(Weak::new()),
            join_waker: RefCell::new(None),
            state: Cell::new(TaskState::Running),
        }
    }

    /// Links this shared state to its owning task. Called once, immediately
    /// after the task is constructed.
    pub(crate) fn set_task(&self, task: &Rc<FutureTask>) {
        *self.task.borrow_mut() = Rc::downgrade(task);
    }

    /// Returns `true` once the task has completed or been aborted.
    pub(crate) fn is_finished(&self) -> bool {
        !matches!(self.state.get(), TaskState::Running)
    }

    fn is_aborted(&self) -> bool {
        matches!(self.state.get(), TaskState::Aborted)
    }

    /// Aborts the task: drops its future so it is never polled again and wakes
    /// the joiner with [`JoinError::Aborted`]. A no-op if the task already
    /// finished or was aborted.
    pub(crate) fn abort(&self) {
        if !matches!(self.state.get(), TaskState::Running) {
            return;
        }
        self.state.set(TaskState::Aborted);

        // Dropping the future cancels any in-flight driver operations it is
        // parked on via their `Drop` impls. If the task is mid-poll (self
        // abort) the future is on the stack and this take is a no-op;
        // `FutureTask::poll` then drops it instead of restoring it.
        if let Some(task) = self.task.borrow().upgrade() {
            let _ = task.future.borrow_mut().take();
        }

        if let Some(waker) = self.join_waker.borrow_mut().take() {
            waker.wake();
        }
    }
}

pub(crate) struct JoinState<T> {
    pub(crate) shared: Rc<TaskShared>,
    result: RefCell<Option<T>>,
}

impl<T> JoinState<T> {
    pub(crate) fn new(shared: Rc<TaskShared>) -> Self {
        Self {
            shared,
            result: RefCell::new(None),
        }
    }

    pub(crate) fn complete(&self, value: T) {
        // A task aborted between its final poll and this call must not deliver
        // a value; the joiner already (or will) observe `JoinError::Aborted`.
        if !matches!(self.shared.state.get(), TaskState::Running) {
            return;
        }
        *self.result.borrow_mut() = Some(value);
        self.shared.state.set(TaskState::Finished);

        if let Some(waker) = self.shared.join_waker.borrow_mut().take() {
            waker.wake();
        }
    }

    pub(crate) fn poll(&self, cx: &mut Context<'_>) -> Poll<Result<T, JoinError>> {
        match self.shared.state.get() {
            TaskState::Finished => Poll::Ready(Ok(self
                .result
                .borrow_mut()
                .take()
                .expect("join handle polled after completion"))),
            TaskState::Aborted => Poll::Ready(Err(JoinError::Aborted)),
            TaskState::Running => {
                *self.shared.join_waker.borrow_mut() = Some(cx.waker().clone());
                Poll::Pending
            }
        }
    }
}
