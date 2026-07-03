//! Local future task plumbing: `FutureTask`, `JoinState`, and the `Send + Sync`
//! waker used to schedule `spawn` continuations.
//!
//! # Why the waker is `Arc`-based
//!
//! `std::task::Waker` is `Send + Sync`: a leaf future may hand `cx.waker()` to
//! another thread and wake it from there, entirely in safe code (channels,
//! timer crates, and `futures` combinators all do this). The waker payload must
//! therefore be thread-safe, even though the [`FutureTask`] it ultimately
//! reschedules is `!Send` and pinned to its owning runtime thread.
//!
//! A waker cannot hold the `Rc<FutureTask>` directly — cloning or dropping that
//! `Rc` from another thread would race its non-atomic refcount (UB), and waking
//! it could schedule the `!Send` future onto the wrong thread. Instead the waker
//! holds only a [`ThreadHandle`] (which is `Send + Sync`) and a numeric task
//! [`id`](TaskWaker::id). Waking looks the task up in the owner thread's
//! registry ([`ThreadState::tasks`](super::state::ThreadState)): a same-thread
//! wake schedules it directly as a microtask, a cross-thread wake routes through
//! the owner's macrotask queue and is resolved on the owner thread. A wake that
//! arrives after the task has completed finds no registry entry and is a no-op.

use std::cell::{Cell, RefCell};
use std::rc::{Rc, Weak};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::task::{Context, Poll, RawWaker, RawWakerVTable, Waker};

use super::LocalBoxFuture;
use super::handles::{QueueError, ThreadHandle};
use super::state::{try_with_installed_thread, with_installed_thread};
use crate::task::JoinError;

pub(crate) struct FutureTask {
    pub(crate) future: RefCell<Option<LocalBoxFuture>>,
    pub(crate) queued: Cell<bool>,
    pub(crate) shared: Rc<TaskShared>,
    /// Registry key for this task on its owning runtime thread. Wakers carry
    /// this id (not an `Rc`) so they can stay `Send + Sync`.
    pub(crate) id: u64,
    /// Pre-built `Send + Sync` waker for this task. Cloned (an atomic refcount
    /// bump) whenever a leaf future stores `cx.waker()`; borrowed directly for
    /// the poll itself, so the poll hot path allocates nothing.
    waker: Waker,
}

impl FutureTask {
    /// Builds a task and its waker. `owner` and `id` identify the task in the
    /// owning thread's registry so a wake from any thread can reach it.
    pub(crate) fn new(
        future: LocalBoxFuture,
        shared: Rc<TaskShared>,
        id: u64,
        owner: ThreadHandle,
    ) -> Rc<Self> {
        let waker_data = Arc::new(TaskWaker {
            owner,
            id,
            scheduled: AtomicBool::new(false),
        });
        Rc::new(Self {
            future: RefCell::new(Some(future)),
            queued: Cell::new(false),
            shared,
            id,
            waker: TaskWaker::into_waker(waker_data),
        })
    }

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

        let mut context = Context::from_waker(&self.waker);
        match future.as_mut().poll(&mut context) {
            Poll::Ready(()) => {
                // The task is done; drop it from the registry so its `Rc` is
                // released (the currently-executing microtask closure still
                // holds one, so the drop happens after this returns).
                deregister_task(self.id);
            }
            Poll::Pending => {
                // If the task aborted itself during this poll (e.g. it holds
                // its own `AbortHandle`), drop the future instead of restoring
                // it so it is never polled again. `abort` has already removed
                // it from the registry.
                if self.shared.is_aborted() {
                    drop(future);
                } else {
                    *self.future.borrow_mut() = Some(future);
                }
            }
        }
    }
}

/// The `Send + Sync` payload behind a task's [`Waker`].
///
/// Holds no reference to the `!Send` [`FutureTask`] — only the owning
/// [`ThreadHandle`] and the task's registry [`id`](Self::id) — so it may be
/// cloned, dropped, and woken from any thread soundly.
struct TaskWaker {
    owner: ThreadHandle,
    id: u64,
    /// Coalesces wakes: set when a schedule is pending and not yet consumed, so
    /// a burst of cross-thread wakes enqueues at most one macrotask onto the
    /// (bounded) remote queue rather than one per wake.
    scheduled: AtomicBool,
}

impl TaskWaker {
    fn into_waker(data: Arc<Self>) -> Waker {
        // SAFETY: the vtable below round-trips `Arc<TaskWaker>` through
        // `Arc::into_raw`/`Arc::from_raw`, preserving the strong count, and
        // every operation it performs is thread-safe (atomic refcounting,
        // atomic `scheduled`, and `ThreadHandle` which is `Send + Sync`).
        unsafe { Waker::from_raw(RawWaker::new(Arc::into_raw(data).cast::<()>(), &TASK_WAKER_VTABLE)) }
    }

    fn wake(self: &Arc<Self>) {
        // Coalesce: if a schedule is already pending, this wake folds into it.
        if self.scheduled.swap(true, Ordering::AcqRel) {
            return;
        }

        if self.owner.is_current() {
            // On the owner thread: schedule the poll directly as a microtask.
            // Consume the pending marker immediately — the schedule below (and
            // `FutureTask::queued`) provide the real dedup on this thread.
            self.scheduled.store(false, Ordering::Release);
            schedule_task_by_id(self.id);
            return;
        }

        // Cross-thread wake: hop to the owner thread and schedule there. The
        // closure captures only a `Send` `Arc<TaskWaker>` and the numeric id.
        let this = Arc::clone(self);
        let id = self.id;
        match self.owner.queue_macrotask(move || {
            this.scheduled.store(false, Ordering::Release);
            schedule_task_by_id(id);
        }) {
            Ok(()) => {}
            // Closed: the owner runtime has exited; nothing can be scheduled.
            // Full: the wake is dropped (see the reserved-capacity follow-up in
            // the release plan, task 2.3). Reset the marker either way so a
            // later wake can retry.
            Err(QueueError::Closed) | Err(QueueError::Full) => {
                self.scheduled.store(false, Ordering::Release);
            }
        }
    }
}

/// Looks the task up in the current thread's registry and schedules a poll.
/// A miss means the task already completed or was aborted — a no-op.
fn schedule_task_by_id(id: u64) {
    let task = with_installed_thread(|state| state.tasks.borrow().get(&id).map(Rc::clone));
    if let Some(task) = task {
        task.schedule();
    }
}

/// Removes a task from the current thread's registry, releasing the runtime's
/// strong reference to it. Best-effort: a no-op if no runtime is installed
/// (e.g. during thread teardown).
fn deregister_task(id: u64) {
    try_with_installed_thread(|state| {
        if let Some(state) = state {
            state.tasks.borrow_mut().remove(&id);
        }
    });
}

static TASK_WAKER_VTABLE: RawWakerVTable = RawWakerVTable::new(
    task_waker_clone,
    task_waker_wake,
    task_waker_wake_by_ref,
    task_waker_drop,
);

unsafe fn task_waker_clone(data: *const ()) -> RawWaker {
    // SAFETY: `data` was produced by `Arc::into_raw` for an `Arc<TaskWaker>` and
    // still owns a strong count. Reconstruct it, bump the count via clone, and
    // hand both the original and the clone back as raw pointers.
    let arc = unsafe { Arc::<TaskWaker>::from_raw(data.cast::<TaskWaker>()) };
    let cloned = Arc::clone(&arc);
    let _ = Arc::into_raw(arc);
    RawWaker::new(Arc::into_raw(cloned).cast::<()>(), &TASK_WAKER_VTABLE)
}

unsafe fn task_waker_wake(data: *const ()) {
    // SAFETY: consumes exactly the strong count encoded in this raw waker,
    // produced by `Arc::into_raw` for an `Arc<TaskWaker>`.
    let arc = unsafe { Arc::<TaskWaker>::from_raw(data.cast::<TaskWaker>()) };
    arc.wake();
}

unsafe fn task_waker_wake_by_ref(data: *const ()) {
    // SAFETY: borrows the raw waker's strong count by reconstructing the `Arc`,
    // then converts it back with `Arc::into_raw` so ownership stays with the
    // waker.
    let arc = unsafe { Arc::<TaskWaker>::from_raw(data.cast::<TaskWaker>()) };
    arc.wake();
    let _ = Arc::into_raw(arc);
}

unsafe fn task_waker_drop(data: *const ()) {
    // SAFETY: releases exactly the strong count stored by `into_waker` or
    // `task_waker_clone` with `Arc::into_raw`.
    drop(unsafe { Arc::<TaskWaker>::from_raw(data.cast::<TaskWaker>()) });
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
    /// Registry id of the owning task, mirrored here so `abort` can deregister
    /// without upgrading the weak reference.
    id: Cell<u64>,
}

impl TaskShared {
    pub(crate) fn new() -> Self {
        Self {
            task: RefCell::new(Weak::new()),
            join_waker: RefCell::new(None),
            state: Cell::new(TaskState::Running),
            id: Cell::new(0),
        }
    }

    /// Links this shared state to its owning task. Called once, immediately
    /// after the task is constructed.
    pub(crate) fn set_task(&self, task: &Rc<FutureTask>) {
        *self.task.borrow_mut() = Rc::downgrade(task);
        self.id.set(task.id);
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

        // Release the runtime's registry reference so an aborted task is not
        // retained (mid-poll self-abort still keeps it alive via the executing
        // microtask closure until that poll returns).
        deregister_task(self.id.get());

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
