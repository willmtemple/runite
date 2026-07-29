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
use super::handles::ThreadHandle;
use super::state::{
    describe_panic, mark_teardown_panicked, try_with_installed_thread, with_installed_thread,
};
use crate::task::JoinError;
use crate::trace_targets;

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
            // A wake arriving while a poll is already queued caused no new
            // work. Counted separately rather than folded into `task_wakes`,
            // so neither number lies: `task_wakes` stays a count of scheduled
            // polls, and the coalescing a consumer asked to see is visible
            // instead of being silently absent.
            with_installed_thread(|state| {
                super::state::RuntimeCounters::bump(&state.shared.counters.coalesced_wakes);
            });
            return;
        }
        with_installed_thread(|state| {
            super::state::RuntimeCounters::bump(&state.shared.counters.task_wakes);
            state.shared.ready_tasks.fetch_add(1, Ordering::AcqRel);
        });

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
        with_installed_thread(|state| {
            state.shared.ready_tasks.fetch_sub(1, Ordering::AcqRel);
        });

        // An abort that landed while this task sat in the microtask queue has
        // already taken the future; nothing left to poll.
        let Some(mut future) = self.future.borrow_mut().take() else {
            return;
        };

        with_installed_thread(|state| {
            super::state::RuntimeCounters::bump(&state.shared.counters.task_polls);
        });
        let mut context = Context::from_waker(&self.waker);
        // Isolate task panics: a future that unwinds must not tear down the
        // event loop that is polling it. Catch the unwind here, report it, and
        // resolve the joiner to `JoinError::Panicked` so awaiters are released
        // rather than hung. `AssertUnwindSafe` is sound because on unwind the
        // future is dropped (never polled again) and the only other state
        // touched — `TaskShared` — is moved to a terminal `Panicked` state.
        let outcome = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            future.as_mut().poll(&mut context)
        }));
        match outcome {
            Ok(Poll::Ready(())) => {
                // The task is done; drop it from the registry so its `Rc` is
                // released (the currently-executing microtask closure still
                // holds one, so the drop happens after this returns).
                deregister_task(self.id);
                drop_future_safely(future, self.id, "completion");
            }
            Ok(Poll::Pending) => {
                // If the task aborted itself during this poll (e.g. it holds
                // its own `AbortHandle`), drop the future instead of restoring
                // it so it is never polled again. `abort` has already removed
                // it from the registry.
                if self.shared.is_aborted() {
                    drop_future_safely(future, self.id, "abort");
                } else {
                    *self.future.borrow_mut() = Some(future);
                }
            }
            Err(payload) => {
                tracing::error!(
                    target: trace_targets::ASYNC,
                    event = "task_panicked",
                    task_id = self.id,
                    panic = describe_panic(&*payload),
                    "spawned task panicked; isolating and reporting JoinError::Panicked to the joiner",
                );
                // Commit the terminal state and release the registry reference
                // before dropping user future state. A `Drop` implementation
                // may itself panic; it must not skip terminal bookkeeping.
                let join_waker = self.shared.mark_panicked();
                deregister_task(self.id);
                wake_join_safely(join_waker, self.id, "panic");
                drop_future_safely(future, self.id, "panic");
            }
        }
    }
}

struct ShutdownCancellation {
    task_id: u64,
    future: Option<LocalBoxFuture>,
    join_waker: Option<Waker>,
}

/// Transitions every supplied task to shutdown-cancelled before invoking any
/// user code through a waker or future destructor.
///
/// Callers remove the tasks from the runtime registry first. The separate
/// prepare/finish phases ensure all task states and deregistration are
/// committed before one task's destructor can inspect or mutate another task.
pub(crate) fn cancel_tasks_for_shutdown(mut tasks: Vec<Rc<FutureTask>>) {
    tasks.sort_unstable_by_key(|task| task.id);
    let mut cancellations = tasks
        .into_iter()
        .filter_map(|task| {
            let join_waker = task.shared.mark_cancelled()?;
            if task.queued.replace(false) {
                with_installed_thread(|state| {
                    state.shared.ready_tasks.fetch_sub(1, Ordering::AcqRel);
                });
            }
            let future = task.future.borrow_mut().take();
            Some(ShutdownCancellation {
                task_id: task.id,
                future,
                join_waker,
            })
        })
        .collect::<Vec<_>>();

    for cancellation in &mut cancellations {
        wake_join_safely(
            cancellation.join_waker.take(),
            cancellation.task_id,
            "shutdown cancellation",
        );
    }
    for cancellation in cancellations {
        if let Some(future) = cancellation.future {
            drop_future_safely(future, cancellation.task_id, "shutdown cancellation");
        }
    }
}

fn wake_join_safely(waker: Option<Waker>, task_id: u64, terminal: &'static str) {
    let Some(waker) = waker else {
        return;
    };
    if let Err(payload) = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| waker.wake())) {
        mark_teardown_panicked();
        tracing::error!(
            target: trace_targets::ASYNC,
            event = "join_waker_panicked",
            task_id,
            terminal,
            panic = describe_panic(&*payload),
            "join waker panicked during terminal task cleanup; isolating panic",
        );
    }
}

fn drop_future_safely(future: LocalBoxFuture, task_id: u64, terminal: &'static str) {
    if let Err(payload) = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| drop(future))) {
        mark_teardown_panicked();
        tracing::error!(
            target: trace_targets::ASYNC,
            event = "task_future_drop_panicked",
            task_id,
            terminal,
            panic = describe_panic(&*payload),
            "spawned task future panicked from Drop; isolating destructor panic",
        );
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
        unsafe {
            Waker::from_raw(RawWaker::new(
                Arc::into_raw(data).cast::<()>(),
                &TASK_WAKER_VTABLE,
            ))
        }
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
        // Route through the capacity-bypassing internal-wake path: a task
        // waker's wake is the task's only scheduling signal, so dropping it on
        // a full queue would strand the task (a lost-wakeup hang). It is
        // bounded instead by the number of live tasks — one pending wake each,
        // coalesced by `scheduled`.
        let this = Arc::clone(self);
        let id = self.id;
        if self
            .owner
            .queue_internal_wake(move || {
                this.scheduled.store(false, Ordering::Release);
                schedule_task_by_id(id);
            })
            .is_err()
        {
            // The only remaining error is a closed owner runtime; nothing more
            // can be scheduled there. Reset the marker so a later wake (if the
            // runtime somehow reopens) can retry.
            self.scheduled.store(false, Ordering::Release);
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
    /// The runtime reached quiescence or its owning thread shut down before
    /// the task could make further progress.
    Cancelled,
    /// The task panicked while being polled.
    Panicked,
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

    /// Returns `true` once the task has reached any terminal state.
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
        let Some(join_waker) = self.mark_terminal(TaskState::Aborted) else {
            return;
        };
        try_with_installed_thread(|state| {
            if let Some(state) = state {
                super::state::RuntimeCounters::bump(&state.shared.counters.tasks_cancelled);
            }
        });

        // Dropping the future cancels any in-flight driver operations it is
        // parked on via their `Drop` impls. If the task is mid-poll (self
        // abort) the future is on the stack and this take is a no-op;
        // `FutureTask::poll` then drops it instead of restoring it.
        let task = self.task.borrow().upgrade();
        let future = task
            .as_ref()
            .and_then(|task| task.future.borrow_mut().take());

        // Release the runtime's registry reference so an aborted task is not
        // retained (mid-poll self-abort still keeps it alive via the executing
        // microtask closure until that poll returns).
        deregister_task(self.id.get());
        wake_join_safely(join_waker, self.id.get(), "abort");

        if let Some(future) = future {
            drop_future_safely(future, self.id.get(), "abort");
        }
    }

    /// Moves the task to a terminal panicked state and wakes the joiner with
    /// [`JoinError::Panicked`]. Called from [`FutureTask::poll`] when the
    /// future unwinds. A no-op if the task already finished or was aborted
    /// (an abort observed during the panicking poll wins).
    fn mark_panicked(&self) -> Option<Waker> {
        self.mark_terminal(TaskState::Panicked).flatten()
    }

    fn mark_cancelled(&self) -> Option<Option<Waker>> {
        self.mark_terminal(TaskState::Cancelled)
    }

    fn mark_terminal(&self, terminal: TaskState) -> Option<Option<Waker>> {
        if !matches!(self.state.get(), TaskState::Running) {
            return None;
        }
        self.state.set(terminal);
        Some(self.join_waker.borrow_mut().take())
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

        let waker = self.shared.join_waker.borrow_mut().take();
        wake_join_safely(waker, self.shared.id.get(), "completion");
    }

    pub(crate) fn poll(&self, cx: &mut Context<'_>) -> Poll<Result<T, JoinError>> {
        match self.shared.state.get() {
            TaskState::Finished => Poll::Ready(Ok(self
                .result
                .borrow_mut()
                .take()
                .expect("join handle polled after completion"))),
            TaskState::Aborted => Poll::Ready(Err(JoinError::Aborted)),
            TaskState::Cancelled => Poll::Ready(Err(JoinError::Cancelled)),
            TaskState::Panicked => Poll::Ready(Err(JoinError::Panicked)),
            TaskState::Running => {
                *self.shared.join_waker.borrow_mut() = Some(cx.waker().clone());
                Poll::Pending
            }
        }
    }
}
