//! Shared blocking-task thread pool.
//!
//! A single fixed-size worker pool used by every platform backend for work that
//! must execute on a real OS thread (filesystem syscalls on macOS, fallback
//! stdin reads, blocking DNS resolution, the Linux fs offload path, etc).
//!
//! The pool is created lazily on first use and lives for the rest of the
//! process. It is intentionally singleton-per-process: each runtime worker
//! thread can submit, and any blocking-pool thread can pick up the work.
//!
//! Capacity is bounded. When the queue is full, [`spawn_blocking`] returns an
//! [`io::Error`] rather than silently spawning a fresh thread; a runaway
//! offload caller is then visible to the user as backpressure instead of
//! becoming a thread-spawn storm.

use std::io;
use std::sync::{Arc, Mutex, OnceLock, mpsc};
use std::thread;

use crate::platform::current::runtime::{ThreadHandle, current_thread_handle};

#[cfg(test)]
use std::cell::RefCell;
#[cfg(test)]
use std::marker::PhantomData;
#[cfg(test)]
use std::rc::Rc;

type BlockingTask = Box<dyn FnOnce() + Send + 'static>;

/// Bounded queue capacity. Matches the macOS fs pool's prior limit.
const QUEUE_CAPACITY: usize = 1024;

/// Lower bound on worker count when no explicit override is supplied.
const MIN_WORKERS: usize = 2;

/// Upper bound on worker count when no explicit override is supplied.
///
/// Blocking workers are for OS-thread-bound calls; oversubscribing helps
/// throughput when the kernel parks workers in syscalls but past a point the
/// scheduler thrashes. 32 is generous for a runtime that nominally runs one
/// reactor per core.
const MAX_WORKERS: usize = 32;

static BLOCKING_POOL: OnceLock<io::Result<BlockingPool>> = OnceLock::new();

#[cfg(test)]
pub(crate) trait BlockingTaskHook: Send + Sync + 'static {
    fn before_execute(&self);
    fn after_execute(&self);
}

#[cfg(test)]
impl BlockingTaskHook for crate::platform::runtime_shared::test_support::ExecutionGate {
    fn before_execute(&self) {
        self.arrive_and_wait();
    }

    fn after_execute(&self) {
        self.mark_completed();
    }
}

#[cfg(test)]
thread_local! {
    static BLOCKING_TASK_HOOKS: RefCell<Vec<Arc<dyn BlockingTaskHook>>> =
        const { RefCell::new(Vec::new()) };
}

#[cfg(test)]
pub(crate) struct BlockingTaskHookGuard {
    hook: Arc<dyn BlockingTaskHook>,
    _not_send: PhantomData<Rc<()>>,
}

#[cfg(test)]
impl Drop for BlockingTaskHookGuard {
    fn drop(&mut self) {
        BLOCKING_TASK_HOOKS.with(|hooks| {
            let installed = hooks
                .borrow_mut()
                .pop()
                .expect("blocking task hook stack should not be empty");
            assert!(
                Arc::ptr_eq(&installed, &self.hook),
                "blocking task hooks must be dropped in stack order"
            );
        });
    }
}

#[cfg(test)]
pub(crate) fn install_task_hook(hook: Arc<dyn BlockingTaskHook>) -> BlockingTaskHookGuard {
    BLOCKING_TASK_HOOKS.with(|hooks| hooks.borrow_mut().push(Arc::clone(&hook)));
    BlockingTaskHookGuard {
        hook,
        _not_send: PhantomData,
    }
}

struct BlockingPool {
    sender: mpsc::SyncSender<BlockingTask>,
}

impl BlockingPool {
    fn spawn(&self, task: BlockingTask) -> io::Result<()> {
        self.sender.try_send(task).map_err(|error| match error {
            mpsc::TrySendError::Full(_) => io::Error::new(
                io::ErrorKind::WouldBlock,
                "runite blocking worker queue is full",
            ),
            mpsc::TrySendError::Disconnected(_) => io::Error::new(
                io::ErrorKind::BrokenPipe,
                "runite blocking worker pool has stopped",
            ),
        })
    }
}

/// Submits `task` to the shared blocking pool.
///
/// Returns `Err` if the pool could not be initialized or its bounded queue is
/// full. Callers must propagate the error rather than fall back to per-call
/// thread spawning, which would defeat the pool.
pub(crate) fn spawn_blocking<F>(task: F) -> io::Result<()>
where
    F: FnOnce() + Send + 'static,
{
    pool()?.spawn(box_task(task))
}

/// Submits runtime-owned blocking work and delivers its terminal outcome.
///
/// The submitting runtime is kept live from before pool submission until
/// `complete` returns (or unwinds). Accepted work calls `complete` exactly once
/// with either the return value or the caught panic. Rejected work calls
/// neither closure and releases its liveness before this function returns.
///
/// Completion code may itself panic: the panic is caught long enough to
/// release liveness, then resumed into [`worker_loop`]'s isolation boundary so
/// the worker remains available. This is the preferred primitive for blocking
/// jobs that own a runtime completion; raw [`spawn_blocking`] remains available
/// for producer loops whose lifetime is managed separately.
pub(crate) fn spawn_blocking_owned<F, C, R>(work: F, complete: C) -> io::Result<()>
where
    F: FnOnce() -> R + Send + 'static,
    C: FnOnce(thread::Result<R>) + Send + 'static,
    R: Send + 'static,
{
    spawn_blocking_owned_with(current_thread_handle(), work, complete, |task| {
        pool()?.spawn(task)
    })
}

fn spawn_blocking_owned_with<F, C, R, S>(
    owner: ThreadHandle,
    work: F,
    complete: C,
    submit: S,
) -> io::Result<()>
where
    F: FnOnce() -> R + Send + 'static,
    C: FnOnce(thread::Result<R>) + Send + 'static,
    R: Send + 'static,
    S: FnOnce(BlockingTask) -> io::Result<()>,
{
    let job = OwnedBlockingJob {
        liveness: BlockingJobLiveness::new(owner),
        work: Some(work),
        complete: Some(complete),
    };
    submit(box_task(move || job.run()))
}

struct BlockingJobLiveness {
    owner: Option<ThreadHandle>,
}

impl BlockingJobLiveness {
    fn new(owner: ThreadHandle) -> Self {
        owner.begin_async_operation();
        Self { owner: Some(owner) }
    }

    fn finish(&mut self) {
        if let Some(owner) = self.owner.take() {
            owner.finish_async_operation();
        }
    }
}

impl Drop for BlockingJobLiveness {
    fn drop(&mut self) {
        self.finish();
    }
}

struct OwnedBlockingJob<F, C> {
    liveness: BlockingJobLiveness,
    work: Option<F>,
    complete: Option<C>,
}

impl<F, C> Drop for OwnedBlockingJob<F, C> {
    fn drop(&mut self) {
        // Terminalize runtime liveness before user-owned captures are dropped.
        // This remains true on queue rejection and unwind, even if one of
        // those captures has a panicking destructor.
        self.liveness.finish();
    }
}

impl<F, C, R> OwnedBlockingJob<F, C>
where
    F: FnOnce() -> R,
    C: FnOnce(thread::Result<R>),
{
    fn run(mut self) {
        let work = self
            .work
            .take()
            .expect("owned blocking work must run at most once");
        let outcome = std::panic::catch_unwind(std::panic::AssertUnwindSafe(work));
        let complete = self
            .complete
            .take()
            .expect("owned blocking completion must run at most once");
        let completion_outcome =
            std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| complete(outcome)));

        // Commit the pending-count terminal state before propagating a
        // completion panic to the worker-loop isolation boundary.
        self.liveness.finish();
        if let Err(payload) = completion_outcome {
            std::panic::resume_unwind(payload);
        }
    }
}

fn box_task(task: impl FnOnce() + Send + 'static) -> BlockingTask {
    #[cfg(test)]
    {
        let hook = BLOCKING_TASK_HOOKS.with(|hooks| hooks.borrow().last().cloned());
        Box::new(move || {
            let finished = hook.map(|hook| {
                hook.before_execute();
                BlockingTaskFinished(hook)
            });
            task();
            drop(finished);
        })
    }

    #[cfg(not(test))]
    Box::new(task)
}

#[cfg(test)]
struct BlockingTaskFinished(Arc<dyn BlockingTaskHook>);

#[cfg(test)]
impl Drop for BlockingTaskFinished {
    fn drop(&mut self) {
        self.0.after_execute();
    }
}

fn pool() -> io::Result<&'static BlockingPool> {
    match BLOCKING_POOL.get_or_init(create_pool) {
        Ok(pool) => Ok(pool),
        Err(error) => Err(io::Error::new(error.kind(), error.to_string())),
    }
}

fn worker_count() -> usize {
    if let Ok(value) = std::env::var("RUNITE_BLOCKING_THREADS")
        && let Ok(parsed) = value.parse::<usize>()
        && parsed >= 1
    {
        return parsed.min(MAX_WORKERS);
    }

    std::thread::available_parallelism()
        .map(usize::from)
        .unwrap_or(MIN_WORKERS)
        .clamp(MIN_WORKERS, MAX_WORKERS)
}

fn create_pool() -> io::Result<BlockingPool> {
    let (sender, receiver) = mpsc::sync_channel::<BlockingTask>(QUEUE_CAPACITY);
    let receiver = Arc::new(Mutex::new(receiver));
    let workers = worker_count();
    let mut spawned = 0usize;
    let mut last_error: Option<io::Error> = None;

    for index in 0..workers {
        let receiver = Arc::clone(&receiver);
        match thread::Builder::new()
            .name(format!("runite-blocking-{index}"))
            .spawn(move || worker_loop(receiver))
        {
            Ok(_) => spawned += 1,
            Err(error) => last_error = Some(error),
        }
    }

    if spawned == 0 {
        return Err(io::Error::other(last_error.expect(
            "at least one blocking worker spawn should have been attempted",
        )));
    }

    Ok(BlockingPool { sender })
}

fn worker_loop(receiver: Arc<Mutex<mpsc::Receiver<BlockingTask>>>) {
    loop {
        let task = {
            let guard = receiver
                .lock()
                .expect("runite blocking queue mutex poisoned");
            guard.recv()
        };
        match task {
            Ok(task) => {
                // Isolate the job's panic so it cannot unwind out of the worker
                // loop and silently retire a pool thread. Losing workers one
                // panic at a time would shrink pool capacity until
                // `spawn_blocking` starts failing. `spawn_blocking` itself
                // converts a caught panic into `JoinError::Panicked` for the
                // awaiter; this is the backstop for every internal caller (fs,
                // dns, stdin offload). The panic is still reported through the
                // process panic hook.
                if let Err(payload) = std::panic::catch_unwind(std::panic::AssertUnwindSafe(task)) {
                    let message = if let Some(message) = payload.downcast_ref::<&'static str>() {
                        message
                    } else if let Some(message) = payload.downcast_ref::<String>() {
                        message.as_str()
                    } else {
                        "Box<dyn Any>"
                    };
                    tracing::error!(
                        target: "runite::runtime",
                        event = "blocking_task_panicked",
                        panic = message,
                        "blocking task panicked; worker kept alive",
                    );
                }
            }
            Err(_) => break,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::platform::runtime_shared::test_support::ExecutionGate;
    use std::sync::atomic::AtomicBool;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::{Arc, Condvar, Mutex};
    use std::time::Duration;

    #[test]
    fn spawn_blocking_runs_task() {
        let done = Arc::new((Mutex::new(false), Condvar::new()));
        let done_clone = Arc::clone(&done);
        spawn_blocking(move || {
            let (lock, cvar) = &*done_clone;
            *lock.lock().unwrap() = true;
            cvar.notify_all();
        })
        .expect("spawn_blocking should succeed");

        let (lock, cvar) = &*done;
        let mut guard = lock.lock().unwrap();
        while !*guard {
            let (next, _) = cvar.wait_timeout(guard, Duration::from_secs(5)).unwrap();
            guard = next;
            if *guard {
                break;
            }
        }
        assert!(*guard, "task should have run");
    }

    #[test]
    fn spawn_blocking_handles_many_tasks() {
        let counter = Arc::new(AtomicUsize::new(0));
        let total = 200usize;
        let pair = Arc::new((Mutex::new(0usize), Condvar::new()));

        for _ in 0..total {
            let counter = Arc::clone(&counter);
            let pair = Arc::clone(&pair);
            spawn_blocking(move || {
                counter.fetch_add(1, Ordering::SeqCst);
                let (lock, cvar) = &*pair;
                let mut done = lock.lock().unwrap();
                *done += 1;
                cvar.notify_all();
            })
            .expect("spawn_blocking should succeed");
        }

        let (lock, cvar) = &*pair;
        let mut done = lock.lock().unwrap();
        while *done < total {
            let (next, _) = cvar.wait_timeout(done, Duration::from_secs(10)).unwrap();
            done = next;
        }
        assert_eq!(counter.load(Ordering::SeqCst), total);
    }

    #[test]
    fn owned_job_liveness_spans_terminal_callback() {
        std::thread::spawn(|| {
            let owner = current_thread_handle();
            let owner_for_completion = owner.clone();
            let pending_during_completion = Arc::new(AtomicUsize::new(0));
            let pending_seen = Arc::clone(&pending_during_completion);
            let queued = Arc::new(Mutex::new(None::<BlockingTask>));
            let queued_for_submit = Arc::clone(&queued);

            spawn_blocking_owned_with(
                owner.clone(),
                || 42usize,
                move |outcome| {
                    assert_eq!(outcome.expect("work should return"), 42);
                    pending_seen.store(
                        owner_for_completion
                            .shared
                            .pending_ops
                            .load(Ordering::Acquire),
                        Ordering::Release,
                    );
                },
                move |task| {
                    *queued_for_submit.lock().unwrap() = Some(task);
                    Ok(())
                },
            )
            .expect("fake submit should accept the owned job");

            assert_eq!(owner.shared.pending_ops.load(Ordering::Acquire), 1);
            queued
                .lock()
                .unwrap()
                .take()
                .expect("accepted job should be queued")();

            assert_eq!(pending_during_completion.load(Ordering::Acquire), 1);
            assert_eq!(owner.shared.pending_ops.load(Ordering::Acquire), 0);
        })
        .join()
        .expect("owned-job liveness test thread should finish");
    }

    #[test]
    fn rejected_owned_jobs_release_liveness_exactly_once() {
        std::thread::spawn(|| {
            let owner = current_thread_handle();
            let work_calls = Arc::new(AtomicUsize::new(0));
            let completion_calls = Arc::new(AtomicUsize::new(0));

            let work_calls_on_init_error = Arc::clone(&work_calls);
            let completion_calls_on_init_error = Arc::clone(&completion_calls);
            let error = spawn_blocking_owned_with(
                owner.clone(),
                move || {
                    work_calls_on_init_error.fetch_add(1, Ordering::AcqRel);
                },
                move |_| {
                    completion_calls_on_init_error.fetch_add(1, Ordering::AcqRel);
                },
                |task| {
                    drop(task);
                    Err(io::Error::other("injected pool initialization failure"))
                },
            )
            .expect_err("injected initialization failure should reject the job");
            assert_eq!(error.kind(), io::ErrorKind::Other);
            assert_eq!(owner.shared.pending_ops.load(Ordering::Acquire), 0);

            let (sender, _receiver) = mpsc::sync_channel(0);
            let full_pool = BlockingPool { sender };
            let work_calls_on_full = Arc::clone(&work_calls);
            let completion_calls_on_full = Arc::clone(&completion_calls);
            let error = spawn_blocking_owned_with(
                owner.clone(),
                move || {
                    work_calls_on_full.fetch_add(1, Ordering::AcqRel);
                },
                move |_| {
                    completion_calls_on_full.fetch_add(1, Ordering::AcqRel);
                },
                |task| full_pool.spawn(task),
            )
            .expect_err("zero-capacity pool should reject without a waiting worker");
            assert_eq!(error.kind(), io::ErrorKind::WouldBlock);
            assert_eq!(owner.shared.pending_ops.load(Ordering::Acquire), 0);
            assert_eq!(work_calls.load(Ordering::Acquire), 0);
            assert_eq!(completion_calls.load(Ordering::Acquire), 0);
        })
        .join()
        .expect("owned-job rejection test thread should finish");
    }

    #[test]
    fn owned_job_catches_work_panic_and_terminalizes() {
        std::thread::spawn(|| {
            let owner = current_thread_handle();
            let owner_for_completion = owner.clone();
            let completion_calls = Arc::new(AtomicUsize::new(0));
            let completion_calls_on_worker = Arc::clone(&completion_calls);
            let mut queued = None;

            spawn_blocking_owned_with(
                owner.clone(),
                || -> usize { panic!("owned blocking work panic") },
                move |outcome| {
                    assert!(outcome.is_err());
                    assert_eq!(
                        owner_for_completion
                            .shared
                            .pending_ops
                            .load(Ordering::Acquire),
                        1
                    );
                    completion_calls_on_worker.fetch_add(1, Ordering::AcqRel);
                },
                |task| {
                    queued = Some(task);
                    Ok(())
                },
            )
            .expect("fake submit should accept panicking work");

            queued.expect("panicking work should be queued")();
            assert_eq!(completion_calls.load(Ordering::Acquire), 1);
            assert_eq!(owner.shared.pending_ops.load(Ordering::Acquire), 0);
        })
        .join()
        .expect("owned-job panic test thread should finish");
    }

    #[test]
    fn worker_survives_completion_panic_without_stranding_owned_job() {
        std::thread::spawn(|| {
            let owner = current_thread_handle();
            let (sender, receiver) = mpsc::sync_channel(2);
            let pool = BlockingPool { sender };
            let follow_up_ran = Arc::new(AtomicBool::new(false));

            spawn_blocking_owned_with(
                owner.clone(),
                || 7usize,
                |_| panic!("owned completion panic"),
                |task| pool.spawn(task),
            )
            .expect("panicking completion job should queue");

            let follow_up_ran_on_worker = Arc::clone(&follow_up_ran);
            pool.spawn(Box::new(move || {
                follow_up_ran_on_worker.store(true, Ordering::Release);
            }))
            .expect("follow-up job should queue");
            drop(pool);

            assert_eq!(owner.shared.pending_ops.load(Ordering::Acquire), 1);
            worker_loop(Arc::new(Mutex::new(receiver)));

            assert!(follow_up_ran.load(Ordering::Acquire));
            assert_eq!(owner.shared.pending_ops.load(Ordering::Acquire), 0);
        })
        .join()
        .expect("worker-panic isolation test thread should finish");
    }

    #[test]
    fn task_local_hook_gates_only_the_submitted_job() {
        let gate = ExecutionGate::default();
        let hook = install_task_hook(Arc::new(gate.clone()));
        let unhooked_ran = Arc::new(AtomicBool::new(false));
        let unhooked_ran_on_worker = Arc::clone(&unhooked_ran);
        let unhooked = std::thread::spawn(move || {
            box_task(move || unhooked_ran_on_worker.store(true, Ordering::Release))
        })
        .join()
        .expect("task-construction thread should finish");
        unhooked();
        assert!(
            unhooked_ran.load(Ordering::Acquire),
            "a hook installed on one thread must not affect another thread"
        );

        let ran = Arc::new(AtomicBool::new(false));
        let ran_on_worker = Arc::clone(&ran);
        let release = gate.release_on_drop();

        spawn_blocking(move || ran_on_worker.store(true, Ordering::Release))
            .expect("controlled task should queue");
        drop(hook);

        let arrived = gate.wait_until_arrived(Duration::from_secs(5));
        let ran_before_release = ran.load(Ordering::Acquire);
        release.release();
        let completed = gate.wait_until_completed(Duration::from_secs(5));
        let ran_after_release = ran.load(Ordering::Acquire);

        assert!(arrived, "controlled task should reach its execution gate");
        assert!(!ran_before_release);
        assert!(completed, "controlled task should finish after release");
        assert!(ran_after_release);
    }
}
