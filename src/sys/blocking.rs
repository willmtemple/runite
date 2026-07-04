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
    pool()?.spawn(Box::new(task))
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
}
