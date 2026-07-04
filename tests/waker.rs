//! Regression tests for the task waker's `Send + Sync` soundness.
//!
//! `std::task::Waker` is `Send + Sync`, so a leaf future may move `cx.waker()`
//! to another thread and clone, drop, or wake it there entirely in safe code.
//! runite's own primitives marshal completions to the owner thread before
//! waking, but third-party leaf futures do not — the task waker must be
//! thread-safe on its own. Before the `Arc`-based waker, this raced a
//! non-atomic `Rc` refcount (UB) or scheduled a `!Send` future onto the wrong
//! thread.

use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll, Waker};
use std::time::Duration;

mod common;

/// State shared with the foreign thread that will wake the task.
struct WakeShared {
    ready: AtomicBool,
    waker: Mutex<Option<Waker>>,
}

/// A leaf future that parks by publishing the task's waker to another thread and
/// completes once that thread flips `ready` and wakes it.
struct ForeignWake {
    shared: Arc<WakeShared>,
}

impl Future for ForeignWake {
    type Output = u32;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<u32> {
        if self.shared.ready.load(Ordering::Acquire) {
            return Poll::Ready(42);
        }
        // Publish the (cross-thread) waker, then re-check to close the race
        // with a wake that lands between the first load and the store.
        *self.shared.waker.lock().unwrap() = Some(cx.waker().clone());
        if self.shared.ready.load(Ordering::Acquire) {
            return Poll::Ready(42);
        }
        Poll::Pending
    }
}

/// A task woken from a plain `std::thread` (no runtime installed) must be
/// rescheduled on, and polled by, its owning runtime thread — and must resolve.
#[test]
fn task_woken_from_foreign_thread_resolves() {
    let value = common::block_on(|| async {
        let shared = Arc::new(WakeShared {
            ready: AtomicBool::new(false),
            waker: Mutex::new(None),
        });

        let foreign = Arc::clone(&shared);
        std::thread::spawn(move || {
            // Wait until the task has parked and published its waker, then wake
            // it from this non-runtime thread.
            loop {
                if let Some(waker) = foreign.waker.lock().unwrap().clone() {
                    foreign.ready.store(true, Ordering::Release);
                    waker.wake();
                    return;
                }
                std::thread::yield_now();
            }
        });

        // The `sleep` keeps the event loop alive across the cross-thread wake
        // window (a task parked on a bare external waker registers no runtime
        // resource, so the loop would otherwise be free to exit). If the wake
        // path were broken the test would hang here until the timeout fires.
        let woken = ForeignWake { shared };
        let result: Option<u32> = runite::select! {
            v = woken => Some(v),
            _ = runite::time::sleep(Duration::from_secs(5)) => None,
        };
        result.expect("cross-thread wake was never delivered")
    });

    assert_eq!(value, 42);
}

/// Cloning and dropping the task waker concurrently from other threads must not
/// race the refcount. Runs many clone/drop cycles on several threads while the
/// owner holds its own copy; a non-atomic refcount would corrupt under TSan or
/// crash intermittently.
#[test]
fn task_waker_clone_drop_is_thread_safe() {
    let ok = common::block_on(|| async {
        let slot: Arc<Mutex<Option<Waker>>> = Arc::new(Mutex::new(None));

        // Capture this task's waker once.
        std::future::poll_fn(|cx| {
            *slot.lock().unwrap() = Some(cx.waker().clone());
            Poll::Ready(())
        })
        .await;

        let waker = slot.lock().unwrap().clone().unwrap();

        let mut handles = Vec::new();
        for _ in 0..8 {
            let w = waker.clone();
            handles.push(std::thread::spawn(move || {
                for _ in 0..10_000 {
                    let c = w.clone();
                    drop(c);
                }
            }));
        }
        for h in handles {
            h.join().unwrap();
        }

        true
    });

    assert!(ok);
}
