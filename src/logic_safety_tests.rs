//! Driver-free safety-tool coverage for shared runtime logic.
//!
//! These tests deliberately enter `MockRuntimeHarness`. Miri and TSan run only
//! this module, so neither tool initializes io_uring, kqueue, or IOCP.

use std::cell::{Cell, RefCell};
use std::future::Future;
use std::io;
use std::pin::Pin;
use std::rc::Rc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, mpsc};
use std::task::{Context, Poll, Waker};

use crate::channel::{mpsc as async_mpsc, watch};
use crate::io::pending::ReadState;
use crate::op::completion::completion_for_current_thread;
use crate::platform::runtime_shared::test_support::{MockRuntime, MockRuntimeHarness};
use crate::platform::runtime_shared::{
    block_on, queue_future, queue_task, run, run_until_stalled, spawn_worker, timeout, yield_now,
};
use crate::sync::{Mutex as AsyncMutex, Notify};

#[test]
fn logic_safety_scheduler_task_timer_and_sync() {
    let harness = MockRuntimeHarness::new();
    let control = harness.plan_driver();
    let events = Rc::new(RefCell::new(Vec::new()));

    harness.enter(|| {
        let events_by_task = Rc::clone(&events);
        queue_task::<MockRuntime, _>(move || events_by_task.borrow_mut().push("macrotask"));

        let mutex = Rc::new(AsyncMutex::new(0usize));
        let notify = Rc::new(Notify::new());
        let waiter_done = Rc::new(Cell::new(false));

        let mutex_by_first = Rc::clone(&mutex);
        let notify_by_first = Rc::clone(&notify);
        let waiter_done_by_first = Rc::clone(&waiter_done);
        queue_future::<MockRuntime, _>(async move {
            let mut value = mutex_by_first.lock().await;
            *value += 1;
            drop(value);
            notify_by_first.notified().await;
            waiter_done_by_first.set(true);
        });

        let mutex_by_second = Rc::clone(&mutex);
        let notify_by_second = Rc::clone(&notify);
        let joined = queue_future::<MockRuntime, _>(async move {
            yield_now().await;
            let mut value = mutex_by_second.lock().await;
            *value += 1;
            drop(value);
            notify_by_second.notify_one();
            42usize
        });

        let events_by_timer = Rc::clone(&events);
        timeout::<MockRuntime, _>(std::time::Duration::ZERO, move || {
            events_by_timer.borrow_mut().push("timer");
        });
        control.fire_timer(1);

        run::<MockRuntime>();

        assert_eq!(block_on::<MockRuntime, _>(joined).unwrap(), 42);
        assert_eq!(*mutex.try_lock().expect("mutex should be released"), 2);
        assert!(waiter_done.get());
    });

    assert_eq!(&*events.borrow(), &["macrotask", "timer"]);
}

#[test]
fn logic_safety_channel_watch_and_concurrent_completions() {
    let harness = MockRuntimeHarness::new();
    let _control = harness.plan_driver();

    harness.enter(|| {
        let (sender, mut receiver) = async_mpsc::unbounded_channel();
        let received = Rc::new(RefCell::new(Vec::new()));
        let received_by_task = Rc::clone(&received);
        queue_future::<MockRuntime, _>(async move {
            while let Some(value) = receiver.recv().await {
                received_by_task.borrow_mut().push(value);
            }
        });
        run_until_stalled::<MockRuntime>();

        let producer_count = if cfg!(miri) { 2 } else { 4 };
        let values_per_producer = if cfg!(miri) { 4 } else { 64 };
        let mut producers = Vec::new();
        for producer in 0..producer_count {
            let sender = sender.clone();
            producers.push(std::thread::spawn(move || {
                for value in 0..values_per_producer {
                    sender
                        .send(producer * values_per_producer + value)
                        .expect("receiver should remain open");
                }
            }));
        }
        drop(sender);
        for producer in producers {
            producer.join().expect("producer should finish");
        }
        run::<MockRuntime>();
        assert_eq!(
            received.borrow().len(),
            producer_count * values_per_producer
        );

        let (watch_sender, mut watch_receiver) = watch::channel(0usize);
        let watched = Arc::new(AtomicUsize::new(0));
        let watched_by_task = Arc::clone(&watched);
        queue_future::<MockRuntime, _>(async move {
            watch_receiver
                .changed()
                .await
                .expect("watch sender should remain open");
            watched_by_task.store(*watch_receiver.borrow(), Ordering::Release);
        });
        run_until_stalled::<MockRuntime>();
        let watch_thread = std::thread::spawn(move || {
            watch_sender
                .send(7)
                .expect("watch receiver should remain open");
        });
        run::<MockRuntime>();
        watch_thread.join().expect("watch sender should finish");
        assert_eq!(watched.load(Ordering::Acquire), 7);

        let completion_count = if cfg!(miri) { 2 } else { 8 };
        let completed = Arc::new(AtomicUsize::new(0));
        let mut completers = Vec::new();
        for value in 1..=completion_count {
            let (future, source) = completion_for_current_thread::<usize>();
            let completed_by_task = Arc::clone(&completed);
            queue_future::<MockRuntime, _>(async move {
                completed_by_task.fetch_add(future.await, Ordering::AcqRel);
            });
            completers.push(std::thread::spawn(move || source.complete(value)));
        }
        run::<MockRuntime>();
        for completer in completers {
            completer.join().expect("completion source should finish");
        }
        assert_eq!(
            completed.load(Ordering::Acquire),
            completion_count * (completion_count + 1) / 2
        );
    });
}

#[test]
fn logic_safety_waker_clone_drop_and_worker() {
    let harness = MockRuntimeHarness::new();
    let _worker_driver = harness.plan_driver();
    let _parent_driver = harness.plan_driver();

    harness.enter(|| {
        let ready = Arc::new(AtomicBool::new(false));
        let (waker_sender, waker_receiver) = mpsc::sync_channel(1);
        let mut waker_sender = Some(waker_sender);
        let ready_by_task = Arc::clone(&ready);
        let woken = queue_future::<MockRuntime, _>(std::future::poll_fn(move |cx| {
            if ready_by_task.load(Ordering::Acquire) {
                return Poll::Ready(());
            }
            if let Some(sender) = waker_sender.take() {
                sender
                    .send(cx.waker().clone())
                    .expect("test should receive the task waker");
            }
            Poll::Pending
        }));
        run_until_stalled::<MockRuntime>();

        let waker = waker_receiver
            .recv()
            .expect("task should publish its waker");
        let clone_threads = if cfg!(miri) { 2 } else { 6 };
        let clone_iterations = if cfg!(miri) { 8 } else { 1_000 };
        let mut cloners = Vec::new();
        for _ in 0..clone_threads {
            let waker = waker.clone();
            cloners.push(std::thread::spawn(move || {
                for _ in 0..clone_iterations {
                    drop(waker.clone());
                }
            }));
        }
        for cloner in cloners {
            cloner.join().expect("waker cloner should finish");
        }
        ready.store(true, Ordering::Release);
        waker.wake();
        run::<MockRuntime>();
        assert!(block_on::<MockRuntime, _>(woken).is_ok());

        let worker_ran = Arc::new(AtomicBool::new(false));
        let worker_ran_on_thread = Arc::clone(&worker_ran);
        let worker = spawn_worker::<MockRuntime, _, _>(
            move || worker_ran_on_thread.store(true, Ordering::Release),
            || {},
        );
        run::<MockRuntime>();
        assert!(worker_ran.load(Ordering::Acquire));
        assert!(block_on::<MockRuntime, _>(worker.join()).is_ok());
    });
}

struct PendingRead {
    dropped: Rc<Cell<bool>>,
}

impl Future for PendingRead {
    type Output = io::Result<Vec<u8>>;

    fn poll(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Self::Output> {
        Poll::Pending
    }
}

impl Drop for PendingRead {
    fn drop(&mut self) {
        self.dropped.set(true);
    }
}

#[test]
fn logic_safety_pending_state_cancels_owned_operation_before_shutdown() {
    let mut state = ReadState::default();
    let dropped = Rc::new(Cell::new(false));
    let mut pending = Some(PendingRead {
        dropped: Rc::clone(&dropped),
    });
    let mut context = Context::from_waker(Waker::noop());
    let mut buffer = [0u8; 8];

    assert!(
        state
            .poll_slice(&mut context, &mut buffer, |_| {
                Box::pin(pending.take().expect("read should start once"))
            })
            .is_pending()
    );
    assert!(
        state
            .poll_shutdown(&mut context, || Box::pin(std::future::ready(Ok(()))))
            .is_ready()
    );
    assert!(
        dropped.get(),
        "resource shutdown must drop its pending operation"
    );
}
