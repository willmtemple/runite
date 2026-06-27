//! Shared test bodies parameterised over a per-platform `Runtime`.
//!
//! Both `platform/linux_x86_64/runtime.rs` and `platform/macos_aarch64/runtime.rs`
//! ship a small `mod tests` that pins these helpers to their concrete
//! marker type. Keeping the bodies here means the integration scenarios stay
//! in lockstep across platforms.

use std::cell::{Cell, RefCell};
use std::rc::Rc;
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::Duration;

use super::{
    IntervalHandle, Runtime, current_thread_handle, interval, queue_future, queue_microtask,
    queue_task, run, spawn_worker, timeout, yield_now,
};
use crate::op::completion::completion_for_current_thread;

pub fn runtime_executes_local_and_remote_work<R: Runtime>() {
    let log = Arc::new(Mutex::new(Vec::<String>::new()));
    let main_handle = current_thread_handle::<R>();

    {
        let log = Arc::clone(&log);
        queue_task::<R, _>(move || log.lock().unwrap().push("main task".into()));
    }
    {
        let log = Arc::clone(&log);
        queue_microtask::<R, _>(move || log.lock().unwrap().push("main microtask".into()));
    }
    {
        let log = Arc::clone(&log);
        queue_future::<R, _>(async move {
            log.lock().unwrap().push("main future start".into());
            yield_now().await;
            log.lock().unwrap().push("main future end".into());
        });
    }
    {
        let log = Arc::clone(&log);
        timeout::<R, _>(Duration::from_millis(5), move || {
            log.lock().unwrap().push("main timeout".into());
        });
    }
    {
        let log = Arc::clone(&log);
        let handle_slot: Rc<RefCell<Option<IntervalHandle>>> = Rc::new(RefCell::new(None));
        let handle_slot_clone = Rc::clone(&handle_slot);
        let tick_count = Rc::new(Cell::new(0usize));
        let tick_count_clone = Rc::clone(&tick_count);
        let interval_handle = interval::<R, _>(Duration::from_millis(3), move || {
            let next = tick_count_clone.get() + 1;
            tick_count_clone.set(next);
            log.lock().unwrap().push(format!("main interval {next}"));
            if next == 2 {
                let handle = handle_slot_clone.borrow_mut().take().unwrap();
                handle.cancel();
            }
        });
        *handle_slot.borrow_mut() = Some(interval_handle);
    }

    {
        let worker_log = Arc::clone(&log);
        let exit_log = Arc::clone(&log);
        let main_handle_for_worker = main_handle.clone();
        spawn_worker::<R, _, _>(
            move || {
                let log = Arc::clone(&worker_log);
                queue_task::<R, _>({
                    let log = Arc::clone(&log);
                    move || log.lock().unwrap().push("worker task".into())
                });
                queue_microtask::<R, _>({
                    let log = Arc::clone(&log);
                    move || log.lock().unwrap().push("worker microtask".into())
                });
                queue_future::<R, _>({
                    let log = Arc::clone(&log);
                    async move {
                        log.lock().unwrap().push("worker future start".into());
                        yield_now().await;
                        log.lock().unwrap().push("worker future end".into());
                    }
                });
                timeout::<R, _>(Duration::from_millis(7), move || {
                    let _ = main_handle_for_worker.queue_macrotask({
                        let log = Arc::clone(&log);
                        move || log.lock().unwrap().push("worker timeout to main".into())
                    });
                });
            },
            {
                let log = Arc::clone(&exit_log);
                move || log.lock().unwrap().push("worker exit".into())
            },
        );
    }

    run::<R>();

    let log = log.lock().unwrap();
    assert!(log.iter().any(|entry| entry == "main task"));
    assert!(log.iter().any(|entry| entry == "main microtask"));
    assert!(log.iter().any(|entry| entry == "main future start"));
    assert!(log.iter().any(|entry| entry == "main future end"));
    assert!(log.iter().any(|entry| entry == "main timeout"));
    assert!(log.iter().any(|entry| entry == "main interval 1"));
    assert!(log.iter().any(|entry| entry == "main interval 2"));
    assert!(log.iter().any(|entry| entry == "worker task"));
    assert!(log.iter().any(|entry| entry == "worker microtask"));
    assert!(log.iter().any(|entry| entry == "worker future start"));
    assert!(log.iter().any(|entry| entry == "worker future end"));
    assert!(log.iter().any(|entry| entry == "worker timeout to main"));
    assert!(log.iter().any(|entry| entry == "worker exit"));
}

pub fn runtime_waits_for_cross_thread_operation_completion<R: Runtime>() {
    let observed = Arc::new(Mutex::new(None::<usize>));

    {
        let observed = Arc::clone(&observed);
        queue_task::<R, _>(move || {
            let (completion, source) = completion_for_current_thread::<usize>();

            thread::spawn(move || {
                source.complete(7);
            });

            queue_future::<R, _>(async move {
                let value = completion.await;
                *observed.lock().unwrap() = Some(value);
            });
        });
    }

    run::<R>();

    assert_eq!(*observed.lock().unwrap(), Some(7));
}

pub fn zero_interval_fires_once_per_turn_without_spinning<R: Runtime>() {
    // interval(Duration::ZERO, ..) must not busy-spin the event loop.
    // Each tick is one macrotask turn.
    let count = Rc::new(Cell::new(0usize));
    let count_clone = Rc::clone(&count);
    let handle_slot: Rc<RefCell<Option<IntervalHandle>>> = Rc::new(RefCell::new(None));
    let handle_slot_clone = Rc::clone(&handle_slot);

    let handle = interval::<R, _>(Duration::ZERO, move || {
        let next = count_clone.get() + 1;
        count_clone.set(next);
        if next == 5 {
            let handle = handle_slot_clone.borrow_mut().take().unwrap();
            handle.cancel();
        }
    });
    *handle_slot.borrow_mut() = Some(handle);

    run::<R>();

    assert_eq!(count.get(), 5);
}
