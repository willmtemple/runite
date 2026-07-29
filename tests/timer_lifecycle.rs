use std::cell::Cell;
use std::rc::Rc;
use std::time::Duration;

struct PanicOnDrop;

impl Drop for PanicOnDrop {
    fn drop(&mut self) {
        panic!("timer capture destructor boom");
    }
}

#[test]
fn zero_timeout_cancelled_before_callback_turn_is_suppressed() {
    let fired = std::thread::spawn(|| {
        let fired = Rc::new(Cell::new(false));
        let fired_by_timeout = Rc::clone(&fired);
        let timeout = runite::time::set_timeout(Duration::ZERO, move || {
            fired_by_timeout.set(true);
        });
        runite::queue_microtask(move || timeout.cancel());

        runite::run();
        fired.get()
    })
    .join()
    .expect("timer ordering test thread should finish");

    assert!(!fired);
}

#[test]
fn interval_and_destructor_panics_leave_timer_runtime_reusable() {
    let (ticks, follow_up_ran, timeout_ran) = std::thread::spawn(|| {
        let bomb = PanicOnDrop;
        let cancelled = runite::time::set_timeout(Duration::from_secs(60), move || {
            drop(bomb);
        });
        assert!(
            std::panic::catch_unwind(|| cancelled.cancel()).is_ok(),
            "capture destructor panic should be isolated during cancellation"
        );

        let ticks = Rc::new(Cell::new(0usize));
        let ticks_by_interval = Rc::clone(&ticks);
        let _interval = runite::time::set_interval(Duration::ZERO, move || {
            ticks_by_interval.set(ticks_by_interval.get() + 1);
            panic!("interval callback boom");
        });
        let follow_up_ran = Rc::new(Cell::new(false));
        let follow_up = Rc::clone(&follow_up_ran);
        runite::queue_macrotask(move || follow_up.set(true));
        runite::run();

        let timeout_ran = Rc::new(Cell::new(false));
        let timeout_ran_by_callback = Rc::clone(&timeout_ran);
        runite::time::set_timeout(Duration::ZERO, move || {
            timeout_ran_by_callback.set(true);
        });
        runite::run();

        (ticks.get(), follow_up_ran.get(), timeout_ran.get())
    })
    .join()
    .expect("timer lifecycle test thread should finish");

    assert_eq!(ticks, 1);
    assert!(follow_up_ran);
    assert!(timeout_ran);
}

/// `cancel_on_drop` turns the cloneable token into a scope guard.
///
/// The plain handles deliberately do not cancel on drop; this test pins that
/// the wrapper does, and that `into_inner` opts back out without cancelling.
#[test]
fn cancel_on_drop_stops_an_interval_at_the_end_of_scope() {
    use std::cell::Cell;
    use std::rc::Rc;

    let ticks = Rc::new(Cell::new(0u32));

    let counter = Rc::clone(&ticks);
    runite::queue_macrotask(move || {
        let guard = runite::time::set_interval(Duration::from_millis(1), move || {
            counter.set(counter.get() + 1);
        })
        .cancel_on_drop();
        // Dropping here must both stop the callbacks and release the runtime,
        // which is the part a leaked interval gets wrong: an uncancelled
        // interval keeps `run()` from ever returning.
        drop(guard);
    });

    runite::run();
    assert_eq!(ticks.get(), 0, "the interval should never have fired");
}

/// `into_inner` hands the timer to a longer-lived owner without cancelling.
#[test]
fn into_inner_releases_the_guard_without_cancelling() {
    use std::cell::Cell;
    use std::rc::Rc;

    let fired = Rc::new(Cell::new(false));
    let flag = Rc::clone(&fired);

    runite::queue_macrotask(move || {
        let guard = runite::time::set_timeout(Duration::from_millis(1), move || {
            flag.set(true);
        })
        .cancel_on_drop();
        // Escaping the guard must not cancel; the timeout still owes a callback.
        let token = guard.into_inner();
        let _ = token;
    });

    runite::run();
    assert!(
        fired.get(),
        "into_inner must not cancel the timeout it releases"
    );
}

/// A shutdown hook runs at teardown, and runs even though `run()` has already
/// returned — which is the case `process::exit` was previously the only answer
/// for.
#[test]
fn shutdown_hooks_run_at_thread_teardown() {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};

    let order = Arc::new(AtomicUsize::new(0));
    let observed = Arc::clone(&order);

    std::thread::spawn(move || {
        runite::queue_macrotask({
            let observed = Arc::clone(&observed);
            move || {
                runite::on_shutdown({
                    let observed = Arc::clone(&observed);
                    move || {
                        observed.fetch_add(1, Ordering::AcqRel);
                    }
                });
                runite::on_shutdown(move || {
                    observed.fetch_add(10, Ordering::AcqRel);
                });
            }
        });
        runite::run();
        // Still zero here: hooks are keyed to teardown, not to `run` returning.
        0
    })
    .join()
    .expect("runtime thread should not panic");

    assert_eq!(
        order.load(Ordering::Acquire),
        11,
        "both hooks should have run once the thread's runtime was torn down"
    );
}
