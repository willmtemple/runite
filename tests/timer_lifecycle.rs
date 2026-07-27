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
