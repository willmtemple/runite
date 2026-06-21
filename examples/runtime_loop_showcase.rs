use runite::{
    IntervalHandle, ThreadHandle, clear_interval, current_thread_handle, queue_future,
    queue_microtask, queue_task, set_interval, set_timeout, spawn_worker, yield_now,
};
use std::cell::{Cell, RefCell};
use std::fmt;
use std::rc::Rc;
use std::sync::OnceLock;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

static START: OnceLock<Instant> = OnceLock::new();
static ACTUAL_ORDER: AtomicUsize = AtomicUsize::new(1);

macro_rules! log_event {
    ($expected:literal, $($arg:tt)*) => {{
        log_event_impl($expected, format_args!($($arg)*));
    }};
}

fn log_event_impl(expected: usize, message: fmt::Arguments<'_>) {
    let actual = ACTUAL_ORDER.fetch_add(1, Ordering::SeqCst);
    let elapsed = START
        .get()
        .expect("showcase start time should be initialized")
        .elapsed()
        .as_millis();
    println!(
        "[actual {actual:02} | expected {expected:02} | +{elapsed:04}ms | ts {}] {message}",
        unix_timestamp_millis(),
    );
}

fn unix_timestamp_millis() -> String {
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock should be after the Unix epoch");
    format!("{}.{:03}", now.as_secs(), now.subsec_millis())
}

fn queue_log(handle: &ThreadHandle, expected: usize, message: impl Into<String>) {
    let message = message.into();
    handle
        .queue_task(move || {
            log_event_impl(expected, format_args!("{message}"));
        })
        .unwrap_or_else(|err| panic!("main thread should accept log task {expected}: {err}"));
}

fn queue_log_microtask(handle: &ThreadHandle, expected: usize, message: impl Into<String>) {
    let message = message.into();
    handle
        .queue_task(move || {
            log_event_impl(expected, format_args!("{message}"));
        })
        .unwrap_or_else(|err| panic!("main thread should accept log microtask {expected}: {err}"));
}

#[runite::main]
fn main() {
    START.get_or_init(Instant::now);

    queue_microtask(|| log_event!(1, "[main] boot microtask: prime UI state"));

    queue_future(async {
        log_event!(2, "[main] future: fetch scene metadata");
        yield_now().await;
        log_event!(4, "[main] future: scene metadata cached");
    });

    queue_microtask(|| {
        log_event!(3, "[main] microtask queued immediately");
    });

    let main_handle = current_thread_handle();
    queue_task(move || {
        log_event!(
            5,
            "[main] boot task: paint first frame and start background worker"
        );

        let dashboard_interval = Rc::new(RefCell::new(None::<IntervalHandle>));
        let dashboard_ticks = Rc::new(Cell::new(0usize));
        {
            let slot = Rc::clone(&dashboard_interval);
            let ticks = Rc::clone(&dashboard_ticks);
            set_dashboard_interval(slot, ticks);
        }

        set_timeout(Duration::from_millis(30), || {
            log_event!(11, "[main] timeout: network snapshot ready");
        });

        let main_for_worker = main_handle.clone();
        let worker = spawn_worker(
            move || {
                queue_log(
                    &main_for_worker,
                    6,
                    "[worker->main] startup task: prepare upload queue",
                );

                {
                    let main_for_microtask = main_for_worker.clone();
                    queue_microtask(move || {
                        queue_log(
                            &main_for_microtask,
                            7,
                            "[worker->main] microtask: inspect staging buffers",
                        );
                    });
                }

                {
                    let main_for_future = main_for_worker.clone();
                    queue_future(async move {
                        queue_log(
                            &main_for_future,
                            8,
                            "[worker->main] future: compile shader variants",
                        );
                        yield_now().await;
                        queue_log(
                            &main_for_future,
                            9,
                            "[worker->main] future: shader cache is warm",
                        );
                    });
                }

                {
                    let main_for_task = main_for_worker.clone();
                    queue_task(move || {
                        queue_log(
                            &main_for_task,
                            10,
                            "[worker->main] task: upload static geometry",
                        );
                    });
                }

                let sample_interval = Rc::new(RefCell::new(None::<IntervalHandle>));
                let sample_count = Rc::new(Cell::new(0usize));
                {
                    let slot = Rc::clone(&sample_interval);
                    let count = Rc::clone(&sample_count);
                    let main_for_samples = main_for_worker.clone();
                    let handle = set_interval(Duration::from_millis(40), move || {
                        let next = count.get() + 1;
                        count.set(next);
                        queue_log(
                            &main_for_samples,
                            if next == 1 { 12 } else { 17 },
                            format!("[worker->main] interval: sample batch {next} ready"),
                        );
                        if next == 2 {
                            let interval = slot.borrow_mut().take().expect("interval should exist");
                            clear_interval(&interval);
                            queue_log(&main_for_samples, 18, "[worker->main] interval stopped");
                        }
                    });
                    *sample_interval.borrow_mut() = Some(handle);
                }

                {
                    let main_for_flush = main_for_worker.clone();
                    set_timeout(Duration::from_millis(110), move || {
                        queue_log_microtask(
                            &main_for_flush,
                            20,
                            "[worker->main] timeout: flushed final upload batch",
                        );
                    });
                }
            },
            || log_event!(21, "[main] worker exited"),
        );

        set_timeout(Duration::from_millis(70), move || {
            let queued = worker.queue_task({
                let main_from_remote_task = main_handle.clone();
                move || {
                    queue_log(
                        &main_from_remote_task,
                        15,
                        "[worker->main] remote task: upload late texture atlas",
                    );

                    let main_from_remote_microtask = main_from_remote_task.clone();
                    queue_microtask(move || {
                        queue_log(
                            &main_from_remote_microtask,
                            16,
                            "[worker->main] remote microtask: retire staging pages",
                        );
                    });
                }
            });

            log_event!(
                14,
                "[main] timeout: queue late texture upload on worker (queued={})",
                queued.is_ok()
            );
        });

        set_timeout(Duration::from_millis(140), || {
            log_event!(22, "[main] final timeout: commit frame statistics");
        });
    });
}

fn set_dashboard_interval(slot: Rc<RefCell<Option<IntervalHandle>>>, ticks: Rc<Cell<usize>>) {
    let slot_for_callback = Rc::clone(&slot);
    let handle = set_interval(Duration::from_millis(50), move || {
        let next = ticks.get() + 1;
        ticks.set(next);
        if next == 1 {
            log_event!(13, "[main] interval: dashboard tick 1");
            return;
        }

        let interval = slot_for_callback
            .borrow_mut()
            .take()
            .expect("interval should exist");
        clear_interval(&interval);
        log_event!(19, "[main] interval: dashboard tick 2 and stop");
    });
    *slot.borrow_mut() = Some(handle);
}
