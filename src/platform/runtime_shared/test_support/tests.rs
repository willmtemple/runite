use std::panic::AssertUnwindSafe;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

use super::*;

const TEST_TIMEOUT: Duration = Duration::from_secs(5);
const DELIVERY_TIMEOUT: Duration = Duration::from_millis(500);

struct PanicOnDropFuture;

impl Future for PanicOnDropFuture {
    type Output = ();

    fn poll(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Self::Output> {
        Poll::Pending
    }
}

impl Drop for PanicOnDropFuture {
    fn drop(&mut self) {
        panic!("future destructor boom");
    }
}

struct GatedDropFuture {
    gate: ExecutionGate,
}

impl Future for GatedDropFuture {
    type Output = ();

    fn poll(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Self::Output> {
        Poll::Pending
    }
}

impl Drop for GatedDropFuture {
    fn drop(&mut self) {
        self.gate.arrive_and_wait();
        self.gate.mark_completed();
    }
}

struct GatedThreadExit {
    gate: ExecutionGate,
}

impl Drop for GatedThreadExit {
    fn drop(&mut self) {
        self.gate.arrive_and_wait();
        self.gate.mark_completed();
    }
}

thread_local! {
    static GATED_THREAD_EXIT: RefCell<Option<GatedThreadExit>> =
        const { RefCell::new(None) };
}

fn gate_current_thread_exit(gate: ExecutionGate) {
    GATED_THREAD_EXIT.with(|slot| {
        let previous = slot.borrow_mut().replace(GatedThreadExit { gate });
        assert!(previous.is_none(), "thread-exit gate already installed");
    });
}

struct PanicOnDropValue;

impl Drop for PanicOnDropValue {
    fn drop(&mut self) {
        panic!("timer capture destructor boom");
    }
}

struct RemoteWakeState {
    ready: AtomicBool,
    waker: Mutex<Option<Waker>>,
}

impl RemoteWakeState {
    fn new() -> Arc<Self> {
        Arc::new(Self {
            ready: AtomicBool::new(false),
            waker: Mutex::new(None),
        })
    }

    fn complete(&self) {
        self.ready.store(true, Ordering::Release);
        if let Some(waker) = self.waker.lock().expect("remote waker poisoned").take() {
            waker.wake();
        }
    }
}

struct RemoteWakeFuture {
    state: Arc<RemoteWakeState>,
}

impl Future for RemoteWakeFuture {
    type Output = ();

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        if self.state.ready.load(Ordering::Acquire) {
            return Poll::Ready(());
        }
        *self.state.waker.lock().expect("remote waker poisoned") = Some(cx.waker().clone());
        if self.state.ready.load(Ordering::Acquire) {
            let _ = self
                .state
                .waker
                .lock()
                .expect("remote waker poisoned")
                .take();
            Poll::Ready(())
        } else {
            Poll::Pending
        }
    }
}

struct TeardownReentrantFuture {
    dropped: Arc<AtomicBool>,
    run_rejected: Arc<AtomicBool>,
}

impl Future for TeardownReentrantFuture {
    type Output = ();

    fn poll(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Self::Output> {
        Poll::Pending
    }
}

impl Drop for TeardownReentrantFuture {
    fn drop(&mut self) {
        self.dropped.store(true, Ordering::Release);
        let reentry = std::panic::catch_unwind(AssertUnwindSafe(run::<MockRuntime>));
        self.run_rejected.store(reentry.is_err(), Ordering::Release);
        queue_task::<MockRuntime, _>(|| {});
    }
}

struct NonParentJoiner {
    control: MockDriverControl,
    registered: mpsc::Receiver<()>,
    result: mpsc::Receiver<Option<bool>>,
    thread: thread::JoinHandle<()>,
}

fn assert_non_parent_worker_joiners_complete(joiner_count: usize) {
    let harness = MockRuntimeHarness::new();
    let _worker_driver = harness.plan_driver();
    let _parent_driver = harness.plan_driver();
    let start_gate = ExecutionGate::default();
    harness.gate_next_thread_spawn(start_gate.clone());

    harness.enter(|| {
        let worker = Arc::new(spawn_worker::<MockRuntime, _, _>(|| {}, || {}));
        let _release = start_gate.release_on_drop();
        assert!(
            start_gate.wait_until_arrived(TEST_TIMEOUT),
            "worker should remain gated while non-parent joiners register"
        );

        let mut joiners = Vec::with_capacity(joiner_count);
        for index in 0..joiner_count {
            let joiner_harness = MockRuntimeHarness::new();
            let control = joiner_harness.plan_driver();
            let worker = Arc::clone(&worker);
            let (registered_sender, registered) = mpsc::sync_channel(1);
            let (result_sender, result) = mpsc::sync_channel(1);
            let thread = thread::Builder::new()
                .name(format!("runite-non-parent-joiner-{index}"))
                .spawn(move || {
                    let outcome = Arc::new(Mutex::new(None));
                    let outcome_by_task = Arc::clone(&outcome);
                    joiner_harness.enter(|| {
                        queue_future::<MockRuntime, _>(async move {
                            *outcome_by_task.lock().expect("join outcome poisoned") =
                                Some(worker.join().await.is_ok());
                        });
                        super::super::run_until_stalled::<MockRuntime>();
                        registered_sender
                            .send(())
                            .expect("joiner registration should be observed");
                        run::<MockRuntime>();
                    });
                    let outcome = outcome.lock().expect("join outcome poisoned").take();
                    let _ = result_sender.send(outcome);
                })
                .expect("non-parent joiner should spawn");
            joiners.push(NonParentJoiner {
                control,
                registered,
                result,
                thread,
            });
        }

        for joiner in &joiners {
            joiner
                .registered
                .recv_timeout(TEST_TIMEOUT)
                .expect("joiner should register before worker release");
        }
        let all_parked = joiners
            .iter()
            .all(|joiner| joiner.control.wait_until_waiting(DELIVERY_TIMEOUT));

        start_gate.release();
        run::<MockRuntime>();

        let mut all_completed = true;
        for joiner in joiners {
            let result = joiner.result.recv_timeout(TEST_TIMEOUT);
            if result.is_err() {
                joiner.control.fail_next_wait(
                    io::ErrorKind::Interrupted,
                    "joiner did not wake after worker completion",
                );
            }
            let thread_finished = joiner.thread.join().is_ok();
            all_completed &= matches!(result, Ok(Some(true))) && thread_finished;
        }

        assert!(
            all_parked,
            "pending non-parent join tasks quiesced instead of retaining runtime liveness"
        );
        assert!(
            all_completed,
            "every pending joiner must observe worker completion"
        );
    });
}

#[test]
fn mock_notifier_returns_scripted_failure() {
    let harness = MockRuntimeHarness::new();
    let control = harness.plan_driver();
    control.fail_next_notify(io::ErrorKind::PermissionDenied, "notification denied");

    let (driver, notifier) = harness
        .enter(MockRuntime::create_driver_pair)
        .expect("mock driver should initialize");
    let error = notifier
        .notify()
        .expect_err("scripted notification should fail");

    assert_eq!(error.kind(), io::ErrorKind::PermissionDenied);
    assert!(
        harness
            .trace()
            .snapshot()
            .contains(&MockRuntimeEvent::NotifyFailed(
                control.id(),
                io::ErrorKind::PermissionDenied
            ))
    );
    drop(notifier);
    drop(driver);
}

#[test]
fn mock_controller_cancellation_unblocks_waiter() {
    let harness = MockRuntimeHarness::new();
    let control = harness.plan_driver();
    let (driver, _notifier) = harness
        .enter(MockRuntime::create_driver_pair)
        .expect("mock driver should initialize");

    thread::scope(|scope| {
        let waiter = scope.spawn(move || driver.wait());
        let cancellation = control.cancel_wait_on_drop();
        let waiting = control.wait_until_waiting(TEST_TIMEOUT);
        drop(cancellation);

        let error = waiter
            .join()
            .expect("mock waiter should not panic")
            .expect_err("controller cancellation should fail the wait");
        assert!(waiting, "mock driver should enter its wait");
        assert_eq!(error.kind(), io::ErrorKind::Interrupted);
    });
}

#[test]
fn mock_runtime_dispatches_controlled_timer() {
    let harness = MockRuntimeHarness::new();
    let control = harness.plan_driver();
    let fired = Arc::new(AtomicBool::new(false));
    let fired_by_timer = Arc::clone(&fired);

    thread::scope(|scope| {
        let controller = control.clone();
        let trigger = scope.spawn(move || {
            let cancellation = controller.cancel_wait_on_drop();
            assert!(
                controller.wait_until_waiting(TEST_TIMEOUT),
                "runtime should wait for its timer"
            );
            controller.set_time(Duration::from_millis(10));
            controller.fire_timer(1);
            cancellation.disarm();
        });

        harness.enter(|| {
            timeout::<MockRuntime, _>(Duration::from_millis(10), move || {
                fired_by_timer.store(true, Ordering::Release);
            });
            run::<MockRuntime>();
        });
        trigger.join().expect("timer controller should finish");
    });

    assert!(fired.load(Ordering::Acquire));
    assert_eq!(
        control.timer_rearms(),
        vec![Some(Duration::from_millis(10)), None]
    );
}

#[test]
fn expired_timeout_can_be_cancelled_before_its_macrotask_runs() {
    let harness = MockRuntimeHarness::new();
    let control = harness.plan_driver();
    control.fire_timer(1);
    let fired = Rc::new(Cell::new(false));

    harness.enter(|| {
        let fired_by_timeout = Rc::clone(&fired);
        let handle = timeout::<MockRuntime, _>(Duration::ZERO, move || {
            fired_by_timeout.set(true);
        });
        queue_microtask::<MockRuntime, _>(move || handle.cancel());
        run::<MockRuntime>();
    });

    assert!(
        !fired.get(),
        "cancellation must suppress an expired callback still queued as a macrotask"
    );
}

#[test]
fn panicking_zero_interval_is_cancelled_and_loop_remains_usable() {
    let harness = MockRuntimeHarness::new();
    let callback_count = Rc::new(Cell::new(0usize));
    let follow_up_ran = Rc::new(Cell::new(false));

    harness.enter(|| {
        let callback_count_by_interval = Rc::clone(&callback_count);
        let interval = interval::<MockRuntime, _>(Duration::ZERO, move || {
            callback_count_by_interval.set(callback_count_by_interval.get() + 1);
            panic!("interval callback boom");
        });
        let follow_up = Rc::clone(&follow_up_ran);
        queue_task::<MockRuntime, _>(move || follow_up.set(true));

        run::<MockRuntime>();
        interval.cancel();
        run::<MockRuntime>();
    });

    assert_eq!(callback_count.get(), 1);
    assert!(
        follow_up_ran.get(),
        "an isolated interval panic must not strand later macrotasks"
    );
}

#[test]
fn timer_capture_destructor_panic_cannot_skip_rearm_or_callback_cleanup() {
    let harness = MockRuntimeHarness::new();
    let control = harness.plan_driver();
    let follow_up_fired = Rc::new(Cell::new(false));

    harness.enter(|| {
        let bomb = PanicOnDropValue;
        let cancelled = timeout::<MockRuntime, _>(Duration::from_millis(5), move || {
            drop(bomb);
        });
        let follow_up_fired_by_timer = Rc::clone(&follow_up_fired);
        timeout::<MockRuntime, _>(Duration::from_millis(10), move || {
            follow_up_fired_by_timer.set(true);
        });

        assert!(
            std::panic::catch_unwind(AssertUnwindSafe(|| cancelled.cancel())).is_ok(),
            "timer cancellation must isolate capture destructor panics"
        );
        control.set_time(Duration::from_millis(10));
        control.fire_timer(1);
        run::<MockRuntime>();
    });

    assert!(
        follow_up_fired.get(),
        "a destructor panic must not leave the driver armed to the removed timer"
    );
}

#[test]
fn timer_teardown_panic_allows_sequential_runtime_reuse() {
    let first = MockRuntimeHarness::new();
    let _first_driver = first.plan_driver();
    first.enter(|| {
        let bomb = PanicOnDropValue;
        timeout::<MockRuntime, _>(Duration::from_secs(60), move || {
            drop(bomb);
        });
    });

    let second = MockRuntimeHarness::new();
    let _second_driver = second.plan_driver();
    let ran = Rc::new(Cell::new(false));
    second.enter(|| {
        let ran_after_reuse = Rc::clone(&ran);
        queue_task::<MockRuntime, _>(move || ran_after_reuse.set(true));
        run::<MockRuntime>();
    });

    assert!(
        ran.get(),
        "timer destructor panic during teardown must not poison later runtime state"
    );
}

#[test]
fn successful_mock_notification_wakes_waiting_runtime() {
    let harness = MockRuntimeHarness::new();
    let control = harness.plan_driver();
    let ran = Arc::new(AtomicBool::new(false));

    thread::scope(|scope| {
        harness.enter(|| {
            let handle = current_thread_handle::<MockRuntime>();
            let timer = timeout::<MockRuntime, _>(Duration::from_secs(60), || {
                panic!("control timer must be cancelled")
            });
            let controller = control.clone();
            let ran_remotely = Arc::clone(&ran);
            let sender = scope.spawn(move || {
                let cancellation = controller.cancel_wait_on_drop();
                assert!(
                    controller.wait_until_waiting(TEST_TIMEOUT),
                    "runtime should enter its driver wait"
                );
                handle
                    .queue_macrotask(move || {
                        timer.cancel();
                        ran_remotely.store(true, Ordering::Release);
                    })
                    .expect("remote task should queue");
                cancellation.disarm();
            });

            run::<MockRuntime>();
            sender.join().expect("remote sender should finish");
        });
    });

    assert!(ran.load(Ordering::Acquire));
    let trace = harness.trace().snapshot();
    assert!(
        trace
            .iter()
            .any(|event| matches!(event, MockRuntimeEvent::NotifySucceeded(_)))
    );
    assert!(
        trace
            .iter()
            .any(|event| matches!(event, MockRuntimeEvent::WakeDrained(_, 1)))
    );
}

#[test]
fn sequential_drivers_reuse_runtime_state_and_driver_identity() {
    let harness = MockRuntimeHarness::new();
    let control = harness.plan_driver();
    let ran_between_entries = Arc::new(AtomicBool::new(false));

    let handle = harness.enter(|| {
        let first = current_thread_handle::<MockRuntime>();
        run::<MockRuntime>();

        assert!(!first.is_closed(), "an idle runtime must remain open");
        assert!(first.is_current(), "idle state must remain installed");

        let ran_between_entries = Arc::clone(&ran_between_entries);
        first
            .queue_macrotask(move || ran_between_entries.store(true, Ordering::Release))
            .expect("an idle runtime must accept work for its next entry");
        run::<MockRuntime>();

        let second = current_thread_handle::<MockRuntime>();
        assert!(
            Arc::ptr_eq(&first.shared, &second.shared),
            "sequential entries must retain the same thread state"
        );
        assert_eq!(block_on::<MockRuntime, _>(async { 17usize }), 17);
        first
    });

    assert!(ran_between_entries.load(Ordering::Acquire));
    assert!(
        harness.created_driver(1).is_none(),
        "sequential entries must not create a second driver"
    );
    assert_eq!(
        harness
            .trace()
            .snapshot()
            .iter()
            .filter(|event| **event == MockRuntimeEvent::DriverCreated(control.id()))
            .count(),
        1
    );
    assert!(
        handle.is_closed(),
        "the harness's final thread teardown must close retained handles"
    );
}

#[test]
fn run_quiescence_cancels_inert_tasks_and_wakes_join_handles() {
    let harness = MockRuntimeHarness::new();
    let reentrant = ReentrantWaker::new(|| {});

    harness.enter(|| {
        let mut handle = queue_future::<MockRuntime, _>(std::future::pending::<usize>());
        let waker = reentrant.waker();
        let mut context = Context::from_waker(&waker);
        assert!(Pin::new(&mut handle).poll(&mut context).is_pending());

        run::<MockRuntime>();

        assert!(handle.is_finished());
        assert_eq!(
            reentrant.wake_count(),
            1,
            "shutdown cancellation must wake a retained join handle"
        );
        let error = block_on::<MockRuntime, _>(handle)
            .expect_err("an inert task must not remain pending after run quiescence");
        assert!(error.is_cancelled());
    });
}

#[test]
fn panicking_future_destructor_cannot_skip_terminal_cleanup() {
    let harness = MockRuntimeHarness::new();
    let follow_up_ran = Rc::new(Cell::new(false));

    harness.enter(|| {
        let handle = queue_future::<MockRuntime, _>(PanicOnDropFuture);
        let run_result = std::panic::catch_unwind(AssertUnwindSafe(run::<MockRuntime>));

        assert!(
            run_result.is_ok(),
            "a future destructor panic must be isolated"
        );
        assert!(handle.is_finished());
        assert!(
            block_on::<MockRuntime, _>(handle)
                .expect_err("the inert task must be cancelled")
                .is_cancelled()
        );

        let follow_up = Rc::clone(&follow_up_ran);
        queue_task::<MockRuntime, _>(move || follow_up.set(true));
        run::<MockRuntime>();
    });

    assert!(
        follow_up_ran.get(),
        "the runtime must remain re-enterable after destructor isolation"
    );
}

#[test]
fn abort_commits_terminal_state_before_panicking_future_destructor() {
    let harness = MockRuntimeHarness::new();

    harness.enter(|| {
        let handle = queue_future::<MockRuntime, _>(PanicOnDropFuture);
        super::super::run_until_stalled::<MockRuntime>();

        let abort = std::panic::catch_unwind(AssertUnwindSafe(|| handle.abort()));
        assert!(abort.is_ok(), "abort must isolate future destructor panic");
        assert!(handle.is_finished());
        assert!(
            block_on::<MockRuntime, _>(handle)
                .expect_err("aborted task must have a terminal result")
                .is_aborted()
        );
    });
}

#[test]
fn final_task_cleanup_allows_tls_reentry_without_aliasing_state() {
    let harness = MockRuntimeHarness::new();
    let future_dropped = Arc::new(AtomicBool::new(false));
    let run_rejected = Arc::new(AtomicBool::new(false));
    let join_waker_reentered = Arc::new(AtomicBool::new(false));

    harness.enter(|| {
        let mut handle = queue_future::<MockRuntime, _>(TeardownReentrantFuture {
            dropped: Arc::clone(&future_dropped),
            run_rejected: Arc::clone(&run_rejected),
        });
        super::super::run_until_stalled::<MockRuntime>();

        let reentered = Arc::clone(&join_waker_reentered);
        let waker = ReentrantWaker::new(move || {
            let handle = current_thread_handle::<MockRuntime>();
            assert!(handle.is_current());
            assert!(handle.is_closed());
            queue_task::<MockRuntime, _>(|| {});
            reentered.store(true, Ordering::Release);
        })
        .waker();
        let mut context = Context::from_waker(&waker);
        assert!(Pin::new(&mut handle).poll(&mut context).is_pending());
    });

    assert!(future_dropped.load(Ordering::Acquire));
    assert!(run_rejected.load(Ordering::Acquire));
    assert!(join_waker_reentered.load(Ordering::Acquire));
}

#[test]
fn teardown_prevents_lazy_runtime_reinstallation_from_driver_drop() {
    let harness = MockRuntimeHarness::new();
    let control = harness.plan_driver();
    let reinstall_rejected = Arc::new(AtomicBool::new(false));
    let reinstall_rejected_on_drop = Arc::clone(&reinstall_rejected);
    control.on_driver_drop(move || {
        let reinstall =
            std::panic::catch_unwind(AssertUnwindSafe(current_thread_handle::<MockRuntime>));
        reinstall_rejected_on_drop.store(reinstall.is_err(), Ordering::Release);
    });

    harness.enter(|| {
        let _ = current_thread_handle::<MockRuntime>();
    });

    assert!(reinstall_rejected.load(Ordering::Acquire));
    assert!(
        harness.created_driver(1).is_none(),
        "teardown must not lazily install a replacement driver"
    );
}

#[test]
fn runtime_owned_worker_tears_down_before_parent_exit_callback() {
    let harness = MockRuntimeHarness::new();
    let worker_driver = harness.plan_driver();
    let _parent_driver = harness.plan_driver();
    let on_exit_ran = Arc::new(AtomicBool::new(false));

    harness.enter(|| {
        let trace = harness.trace();
        let on_exit_ran = Arc::clone(&on_exit_ran);
        let _worker = spawn_worker::<MockRuntime, _, _>(
            || {},
            move || {
                let events = trace.snapshot();
                assert!(events.contains(&MockRuntimeEvent::DriverUnbound(worker_driver.id())));
                assert!(events.contains(&MockRuntimeEvent::DriverDropped(worker_driver.id())));
                on_exit_ran.store(true, Ordering::Release);
            },
        );
        run::<MockRuntime>();
    });

    assert!(on_exit_ran.load(Ordering::Acquire));
}

#[test]
fn worker_completion_waits_for_pending_future_destructor() {
    let harness = MockRuntimeHarness::new();
    let worker_driver = harness.plan_driver();
    let _parent_driver = harness.plan_driver();
    let destructor_gate = ExecutionGate::default();
    let on_exit_ran = Arc::new(AtomicBool::new(false));

    harness.enter(|| {
        let gate_on_worker = destructor_gate.clone();
        let on_exit_ran_by_parent = Arc::clone(&on_exit_ran);
        let worker = spawn_worker::<MockRuntime, _, _>(
            move || {
                queue_future::<MockRuntime, _>(GatedDropFuture {
                    gate: gate_on_worker,
                });
            },
            move || on_exit_ran_by_parent.store(true, Ordering::Release),
        );
        let _release = destructor_gate.release_on_drop();

        assert!(
            destructor_gate.wait_until_arrived(TEST_TIMEOUT),
            "worker should reach pending-future destruction"
        );
        assert!(
            !worker.is_finished(),
            "completion must not publish while a worker destructor is pending"
        );
        assert!(!on_exit_ran.load(Ordering::Acquire));
        assert!(
            !harness
                .trace()
                .snapshot()
                .contains(&MockRuntimeEvent::DriverDropped(worker_driver.id())),
            "driver teardown must follow pending user destructors"
        );

        destructor_gate.release();
        run::<MockRuntime>();

        assert!(destructor_gate.wait_until_completed(TEST_TIMEOUT));
        assert!(worker.is_finished());
        assert!(block_on::<MockRuntime, _>(worker.join()).is_ok());
    });

    assert!(on_exit_ran.load(Ordering::Acquire));
}

#[test]
fn worker_completion_waits_for_os_exit_without_blocking_parent_drain() {
    let harness = MockRuntimeHarness::new();
    let _worker_driver = harness.plan_driver();
    let _parent_driver = harness.plan_driver();
    let exit_gate = ExecutionGate::default();
    let parent_progress = EventTrace::new();
    let on_exit_ran = Arc::new(AtomicBool::new(false));

    harness.enter(|| {
        // Signal after the poll and again after the drop, so a stall names the
        // operation that stalled. Both take the completion mutex, and a single
        // combined signal cannot tell "poll blocked" from "drop blocked" from
        // "join resolved early" -- three different bugs.
        #[derive(Debug)]
        enum JoinPollStage {
            Polled(bool),
            Dropped,
        }

        // The poller must be running *before* the worker parks in its TLS
        // destructor. On Windows a thread inside DLL_THREAD_DETACH holds the
        // loader lock, which stops any newly created thread from executing its
        // first instruction, so a poller spawned after the worker parked would
        // not start until the worker was released -- measuring the loader lock
        // instead of the join future. It waits here for the future instead.
        let (join_sender, join_receiver) = mpsc::channel::<super::super::handles::WorkerJoin>();
        let (poll_sender, poll_receiver) = mpsc::sync_channel(2);
        let poller = TrackedThread::new(
            thread::Builder::new()
                .name("runite-worker-join-poller".into())
                .spawn(move || {
                    let Ok(mut join) = join_receiver.recv() else {
                        return;
                    };
                    let reentrant = ReentrantWaker::new(|| {});
                    let waker = reentrant.waker();
                    let mut context = Context::from_waker(&waker);
                    let pending = Pin::new(&mut join).poll(&mut context).is_pending();
                    let _ = poll_sender.send(JoinPollStage::Polled(pending));
                    drop(join);
                    let _ = poll_sender.send(JoinPollStage::Dropped);
                })
                .expect("worker join poller should spawn"),
        );

        // Same constraint: this thread releases the gate, so it has to be
        // running before the worker takes the loader lock, or nothing can ever
        // let the worker go.
        let progress_for_controller = parent_progress.clone();
        let gate_for_controller = exit_gate.clone();
        let controller = TrackedThread::new(
            thread::Builder::new()
                .name("runite-worker-exit-controller".into())
                .spawn(move || {
                    let progressed = progress_for_controller.wait_for_len(1, DELIVERY_TIMEOUT);
                    gate_for_controller.release();
                    progressed
                })
                .expect("worker exit controller should spawn"),
        );

        let gate_on_worker = exit_gate.clone();
        let on_exit_ran_by_parent = Arc::clone(&on_exit_ran);
        let worker = spawn_worker::<MockRuntime, _, _>(
            move || gate_current_thread_exit(gate_on_worker),
            move || on_exit_ran_by_parent.store(true, Ordering::Release),
        );
        let _release = exit_gate.release_on_drop();
        assert!(
            exit_gate.wait_until_arrived(TEST_TIMEOUT),
            "worker should reach its OS-thread TLS destructor"
        );
        assert!(
            !worker.is_finished(),
            "completion must remain unpublished until the OS thread exits"
        );
        assert!(!on_exit_ran.load(Ordering::Acquire));

        join_sender
            .send(worker.join())
            .expect("join poller should still be waiting");

        let polled = poll_receiver.recv_timeout(DELIVERY_TIMEOUT).ok();
        let dropped = polled.is_some() && poll_receiver.recv_timeout(DELIVERY_TIMEOUT).is_ok();
        if polled.is_none() || !dropped {
            // Let the worker finish so the helper threads can exit.
            exit_gate.release();
        }

        match polled {
            None => panic!("polling the join future blocked on the worker OS thread"),
            Some(JoinPollStage::Polled(false)) => {
                panic!("join resolved before the worker OS thread exited")
            }
            Some(JoinPollStage::Polled(true)) => assert!(
                dropped,
                "dropping the join future blocked on the worker OS thread"
            ),
            Some(JoinPollStage::Dropped) => {
                unreachable!("the poller signals its stages in order")
            }
        }

        let progress_on_parent = parent_progress.clone();
        queue_task::<MockRuntime, _>(move || progress_on_parent.record(()));

        super::super::run_until_stalled::<MockRuntime>();
        assert!(
            controller
                .join()
                .expect("worker exit controller should finish"),
            "parent drain blocked on OS join before running ready parent work"
        );
        // Only now that the gate is released can a helper thread finish: on
        // Windows its own TLS teardown needs the loader lock the parked worker
        // was holding.
        poller.join().expect("worker join poller should finish");

        run::<MockRuntime>();
        assert!(exit_gate.wait_until_completed(TEST_TIMEOUT));
        assert!(worker.is_finished());
        assert!(block_on::<MockRuntime, _>(worker.join()).is_ok());
        assert!(on_exit_ran.load(Ordering::Acquire));
    });
}

#[test]
fn dropping_pending_worker_join_unregisters_its_waker() {
    let harness = MockRuntimeHarness::new();
    let _worker_driver = harness.plan_driver();
    let _parent_driver = harness.plan_driver();
    let start_gate = ExecutionGate::default();
    harness.gate_next_thread_spawn(start_gate.clone());

    harness.enter(|| {
        let worker = spawn_worker::<MockRuntime, _, _>(|| {}, || {});
        let parent = current_thread_handle::<MockRuntime>();
        let _release = start_gate.release_on_drop();
        assert!(
            start_gate.wait_until_arrived(TEST_TIMEOUT),
            "worker should remain gated while join is registered"
        );

        let reentrant = ReentrantWaker::new(|| {});
        let waker = reentrant.waker();
        let mut context = Context::from_waker(&waker);
        let mut join = worker.join();
        assert!(Pin::new(&mut join).poll(&mut context).is_pending());
        assert_eq!(worker.completion.waiter_count(), 1);
        assert_eq!(parent.shared.pending_ops.load(Ordering::Acquire), 1);
        assert!(Pin::new(&mut join).poll(&mut context).is_pending());
        assert_eq!(
            worker.completion.waiter_count(),
            1,
            "repolling must update rather than duplicate waiter registration"
        );
        assert_eq!(
            parent.shared.pending_ops.load(Ordering::Acquire),
            1,
            "repolling must not duplicate runtime liveness"
        );

        drop(join);
        assert_eq!(worker.completion.waiter_count(), 0);
        assert_eq!(
            parent.shared.pending_ops.load(Ordering::Acquire),
            0,
            "dropping join must release runtime liveness exactly once"
        );
        start_gate.release();
        run::<MockRuntime>();
        assert_eq!(
            reentrant.wake_count(),
            0,
            "a dropped join future must not retain or wake its old waker"
        );
        assert!(block_on::<MockRuntime, _>(worker.join()).is_ok());
    });
}

#[test]
fn dropping_join_after_publication_drains_invalidates_queued_waker() {
    let harness = MockRuntimeHarness::new();
    let _worker_driver = harness.plan_driver();
    let _parent_driver = harness.plan_driver();
    let start_gate = ExecutionGate::default();
    harness.gate_next_thread_spawn(start_gate.clone());
    let first_wake_gate = ExecutionGate::default();

    harness.enter(|| {
        let worker = spawn_worker::<MockRuntime, _, _>(|| {}, || {});
        let _start_release = start_gate.release_on_drop();
        let _wake_release = first_wake_gate.release_on_drop();
        assert!(
            start_gate.wait_until_arrived(TEST_TIMEOUT),
            "worker should remain gated while joiners register"
        );

        let gate_on_first_wake = first_wake_gate.clone();
        let first_waker = ReentrantWaker::new(move || gate_on_first_wake.arrive_and_wait());
        let second_waker = ReentrantWaker::new(|| {});
        let first_waker_value = first_waker.waker();
        let second_waker_value = second_waker.waker();
        let mut first_context = Context::from_waker(&first_waker_value);
        let mut second_context = Context::from_waker(&second_waker_value);
        let mut first_join = worker.join();
        let mut second_join = worker.join();
        assert!(
            Pin::new(&mut first_join)
                .poll(&mut first_context)
                .is_pending()
        );
        assert!(
            Pin::new(&mut second_join)
                .poll(&mut second_context)
                .is_pending()
        );

        start_gate.release();
        assert!(
            first_wake_gate.wait_until_arrived(TEST_TIMEOUT),
            "first join waker should block publication wake delivery"
        );
        drop(second_join);
        first_wake_gate.release();
        drop(first_join);

        assert!(
            harness.trace().wait_until(TEST_TIMEOUT, |events| {
                events.contains(&MockRuntimeEvent::ThreadJoined)
            }),
            "worker reaper should finish after blocked wake is released"
        );
        assert_eq!(first_waker.wake_count(), 1);
        assert_eq!(
            second_waker.wake_count(),
            0,
            "dropping a drained waiter must invalidate its queued wake"
        );
        run::<MockRuntime>();
    });
}

#[test]
fn non_parent_worker_join_keeps_its_runtime_live() {
    assert_non_parent_worker_joiners_complete(1);
}

#[test]
fn concurrent_worker_joiners_all_keep_their_runtimes_live() {
    assert_non_parent_worker_joiners_complete(3);
}

#[test]
fn explicit_worker_teardown_panic_is_reported_by_join() {
    let harness = MockRuntimeHarness::new();
    let worker_driver = harness.plan_driver();
    worker_driver.on_driver_drop(|| panic!("worker driver destructor boom"));
    let _parent_driver = harness.plan_driver();
    let on_exit_ran = Arc::new(AtomicBool::new(false));

    harness.enter(|| {
        let on_exit_ran_by_parent = Arc::clone(&on_exit_ran);
        let worker = spawn_worker::<MockRuntime, _, _>(
            || {},
            move || on_exit_ran_by_parent.store(true, Ordering::Release),
        );

        let error = block_on::<MockRuntime, _>(worker.join())
            .expect_err("teardown panic must fail worker join");
        assert!(error.is_runtime_panicked());
        run::<MockRuntime>();
    });

    assert!(on_exit_ran.load(Ordering::Acquire));
}

#[test]
fn parent_run_tracks_worker_after_public_handle_is_dropped() {
    let harness = MockRuntimeHarness::new();
    let _worker_driver = harness.plan_driver();
    let _parent_driver = harness.plan_driver();
    let worker_ran = Arc::new(AtomicBool::new(false));
    let on_exit_ran = Arc::new(AtomicBool::new(false));

    harness.enter(|| {
        let worker_ran_on_thread = Arc::clone(&worker_ran);
        let on_exit_ran_by_parent = Arc::clone(&on_exit_ran);
        let worker = spawn_worker::<MockRuntime, _, _>(
            move || {
                thread::sleep(Duration::from_millis(10));
                worker_ran_on_thread.store(true, Ordering::Release);
            },
            move || on_exit_ran_by_parent.store(true, Ordering::Release),
        );
        drop(worker);

        run::<MockRuntime>();
    });

    assert!(worker_ran.load(Ordering::Acquire));
    assert!(
        on_exit_ran.load(Ordering::Acquire),
        "parent shutdown must wait for worker teardown and its exit callback"
    );
}

#[test]
fn worker_join_is_awaitable_from_parent_and_repeatable() {
    let harness = MockRuntimeHarness::new();
    let worker_driver = harness.plan_driver();
    let _parent_driver = harness.plan_driver();
    let joined = Rc::new(Cell::new(false));

    harness.enter(|| {
        let worker = spawn_worker::<MockRuntime, _, _>(|| {}, || {});
        let joined_by_parent = Rc::clone(&joined);
        queue_future::<MockRuntime, _>(async move {
            assert!(worker.join().await.is_ok());
            assert!(worker.is_finished());
            assert!(worker.join().await.is_ok());
            joined_by_parent.set(true);
        });

        run::<MockRuntime>();
    });

    assert!(joined.get());
    let events = harness.trace().snapshot();
    assert!(events.contains(&MockRuntimeEvent::DriverUnbound(worker_driver.id())));
    assert!(events.contains(&MockRuntimeEvent::DriverDropped(worker_driver.id())));
}

#[test]
fn worker_runtime_panic_is_reported_by_repeated_join_observation() {
    let harness = MockRuntimeHarness::new();
    let worker_driver = harness.plan_driver();
    worker_driver.fail_next_poll(io::ErrorKind::Other, "scripted worker runtime failure");
    let _parent_driver = harness.plan_driver();

    harness.enter(|| {
        let worker = spawn_worker::<MockRuntime, _, _>(|| {}, || {});

        let first = block_on::<MockRuntime, _>(worker.join())
            .expect_err("worker runtime panic should be reported");
        assert!(first.is_runtime_panicked());
        assert!(worker.is_finished());

        let second = block_on::<MockRuntime, _>(worker.join())
            .expect_err("worker outcome should remain observable");
        assert_eq!(first, second);
        run::<MockRuntime>();
    });
}

#[test]
fn isolated_worker_callback_panic_does_not_fail_join() {
    let harness = MockRuntimeHarness::new();
    let _worker_driver = harness.plan_driver();
    let _parent_driver = harness.plan_driver();
    let on_exit_ran = Arc::new(AtomicBool::new(false));

    harness.enter(|| {
        let on_exit_ran_by_parent = Arc::clone(&on_exit_ran);
        let worker = spawn_worker::<MockRuntime, _, _>(
            || panic!("isolated worker callback panic"),
            move || on_exit_ran_by_parent.store(true, Ordering::Release),
        );
        run::<MockRuntime>();

        assert!(
            block_on::<MockRuntime, _>(worker.join()).is_ok(),
            "the scheduled-callback panic firewall should keep the worker runtime healthy"
        );
    });

    assert!(on_exit_ran.load(Ordering::Acquire));
}

#[test]
fn worker_idle_close_race_executes_every_accepted_remote_task() {
    let harness = MockRuntimeHarness::new();
    let _worker_driver = harness.plan_driver();
    let _parent_driver = harness.plan_driver();
    let start_gate = ExecutionGate::default();
    let idle_close_gate = ExecutionGate::default();
    harness.gate_next_thread_spawn(start_gate.clone());
    let remote_ran = Arc::new(AtomicBool::new(false));

    harness.enter(|| {
        let worker = spawn_worker::<MockRuntime, _, _>(|| {}, || {});
        let idle_close_gate_on_worker = idle_close_gate.clone();
        worker.thread.shared.set_before_worker_idle_close(move || {
            idle_close_gate_on_worker.arrive_and_wait();
        });

        let _start_release = start_gate.release_on_drop();
        let _idle_release = idle_close_gate.release_on_drop();
        start_gate.release();
        assert!(
            idle_close_gate.wait_until_arrived(TEST_TIMEOUT),
            "worker should reach its idle-to-closed commit"
        );

        let remote_ran_on_worker = Arc::clone(&remote_ran);
        let queued = worker.queue_macrotask(move || {
            remote_ran_on_worker.store(true, Ordering::Release);
        });
        idle_close_gate.release();

        run::<MockRuntime>();

        assert!(queued.is_ok(), "the racing task should be accepted");
        assert!(
            remote_ran.load(Ordering::Acquire),
            "every accepted racing task must execute before worker closure"
        );
        assert!(matches!(
            worker.queue_macrotask(|| {}),
            Err(super::super::QueueError::Closed)
        ));
    });
}

#[test]
fn bind_panic_closes_worker_and_releases_parent() {
    let harness = MockRuntimeHarness::new();
    let worker_driver = harness.plan_driver();
    worker_driver.panic_on_bind();
    let _parent_driver = harness.plan_driver();
    let on_exit_ran = Arc::new(AtomicBool::new(false));

    harness.enter(|| {
        let on_exit_ran_by_parent = Arc::clone(&on_exit_ran);
        let worker = spawn_worker::<MockRuntime, _, _>(
            || {},
            move || {
                on_exit_ran_by_parent.store(true, Ordering::Release);
            },
        );
        run::<MockRuntime>();

        assert!(worker.is_finished());
        let error = block_on::<MockRuntime, _>(worker.join())
            .expect_err("driver bind panic should be reported by join");
        assert!(error.is_setup_panicked());
        assert!(worker.thread().is_closed());
        assert!(matches!(
            worker.queue_macrotask(|| {}),
            Err(super::super::QueueError::Closed)
        ));
    });

    assert!(on_exit_ran.load(Ordering::Acquire));
    assert!(
        harness
            .trace()
            .snapshot()
            .contains(&MockRuntimeEvent::DriverBindPanicked(worker_driver.id()))
    );
}

#[test]
fn fail_once_notification_does_not_strand_accepted_work() {
    let harness = MockRuntimeHarness::new();
    let control = harness.plan_driver();
    control.fail_next_notify(io::ErrorKind::WouldBlock, "submission queue full");
    let delivered = EventTrace::new();

    let promptly_delivered = thread::scope(|scope| {
        harness.enter(|| {
            let handle = current_thread_handle::<MockRuntime>();
            let timer = timeout::<MockRuntime, _>(Duration::from_secs(60), || {
                panic!("control timer must be cancelled")
            });
            let controller = control.clone();
            let delivered_by_task = delivered.clone();
            let sender = scope.spawn(move || {
                let cancellation = controller.cancel_wait_on_drop();
                assert!(
                    controller.wait_until_waiting(TEST_TIMEOUT),
                    "runtime should be parked before the remote enqueue"
                );
                handle
                    .queue_macrotask(move || {
                        delivered_by_task.record(());
                        timer.cancel();
                    })
                    .expect("a transient notification failure must be retried");
                let prompt = delivered.wait_for_len(1, DELIVERY_TIMEOUT);
                if !prompt {
                    controller.wake_runtime(1);
                }
                cancellation.disarm();
                prompt
            });

            run::<MockRuntime>();
            sender.join().expect("remote sender should finish")
        })
    });

    assert!(
        promptly_delivered,
        "accepted work must wake an already parked runtime without a rescue event"
    );
    let trace = harness.trace().snapshot();
    assert!(trace.contains(&MockRuntimeEvent::NotifyFailed(
        control.id(),
        io::ErrorKind::WouldBlock
    )));
    assert!(trace.contains(&MockRuntimeEvent::NotifySucceeded(control.id())));
}

#[test]
fn persistent_notification_failure_retains_internal_wake_until_recovery() {
    let harness = MockRuntimeHarness::new();
    let control = harness.plan_driver();
    control.fail_notifications(io::ErrorKind::WouldBlock, "notifications unavailable");
    let internal_wake_ran = Arc::new(AtomicBool::new(false));

    let accepted = thread::scope(|scope| {
        harness.enter(|| {
            let handle = current_thread_handle::<MockRuntime>();
            let timer = timeout::<MockRuntime, _>(Duration::from_secs(60), || {
                panic!("control timer must be cancelled")
            });
            let controller = control.clone();
            let trace = harness.trace();
            let internal_wake_ran_by_task = Arc::clone(&internal_wake_ran);
            let sender = scope.spawn(move || {
                let cancellation = controller.cancel_wait_on_drop();
                assert!(
                    controller.wait_until_waiting(TEST_TIMEOUT),
                    "runtime should be parked before the internal wake"
                );
                let accepted = handle.queue_internal_wake(move || {
                    internal_wake_ran_by_task.store(true, Ordering::Release);
                    timer.cancel();
                });

                assert!(
                    trace.wait_until(TEST_TIMEOUT, |events| {
                        events
                            .iter()
                            .filter(|event| matches!(event, MockRuntimeEvent::NotifyFailed(_, _)))
                            .count()
                            >= 4
                    }),
                    "durable notifier should keep retrying persistent failure"
                );
                controller.allow_notifications();
                cancellation.disarm();
                accepted
            });

            run::<MockRuntime>();
            sender.join().expect("remote sender should finish")
        })
    });

    assert!(accepted.is_ok());
    assert!(
        internal_wake_ran.load(Ordering::Acquire),
        "an accepted internal wake must survive notification failure"
    );
}

#[test]
fn persistent_notification_retry_stops_at_final_thread_closure() {
    let harness = MockRuntimeHarness::new();
    let control = harness.plan_driver();
    control.fail_notifications(io::ErrorKind::WouldBlock, "notifications unavailable");
    let internal_wake_ran = Arc::new(AtomicBool::new(false));

    let handle = harness.enter(|| {
        let handle = current_thread_handle::<MockRuntime>();
        let internal_wake_ran_by_task = Arc::clone(&internal_wake_ran);
        handle
            .queue_internal_wake(move || {
                internal_wake_ran_by_task.store(true, Ordering::Release);
            })
            .expect("internal wake should remain owned until closure");
        assert!(
            harness.trace().wait_until(TEST_TIMEOUT, |events| {
                events
                    .iter()
                    .filter(|event| matches!(event, MockRuntimeEvent::NotifyFailed(_, _)))
                    .count()
                    >= 4
            }),
            "durable notifier should retry before final closure"
        );
        handle
    });

    assert!(handle.is_closed());
    assert!(!internal_wake_ran.load(Ordering::Acquire));
}

/// A completion resolved on its owning thread must not notify that thread.
///
/// The notification exists to make a *parked* thread re-evaluate quiescence, so
/// notifying the thread that is already dispatching the completion buys nothing
/// and costs a wake round trip — on Linux an `IORING_OP_MSG_RING` to the ring's
/// own fd plus the `io_uring_enter` to submit it, per completion, which is what
/// collapses deferred-submission batches back to one operation each.
///
/// Counting notifier calls rather than timing anything keeps this a statement
/// about syscalls, which is the thing that regressed.
#[test]
fn same_thread_completions_do_not_notify_their_own_runtime() {
    let harness = MockRuntimeHarness::new();
    let _control = harness.plan_driver();
    let trace = harness.trace();

    harness.enter(|| {
        let _ = current_thread_handle::<MockRuntime>();
        for value in 0..8usize {
            let (future, source) = completion_for_current_thread::<usize>();
            // Resolve on this very thread, then drive the loop so the waker and
            // the liveness release both run here.
            source.complete(value);
            assert_eq!(block_on::<MockRuntime, _>(future), value);
        }
    });

    let notifies = trace
        .snapshot()
        .iter()
        .filter(|event| matches!(event, MockRuntimeEvent::NotifyAttempted(_)))
        .count();
    assert_eq!(
        notifies, 0,
        "same-thread completions should not wake their own runtime"
    );
}

/// The converse: a completion resolved from another thread still notifies, or
/// a parked runtime would never learn about it.
#[test]
fn cross_thread_completions_still_notify() {
    let harness = MockRuntimeHarness::new();
    let control = harness.plan_driver();
    let trace = harness.trace();

    thread::scope(|scope| {
        harness.enter(|| {
            let _ = current_thread_handle::<MockRuntime>();
            let (future, source) = completion_for_current_thread::<usize>();
            let controller = control.clone();
            let sender = scope.spawn(move || {
                assert!(
                    controller.wait_until_waiting(TEST_TIMEOUT),
                    "block_on should park before the completion arrives"
                );
                source.complete(7);
            });
            assert_eq!(block_on::<MockRuntime, _>(future), 7);
            sender.join().expect("sender should finish");
        });
    });

    let notifies = trace
        .snapshot()
        .iter()
        .filter(|event| matches!(event, MockRuntimeEvent::NotifyAttempted(_)))
        .count();
    assert!(
        notifies >= 1,
        "a cross-thread completion must notify the parked runtime"
    );
}

#[test]
fn parked_block_on_completion_survives_persistent_notification_failure() {
    let harness = MockRuntimeHarness::new();
    let control = harness.plan_driver();

    let value = thread::scope(|scope| {
        harness.enter(|| {
            let _ = current_thread_handle::<MockRuntime>();
            let (future, source) = completion_for_current_thread::<usize>();
            let controller = control.clone();
            let trace = harness.trace();
            let sender = scope.spawn(move || {
                let cancellation = controller.cancel_wait_on_drop();
                assert!(
                    controller.wait_until_waiting(TEST_TIMEOUT),
                    "block_on should park before completion"
                );
                controller
                    .fail_notifications(io::ErrorKind::WouldBlock, "notifications unavailable");
                source.complete(41);
                assert!(
                    trace.wait_until(TEST_TIMEOUT, |events| {
                        events
                            .iter()
                            .filter(|event| matches!(event, MockRuntimeEvent::NotifyFailed(_, _)))
                            .count()
                            >= 4
                    }),
                    "completion wake should retry while notifications fail"
                );
                controller.allow_notifications();
                cancellation.disarm();
            });

            let value = block_on::<MockRuntime, _>(future);
            sender.join().expect("completion sender should finish");
            value
        })
    });

    assert_eq!(value, 41);
}

#[test]
fn completion_between_ready_check_and_idle_commit_is_not_cancelled() {
    let harness = MockRuntimeHarness::new();
    let _control = harness.plan_driver();
    let idle_commit_gate = ExecutionGate::default();
    let observed = Arc::new(AtomicUsize::new(0));

    thread::scope(|scope| {
        harness.enter(|| {
            let owner = current_thread_handle::<MockRuntime>();
            let (future, source) = completion_for_current_thread::<usize>();
            let observed_by_task = Arc::clone(&observed);
            let join = queue_future::<MockRuntime, _>(async move {
                observed_by_task.store(future.await, Ordering::Release);
            });

            let idle_commit_gate_on_runtime = idle_commit_gate.clone();
            owner.shared.set_after_idle_ready_check(move || {
                idle_commit_gate_on_runtime.arrive_and_wait();
            });

            let idle_commit_gate_on_source = idle_commit_gate.clone();
            let completion = scope.spawn(move || {
                let release = idle_commit_gate_on_source.release_on_drop();
                assert!(
                    idle_commit_gate_on_source.wait_until_arrived(TEST_TIMEOUT),
                    "runtime should pause after its ready-work check"
                );
                source.complete(73);
                release.release();
            });

            run::<MockRuntime>();
            completion.join().expect("completion source should finish");
            block_on::<MockRuntime, _>(join)
                .expect("completion wake must win the idle-cancellation race");
        });
    });

    assert_eq!(observed.load(Ordering::Acquire), 73);
}

#[test]
fn parked_block_on_waker_survives_persistent_notification_failure() {
    let harness = MockRuntimeHarness::new();
    let control = harness.plan_driver();
    let wake_state = RemoteWakeState::new();

    thread::scope(|scope| {
        harness.enter(|| {
            let controller = control.clone();
            let trace = harness.trace();
            let remote_state = Arc::clone(&wake_state);
            let sender = scope.spawn(move || {
                let cancellation = controller.cancel_wait_on_drop();
                assert!(
                    controller.wait_until_waiting(TEST_TIMEOUT),
                    "block_on should park before its top-level wake"
                );
                controller
                    .fail_notifications(io::ErrorKind::WouldBlock, "notifications unavailable");
                remote_state.complete();
                assert!(
                    trace.wait_until(TEST_TIMEOUT, |events| {
                        events
                            .iter()
                            .filter(|event| matches!(event, MockRuntimeEvent::NotifyFailed(_, _)))
                            .count()
                            >= 4
                    }),
                    "top-level waker should retry while notifications fail"
                );
                controller.allow_notifications();
                cancellation.disarm();
            });

            block_on::<MockRuntime, _>(RemoteWakeFuture {
                state: Arc::clone(&wake_state),
            });
            sender.join().expect("remote waker should finish");
        });
    });
}

#[test]
fn mock_runtime_observes_teardown_order() {
    let harness = MockRuntimeHarness::new();
    let control = harness.plan_driver();

    harness.enter(run::<MockRuntime>);

    let trace = harness.trace().snapshot();
    let unbound = trace
        .iter()
        .position(|event| *event == MockRuntimeEvent::DriverUnbound(control.id()))
        .expect("driver should be unbound");
    let dropped = trace
        .iter()
        .position(|event| *event == MockRuntimeEvent::DriverDropped(control.id()))
        .expect("driver should be dropped");
    let notifier_dropped = trace
        .iter()
        .position(|event| *event == MockRuntimeEvent::NotifierDropped(control.id()))
        .expect("notifier should be dropped");

    assert!(unbound < dropped);
    assert!(unbound < notifier_dropped);
}

#[test]
fn mock_driver_dispatches_completions_in_script_order() {
    let harness = MockRuntimeHarness::new();
    let control = harness.plan_driver();
    let order = EventTrace::new();
    let second = order.clone();
    control.queue_completion("second", move || second.record(2usize));
    let first = order.clone();
    control.queue_completion("first", move || first.record(1usize));

    let (driver, _notifier) = harness
        .enter(MockRuntime::create_driver_pair)
        .expect("mock driver should initialize");
    assert!(driver.poll().expect("first poll should succeed").is_some());
    assert!(driver.poll().expect("second poll should succeed").is_some());

    assert_eq!(order.snapshot(), vec![2, 1]);
    let dispatched = harness
        .trace()
        .snapshot()
        .into_iter()
        .filter_map(|event| match event {
            MockRuntimeEvent::CompletionDispatched(_, label) => Some(label),
            _ => None,
        })
        .collect::<Vec<_>>();
    assert_eq!(dispatched, vec!["second", "first"]);
}

#[test]
fn failed_worker_spawn_leaves_parent_runtime_usable() {
    let harness = MockRuntimeHarness::new();
    let _worker_driver = harness.plan_driver();
    let parent_driver = harness.plan_driver();
    parent_driver.fail_next_wait(
        io::ErrorKind::Interrupted,
        "a phantom child made the parent wait",
    );
    harness.fail_next_thread_spawn(io::ErrorKind::WouldBlock, "thread limit reached");

    let worker_ran = Arc::new(AtomicBool::new(false));
    let exit_ran = Arc::new(AtomicBool::new(false));
    let parent_ran = Arc::new(AtomicBool::new(false));

    harness.enter(|| {
        let worker_ran = Arc::clone(&worker_ran);
        let exit_ran = Arc::clone(&exit_ran);
        let spawn = std::panic::catch_unwind(AssertUnwindSafe(|| {
            spawn_worker::<MockRuntime, _, _>(
                move || worker_ran.store(true, Ordering::Release),
                move || exit_ran.store(true, Ordering::Release),
            )
        }));
        assert!(spawn.is_err(), "scripted worker spawn should panic");

        let parent_ran = Arc::clone(&parent_ran);
        queue_task::<MockRuntime, _>(move || parent_ran.store(true, Ordering::Release));
        run::<MockRuntime>();
    });

    assert!(!worker_ran.load(Ordering::Acquire));
    assert!(!exit_ran.load(Ordering::Acquire));
    assert!(parent_ran.load(Ordering::Acquire));
    let trace = harness.trace().snapshot();
    assert!(trace.contains(&MockRuntimeEvent::ThreadSpawnFailed(
        io::ErrorKind::WouldBlock
    )));
    assert!(!trace.contains(&MockRuntimeEvent::ThreadStarted));
    assert!(!trace.contains(&MockRuntimeEvent::DriverWaitStarted(parent_driver.id())));
}

#[test]
fn mock_harness_releases_and_joins_scheduler_worker_on_panic() {
    let harness = MockRuntimeHarness::new();
    let gate = ExecutionGate::default();
    harness.gate_next_thread_spawn(gate.clone());
    let ran = Arc::new(AtomicBool::new(false));
    let test_panic = std::panic::catch_unwind(AssertUnwindSafe(|| {
        harness.enter(|| {
            let ran_on_worker = Arc::clone(&ran);
            let _worker = spawn_worker::<MockRuntime, _, _>(
                move || ran_on_worker.store(true, Ordering::Release),
                || {},
            );

            assert!(
                gate.wait_until_arrived(TEST_TIMEOUT),
                "spawned worker should reach its gate"
            );
            assert!(!ran.load(Ordering::Acquire));
            panic!("fail before explicitly releasing the worker");
        });
    }));

    assert!(test_panic.is_err());
    assert!(ran.load(Ordering::Acquire));
    let trace = harness.trace().snapshot();
    let finished = trace
        .iter()
        .position(|event| *event == MockRuntimeEvent::ThreadFinished)
        .expect("worker should finish during panic cleanup");
    let joined = trace
        .iter()
        .position(|event| *event == MockRuntimeEvent::ThreadJoined)
        .expect("worker should be joined during panic cleanup");
    assert!(finished < joined);
}

#[test]
fn reusable_helpers_are_bounded_and_reentrant() {
    let (spy, observed) = DropSpy::new("future");
    drop(spy);
    assert_eq!(observed.count(), 1);
    assert_eq!(observed.trace(), vec!["future"]);

    let callbacks = Arc::new(AtomicUsize::new(0));
    let callbacks_on_wake = Arc::clone(&callbacks);
    let reentrant = ReentrantWaker::new(move || {
        callbacks_on_wake.fetch_add(1, Ordering::AcqRel);
    });
    reentrant.waker().wake_by_ref();
    assert_eq!(reentrant.wake_count(), 1);
    assert_eq!(callbacks.load(Ordering::Acquire), 1);

    assert_eq!(
        completes_within(TEST_TIMEOUT, |_| 21usize * 2),
        42,
        "bounded helper should return task output"
    );
}

#[test]
fn bounded_helper_cancels_and_joins_on_timeout() {
    let stopped = Arc::new(AtomicBool::new(false));
    let stopped_by_worker = Arc::clone(&stopped);

    let timeout = std::panic::catch_unwind(AssertUnwindSafe(|| {
        completes_within(Duration::from_millis(10), move |cancellation| {
            cancellation.wait_cancelled();
            assert!(cancellation.is_cancelled());
            stopped_by_worker.store(true, Ordering::Release);
        });
    }));

    assert!(
        timeout.is_err(),
        "the bounded helper should report a timeout"
    );
    assert!(
        stopped.load(Ordering::Acquire),
        "the worker must stop before the timeout panic is resumed"
    );
}

#[cfg(unix)]
#[test]
fn fd_reuse_helper_preserves_the_owned_number() {
    use std::fs::File;
    use std::os::fd::{AsRawFd, OwnedFd};

    let target: OwnedFd = File::open("/dev/null")
        .expect("source fd should open")
        .into();
    let number = target.as_raw_fd();
    let reused = reuse_fd_number(target).expect("fd should be replaced");

    assert_eq!(reused.as_raw_fd(), number);
}
