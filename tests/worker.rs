use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, Condvar, Mutex};
use std::task::{Context, Poll};
use std::time::Duration;

#[derive(Clone, Default)]
struct DropGate {
    state: Arc<(Mutex<(bool, bool)>, Condvar)>,
}

impl DropGate {
    fn arrive_and_wait(&self) {
        let (state, changed) = &*self.state;
        let mut state = state.lock().expect("drop gate poisoned");
        state.0 = true;
        changed.notify_all();
        while !state.1 {
            state = changed.wait(state).expect("drop gate poisoned");
        }
    }

    fn wait_until_arrived(&self) -> bool {
        let (state, changed) = &*self.state;
        let state = state.lock().expect("drop gate poisoned");
        let (state, _) = changed
            .wait_timeout_while(state, Duration::from_secs(5), |state| !state.0)
            .expect("drop gate poisoned");
        state.0
    }

    fn release(&self) {
        let (state, changed) = &*self.state;
        state.lock().expect("drop gate poisoned").1 = true;
        changed.notify_all();
    }
}

struct PendingDrop {
    gate: DropGate,
}

struct ReleaseOnDrop(DropGate);

impl Drop for ReleaseOnDrop {
    fn drop(&mut self) {
        self.0.release();
    }
}

impl Future for PendingDrop {
    type Output = ();

    fn poll(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Self::Output> {
        Poll::Pending
    }
}

impl Drop for PendingDrop {
    fn drop(&mut self) {
        self.gate.arrive_and_wait();
    }
}

#[test]
fn worker_join_waits_for_teardown_and_is_repeatable() {
    std::thread::spawn(|| {
        let gate = DropGate::default();
        let gate_on_worker = gate.clone();
        let on_exit = std::rc::Rc::new(std::cell::Cell::new(false));
        let on_exit_callback = std::rc::Rc::clone(&on_exit);
        let worker = runite::spawn_worker(
            move || {
                runite::spawn(PendingDrop {
                    gate: gate_on_worker,
                });
            },
            move || on_exit_callback.set(true),
        );
        let release = ReleaseOnDrop(gate.clone());

        assert!(
            gate.wait_until_arrived(),
            "worker should reach pending-future destruction"
        );
        assert!(
            !worker.is_finished(),
            "worker completion must follow pending destructors"
        );

        gate.release();
        drop(release);
        let result: Result<(), runite::WorkerJoinError> = runite::block_on(worker.join());
        result.expect("worker should exit normally");
        assert!(worker.is_finished());
        runite::block_on(worker.join()).expect("worker result should remain observable");

        runite::run();
        assert!(on_exit.get());
        assert!(worker.thread().is_closed());
    })
    .join()
    .expect("worker lifecycle test thread should finish");
}

#[test]
fn parent_run_tracks_worker_after_handle_drop() {
    let (worker_ran, on_exit_ran) = std::thread::spawn(|| {
        let worker_ran = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let worker_ran_on_thread = Arc::clone(&worker_ran);
        let on_exit_ran = std::rc::Rc::new(std::cell::Cell::new(false));
        let on_exit_callback = std::rc::Rc::clone(&on_exit_ran);

        let worker = runite::spawn_worker(
            move || {
                std::thread::sleep(Duration::from_millis(10));
                worker_ran_on_thread.store(true, std::sync::atomic::Ordering::Release);
            },
            move || on_exit_callback.set(true),
        );
        drop(worker);
        runite::run();

        (
            worker_ran.load(std::sync::atomic::Ordering::Acquire),
            on_exit_ran.get(),
        )
    })
    .join()
    .expect("parent runtime test thread should finish");

    assert!(worker_ran);
    assert!(on_exit_ran);
}

#[test]
fn join_remains_available_after_parent_runtime_shutdown() {
    std::thread::spawn(|| {
        let gate = DropGate::default();
        let gate_on_worker = gate.clone();
        let gate_on_parent = gate.clone();
        let (sender, receiver) = std::sync::mpsc::sync_channel(1);

        let parent = std::thread::spawn(move || {
            let worker = runite::spawn_worker(
                move || gate_on_worker.arrive_and_wait(),
                || panic!("a shut-down parent must not run on_exit"),
            );
            assert!(
                gate_on_parent.wait_until_arrived(),
                "worker should start before its parent exits"
            );
            sender
                .send(worker)
                .expect("worker handle should cross threads");
        });

        let worker = receiver.recv().expect("parent should return worker handle");
        parent.join().expect("parent runtime should shut down");
        assert!(!worker.is_finished());

        gate.release();
        runite::block_on(worker.join()).expect("worker should remain joinable");
        assert!(worker.is_finished());
    })
    .join()
    .expect("parent shutdown test thread should finish");
}
