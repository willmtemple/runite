//! The two things a consumer needs before it can merge its timeline with
//! runite's: a name for each runtime, and a clock both sides agree on.
//!
//! Task ids and timer ids restart at 1 on every runtime thread, so a record
//! carrying only `timer_id = 3` names as many timers as there are threads.
//! These tests pin the identity that disambiguates them, and the epoch the
//! clock readings share.

#![cfg(any(
    target_os = "linux",
    all(target_os = "macos", target_arch = "aarch64"),
    windows
))]

use std::sync::mpsc;
use std::time::Duration;

use runite::time::monotonic_now;

/// A thread with no runtime has no runtime identity, and asking does not give
/// it one. A diagnostic path has to be callable from anywhere.
#[test]
fn a_thread_without_a_runtime_has_no_identity() {
    let observed = std::thread::spawn(|| {
        let identity = runite::current_runtime_id();
        // Asking must not have installed a runtime behind the caller's back.
        (identity, runite::metrics::snapshot())
    })
    .join()
    .expect("the probing thread should not panic");

    assert_eq!(observed.0, None);
    assert_eq!(observed.1, runite::metrics::Snapshot::default());
}

/// One thread's runtime keeps one identity for its whole life, and two runtime
/// threads never share one. This is the property that makes a per-thread
/// `timer_id` readable in a merged timeline.
#[test]
fn each_runtime_thread_has_its_own_stable_identity() {
    let here = runite::block_on(async { runite::current_runtime_id() })
        .expect("block_on installs a runtime");
    assert_eq!(
        runite::block_on(async { runite::current_runtime_id() }),
        Some(here),
        "re-entering the same thread's runtime does not renumber it"
    );

    let (sender, receiver) = mpsc::channel();
    let first = sender.clone();
    let worker = runite::spawn_worker(
        move || {
            first
                .send(runite::current_runtime_id())
                .expect("the test should still be listening");
        },
        || {},
    );
    runite::block_on(worker.join()).expect("worker should exit normally");

    let second = runite::spawn_worker(
        move || {
            sender
                .send(runite::current_runtime_id())
                .expect("the test should still be listening");
        },
        || {},
    );
    runite::block_on(second.join()).expect("worker should exit normally");

    let first = receiver.recv().expect("the first worker reports").unwrap();
    let second = receiver.recv().expect("the second worker reports").unwrap();

    assert_ne!(here, first);
    assert_ne!(here, second);
    assert_ne!(first, second);
}

/// A finished runtime's identity is not handed to the next one, even when the
/// OS reuses the thread underneath it. The two share no task, timer, or
/// operation ids, so reusing the identity would merge unrelated timelines.
#[test]
fn a_finished_runtime_does_not_lend_its_identity_to_the_next() {
    fn one_runtime() -> Option<runite::RuntimeId> {
        std::thread::spawn(|| {
            let identity = runite::block_on(async { runite::current_runtime_id() });
            runite::shutdown();
            identity
        })
        .join()
        .expect("the runtime thread should not panic")
    }

    let first = one_runtime();
    let second = one_runtime();

    assert!(first.is_some());
    assert_ne!(first, second);
}

/// Every thread in the process reads one clock, so a timeline merged from
/// several runtime threads needs no per-thread correction.
#[test]
fn every_thread_reads_the_same_clock() {
    let (sender, receiver) = mpsc::channel();
    let before = monotonic_now();
    let worker = runite::spawn_worker(
        move || {
            sender
                .send(monotonic_now())
                .expect("the test should still be listening");
        },
        || {},
    );
    runite::block_on(worker.join()).expect("worker should exit normally");
    let after = monotonic_now();

    let elsewhere = receiver.recv().expect("the worker reports its clock");
    assert!(
        before <= elsewhere && elsewhere <= after,
        "a worker's reading should fall inside the parent's, saw {before:?} .. {elsewhere:?} .. {after:?}"
    );
}

/// The clock is the one the runtime arms its own deadlines against, which is
/// the whole reason it is exposed: a lateness measured against it is real
/// rather than an estimate across two unrelated clocks.
#[test]
fn runtime_deadlines_are_on_the_exposed_clock() {
    const NAP: Duration = Duration::from_millis(20);

    let before = monotonic_now();
    runite::block_on(runite::time::sleep(NAP));
    let after = monotonic_now();

    assert!(
        after.saturating_sub(before) >= NAP,
        "a {NAP:?} sleep should not finish early on the clock that armed it"
    );
}
