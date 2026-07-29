//! `current_turn`: a join key between a runtime turn and the work done in it.

use std::cell::RefCell;
use std::rc::Rc;

use runite::TurnId;

/// Outside the loop there is no turn.
#[test]
fn no_turn_outside_the_event_loop() {
    assert!(runite::current_turn().is_none());
    runite::run();
    assert!(
        runite::current_turn().is_none(),
        "the turn must not leak past the loop"
    );
}

/// Work that runs inside one turn observes one identifier, and a microtask and
/// the task that queued it share it — that sharing is the whole point, since a
/// reactive flush happens in the microtask checkpoint of the turn that drove it.
#[test]
fn work_in_one_turn_shares_one_identifier() {
    type Observations = Rc<RefCell<Vec<(&'static str, Option<TurnId>)>>>;

    let observed: Observations = Rc::new(RefCell::new(vec![]));
    let recorder = Rc::clone(&observed);

    runite::queue_macrotask(move || {
        recorder
            .borrow_mut()
            .push(("macrotask", runite::current_turn()));
        let inner = Rc::clone(&recorder);
        runite::queue_microtask(move || {
            inner
                .borrow_mut()
                .push(("microtask", runite::current_turn()));
        });
    });
    runite::run();

    let observed = observed.borrow();
    assert_eq!(observed.len(), 2, "both callbacks should have run");
    let macrotask = observed[0].1.expect("a macrotask runs inside a turn");
    let microtask = observed[1].1.expect("a microtask runs inside a turn");
    assert_ne!(
        macrotask, microtask,
        "a microtask queued by a macrotask drains in the next turn's checkpoint"
    );
    assert!(
        microtask > macrotask,
        "turn identifiers increase: {macrotask} then {microtask}"
    );
}

/// Identifiers are monotonic and never repeat across turns.
#[test]
fn turn_identifiers_are_monotonic_and_unique() {
    let seen: Rc<RefCell<Vec<TurnId>>> = Rc::new(RefCell::new(vec![]));

    for _ in 0..8 {
        let recorder = Rc::clone(&seen);
        runite::queue_macrotask(move || {
            if let Some(turn) = runite::current_turn() {
                recorder.borrow_mut().push(turn);
            }
        });
    }
    runite::run();

    let seen = seen.borrow();
    assert_eq!(seen.len(), 8, "every macrotask should have recorded a turn");
    for pair in seen.windows(2) {
        assert!(
            pair[1] > pair[0],
            "turns must increase strictly: saw {} then {}",
            pair[0],
            pair[1]
        );
    }
}

/// Turns exist under every entry point that drives the loop, not just `run`.
/// A host that embeds the runtime through `run_ready_tasks` must still be able
/// to key its own records.
#[test]
fn every_entry_point_produces_turns() {
    fn turn_seen_by(drive: impl FnOnce()) -> Option<TurnId> {
        let seen = Rc::new(RefCell::new(None));
        let recorder = Rc::clone(&seen);
        runite::queue_macrotask(move || {
            *recorder.borrow_mut() = runite::current_turn();
        });
        drive();
        *seen.borrow()
    }

    let stalled = turn_seen_by(runite::run_until_stalled);
    assert!(stalled.is_some(), "run_until_stalled should drive turns");

    let ready = turn_seen_by(runite::run_ready_tasks);
    assert!(ready.is_some(), "run_ready_tasks should drive turns");

    // `block_on` resolves on its first poll, so it would return before running
    // a queued macrotask. Ask the future itself instead.
    let blocked = runite::block_on(async { runite::current_turn() });
    assert!(blocked.is_some(), "block_on should drive turns");

    let mut all = [stalled.unwrap(), ready.unwrap(), blocked.unwrap()];
    all.sort();
    assert!(
        all[0] < all[1] && all[1] < all[2],
        "identifiers stay unique across entry points"
    );

    runite::run();
}

/// A task poll sees the turn it is polled in.
#[runite::test]
async fn a_spawned_task_observes_its_turn() {
    let turn = runite::current_turn();
    assert!(turn.is_some(), "a task body runs inside a turn");

    let inner = runite::spawn(async { runite::current_turn() })
        .await
        .expect("task should finish");
    assert!(inner.is_some(), "a spawned task also runs inside a turn");
}
