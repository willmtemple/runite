//! Tests for runite's control-flow and entry-point macros.

use std::cell::{Cell, RefCell};
use std::future::Future;
use std::pin::Pin;
use std::rc::Rc;
use std::task::{Context, Poll};
use std::time::Duration;

/// The generated `#[test]` drives the async body to completion, including real
/// async I/O (a timer).
#[runite::test]
async fn drives_async_body() {
    let before = std::time::Instant::now();
    runite::time::sleep(Duration::from_millis(5)).await;
    assert!(before.elapsed() >= Duration::from_millis(5));
}

/// A test body may return a `Termination` type (here `Result`) so it can use
/// `?`; `Ok` reports success.
#[runite::test]
async fn supports_result_return() -> Result<(), Box<dyn std::error::Error>> {
    runite::time::sleep(Duration::from_millis(1)).await;
    let value: u32 = "42".parse()?;
    assert_eq!(value, 42);
    Ok(())
}

/// A spawned task on the test's loop runs to completion within the test.
#[runite::test]
async fn can_spawn_tasks() {
    let handle = runite::spawn(async { 7u32 + 8 });
    assert_eq!(handle.await.expect("spawned task should finish"), 15);
}

/// Attributes below `#[runite::test]` (here `#[should_panic]`) are forwarded to
/// the generated test wrapper.
#[runite::test]
#[should_panic = "expected boom"]
async fn forwards_should_panic() {
    panic!("expected boom");
}

/// `#[ignore]` is likewise attached to the generated harness wrapper.
#[runite::test]
#[ignore = "attribute forwarding regression"]
async fn forwards_ignore() {
    panic!("an ignored runite test must not execute");
}

/// The `crate = "..."` argument selects the path to the runite crate, so a
/// renamed dependency still works. Here we spell the real crate name.
#[runite::test(crate = "runite")]
async fn honors_crate_path_argument() {
    let handle = runite::spawn(async { 21u32 * 2 });
    assert_eq!(handle.await.expect("spawned task should finish"), 42);
}

struct PendingOnce {
    name: &'static str,
    polls: Rc<RefCell<Vec<&'static str>>>,
    pending: bool,
}

impl PendingOnce {
    fn new(name: &'static str, polls: Rc<RefCell<Vec<&'static str>>>) -> Self {
        Self {
            name,
            polls,
            pending: true,
        }
    }
}

impl Future for PendingOnce {
    type Output = &'static str;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        self.polls.borrow_mut().push(self.name);
        if self.pending {
            self.pending = false;
            cx.waker().wake_by_ref();
            Poll::Pending
        } else {
            Poll::Ready(self.name)
        }
    }
}

struct PollSpy {
    polled: Rc<Cell<bool>>,
}

impl Future for PollSpy {
    type Output = ();

    fn poll(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Self::Output> {
        self.polled.set(true);
        Poll::Ready(())
    }
}

struct DropSpyFuture {
    name: &'static str,
    drops: Rc<RefCell<Vec<&'static str>>>,
}

impl Future for DropSpyFuture {
    type Output = ();

    fn poll(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Self::Output> {
        Poll::Pending
    }
}

impl Drop for DropSpyFuture {
    fn drop(&mut self) {
        self.drops.borrow_mut().push(self.name);
    }
}

#[test]
fn select_default_rotates_polling_for_fairness() {
    let polls = Rc::new(RefCell::new(Vec::new()));
    let selected = runite::block_on(async {
        runite::select! {
            value = PendingOnce::new("first", Rc::clone(&polls)) => value,
            value = PendingOnce::new("second", Rc::clone(&polls)) => value,
        }
    });

    assert_eq!(selected, "second");
    assert_eq!(polls.borrow().as_slice(), ["first", "second", "second"]);
}

#[test]
fn select_biased_polls_in_lexical_order() {
    let polls = Rc::new(RefCell::new(Vec::new()));
    let selected = runite::block_on(async {
        runite::select! {
            biased;
            value = PendingOnce::new("first", Rc::clone(&polls)) => value,
            value = PendingOnce::new("second", Rc::clone(&polls)) => value,
        }
    });

    assert_eq!(selected, "first");
    assert_eq!(polls.borrow().as_slice(), ["first", "second", "first"]);
}

#[test]
fn select_evaluates_guards_once_before_constructing_futures() {
    let evaluations = Rc::new(RefCell::new(Vec::new()));
    let polls = Rc::new(RefCell::new(Vec::new()));
    let first_guard_evaluations = Rc::new(Cell::new(0));
    let second_guard_evaluations = Rc::new(Cell::new(0));

    let selected = runite::block_on(async {
        runite::select! {
            value = {
                evaluations.borrow_mut().push("first future");
                PendingOnce::new("first", Rc::clone(&polls))
            }, if {
                first_guard_evaluations.set(first_guard_evaluations.get() + 1);
                evaluations.borrow_mut().push("first guard");
                true
            } => value,
            value = {
                evaluations.borrow_mut().push("second future");
                PendingOnce::new("second", Rc::clone(&polls))
            }, if {
                second_guard_evaluations.set(second_guard_evaluations.get() + 1);
                evaluations.borrow_mut().push("second guard");
                true
            } => value,
        }
    });

    assert_eq!(selected, "second");
    assert_eq!(
        evaluations.borrow().as_slice(),
        [
            "first guard",
            "second guard",
            "first future",
            "second future"
        ]
    );
    assert_eq!(first_guard_evaluations.get(), 1);
    assert_eq!(second_guard_evaluations.get(), 1);
}

#[test]
fn select_default_is_fair_across_immediately_ready_invocations() {
    let selected = runite::block_on(async {
        let mut selected = Vec::new();
        for _ in 0..4 {
            selected.push(runite::select! {
                value = async { 0usize } => value,
                value = async { 1usize } => value,
            });
        }
        selected
    });

    assert_eq!(selected, [0, 1, 0, 1]);
}

#[test]
fn select_disabled_branches_fall_through_to_else_without_polling() {
    let constructed = Rc::new(Cell::new(false));
    let polled = Rc::new(Cell::new(false));
    let selected = runite::block_on(async {
        runite::select! {
            _ = {
                constructed.set(true);
                PollSpy {
                    polled: Rc::clone(&polled),
                }
            }, if false => "disabled",
            else => "else",
        }
    });

    assert_eq!(selected, "else");
    assert!(constructed.get());
    assert!(!polled.get());
}

#[test]
#[allow(clippy::redundant_async_block)]
fn select_supports_an_else_only_invocation() {
    let selected = runite::block_on(async {
        runite::select! {
            else => async { 42 }.await,
        }
    });

    assert_eq!(selected, 42);
}

#[test]
#[should_panic(expected = "all branches are disabled")]
fn select_panics_when_all_branches_are_disabled_without_else() {
    runite::block_on(async {
        runite::select! {
            _ = std::future::pending::<()>(), if false => (),
        }
    });
}

#[test]
fn select_pattern_mismatch_disables_branch_and_runs_else() {
    let selected = runite::block_on(async {
        runite::select! {
            Some(value) = async { None::<usize> } => value,
            else => 17,
        }
    });

    assert_eq!(selected, 17);
}

#[test]
fn select_preserves_bare_variant_and_constant_patterns() {
    const EXPECTED: usize = 7;

    let selected = runite::block_on(async {
        let none = runite::select! {
            None = async { Some(1usize) } => "matched None",
            else => "None mismatch",
        };
        let constant = runite::select! {
            EXPECTED = async { 8usize } => "matched constant",
            else => "constant mismatch",
        };
        let nested_constant = runite::select! {
            Some(EXPECTED) = async { Some(8usize) } => "matched nested constant",
            else => "nested constant mismatch",
        };
        let binding = runite::select! {
            value = async { String::from("binding") } => value,
        };
        (none, constant, nested_constant, binding)
    });

    assert_eq!(
        selected,
        (
            "None mismatch",
            "constant mismatch",
            "nested constant mismatch",
            String::from("binding")
        )
    );
}

/// Regression: a constant or unit variant underneath an explicit `&` used to be
/// erased from both comparison shapes, so the branch always appeared to match
/// and the handler then hit its unreachable arm.
#[test]
fn select_checks_constants_underneath_reference_patterns() {
    const EXPECTED: usize = 7;

    let selected = runite::block_on(async {
        let unit_variant_hit = runite::select! {
            &None = async { &None::<usize> } => "matched &None",
            else => "&None mismatch",
        };
        let unit_variant_miss = runite::select! {
            &None = async { &Some(1usize) } => "matched &None",
            else => "&None mismatch",
        };
        let constant_hit = runite::select! {
            &EXPECTED = async { &EXPECTED } => "matched &constant",
            else => "&constant mismatch",
        };
        let constant_miss = runite::select! {
            &EXPECTED = async { &8usize } => "matched &constant",
            else => "&constant mismatch",
        };
        let nested_miss = runite::select! {
            Some(&EXPECTED) = async { Some(&9usize) } => "matched nested &constant",
            else => "nested &constant mismatch",
        };
        // A binding underneath a reference must still bind, not be treated as a
        // constant comparison.
        let reference_binding = runite::select! {
            &value = async { &41usize } => value + 1,
            else => 0,
        };
        (
            unit_variant_hit,
            unit_variant_miss,
            constant_hit,
            constant_miss,
            nested_miss,
            reference_binding,
        )
    });

    assert_eq!(
        selected,
        (
            "matched &None",
            "&None mismatch",
            "matched &constant",
            "&constant mismatch",
            "nested &constant mismatch",
            42,
        )
    );
}

#[test]
fn select_extracts_mutable_patterns_by_value() {
    let selected = runite::block_on(async {
        let first = runite::select! {
            Some(mut value) = async { Some(String::from("a")) } => {
                value.push('b');
                value
            },
        };
        let second = runite::select! {
            ref mut value = async { 2usize } => {
                *value += 1;
                *value
            },
        };
        let mut referent = 3usize;
        let third = runite::select! {
            &mut value = async { &mut referent } => value + 1,
        };
        (first, second, third)
    });

    assert_eq!(selected, (String::from("ab"), 3, 4));
}

#[test]
fn select_drops_losers_before_handler() {
    let events = Rc::new(RefCell::new(Vec::new()));
    let handler_events = Rc::clone(&events);

    let selected = runite::block_on(async {
        runite::select! {
            _ = DropSpyFuture {
                name: "first loser",
                drops: Rc::clone(&events),
            } => "unreachable",
            _ = async {} => {
                handler_events.borrow_mut().push("handler");
                "winner"
            },
            _ = DropSpyFuture {
                name: "second loser",
                drops: Rc::clone(&events),
            } => "unreachable",
        }
    });

    assert_eq!(selected, "winner");
    let events = events.borrow();
    assert_eq!(events.last(), Some(&"handler"));
    assert!(events[..2].contains(&"first loser"));
    assert!(events[..2].contains(&"second loser"));
}

#[test]
fn select_handler_can_await() {
    let selected = runite::block_on(async {
        runite::select! {
            value = async { 40usize } => async move { value + 2 }.await,
        }
    });

    assert_eq!(selected, 42);
}

#[allow(clippy::needless_return)]
async fn return_from_select_handler() -> usize {
    runite::select! {
        _ = async {} => return 42,
    }
}

#[test]
fn select_handler_can_return_from_surrounding_function() {
    assert_eq!(runite::block_on(return_from_select_handler()), 42);
}

#[test]
#[allow(clippy::never_loop)]
fn select_handler_can_break_surrounding_loop() {
    let selected = runite::block_on(async {
        let mut attempts = 0usize;
        loop {
            attempts += 1;
            runite::select! {
                value = async { attempts } => break value,
            }
        }
    });

    assert_eq!(selected, 1);
}

struct LoopResource {
    value: usize,
}

impl LoopResource {
    async fn next(&mut self) -> usize {
        self.value += 1;
        self.value
    }
}

#[test]
fn select_accepts_methods_borrowing_loop_owned_resources() {
    let selected = runite::block_on(async {
        let mut resource = LoopResource { value: 0 };
        let mut selected = Vec::new();

        for _ in 0..3 {
            let next = resource.next();
            selected.push(runite::select! {
                value = next => value,
            });
        }

        selected
    });

    assert_eq!(selected, [1, 2, 3]);
}

#[test]
fn select_supports_more_than_sixteen_arms() {
    let selected = runite::block_on(async {
        runite::select! {
            _ = std::future::pending::<()>() => 0,
            _ = std::future::pending::<()>() => 1,
            _ = std::future::pending::<()>() => 2,
            _ = std::future::pending::<()>() => 3,
            _ = std::future::pending::<()>() => 4,
            _ = std::future::pending::<()>() => 5,
            _ = std::future::pending::<()>() => 6,
            _ = std::future::pending::<()>() => 7,
            _ = std::future::pending::<()>() => 8,
            _ = std::future::pending::<()>() => 9,
            _ = std::future::pending::<()>() => 10,
            _ = std::future::pending::<()>() => 11,
            _ = std::future::pending::<()>() => 12,
            _ = std::future::pending::<()>() => 13,
            _ = std::future::pending::<()>() => 14,
            _ = std::future::pending::<()>() => 15,
            _ = std::future::pending::<()>() => 16,
            _ = std::future::pending::<()>() => 17,
            _ = std::future::pending::<()>() => 18,
            value = async { 19usize } => value,
        }
    });

    assert_eq!(selected, 19);
}
