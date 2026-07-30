//! A configured synchronous `#[runite::main]` must drive the runtime it built.
//!
//! Nothing else reaches that shape: every other test of the attribute is a
//! `#[runite::test]`. A `main` whose loop is never run exits without executing
//! a single spawned task, and silently, because dropping a `JoinHandle`
//! detaches rather than errors — so the failure has to be observed from
//! outside the body.

use std::sync::atomic::{AtomicBool, Ordering};

static DRAINED: AtomicBool = AtomicBool::new(false);

/// Checked from `Termination::report`, which runs after the attribute's
/// `run()`. Inside the body it would always be false.
struct Drained;

impl std::process::Termination for Drained {
    fn report(self) -> std::process::ExitCode {
        assert!(
            DRAINED.load(Ordering::SeqCst),
            "the attribute must drain its loop after a synchronous body returns"
        );
        std::process::ExitCode::SUCCESS
    }
}

#[runite::main(ring_entries = 8)]
fn main() -> Drained {
    runite::spawn(async {
        DRAINED.store(true, Ordering::SeqCst);
    });

    Drained
}
