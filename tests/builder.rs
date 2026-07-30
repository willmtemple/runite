//! Contract tests for `runite::Builder` and the `Runtime` it produces.
//!
//! Every test that cares about a *fresh* thread spawns one. `Builder::build`
//! is only meaningful before a thread has a runtime, and libtest's per-test
//! threads are not a guarantee worth relying on for that.

use std::io::ErrorKind;
use std::rc::Rc;
use std::time::Duration;

/// Runs `test` on a thread that has never touched the runtime, so
/// `Builder::build` sees an uninitialized thread.
fn on_a_fresh_thread<T: Send + 'static>(test: impl FnOnce() -> T + Send + 'static) -> T {
    std::thread::spawn(test)
        .join()
        .expect("runtime thread should not panic")
}

#[test]
fn a_default_build_produces_a_working_runtime() {
    on_a_fresh_thread(|| {
        let runtime = runite::Builder::new()
            .build()
            .expect("runtime should start");
        assert_eq!(runtime.block_on(async { 6 * 7 }), 42);
    });
}

#[test]
fn the_free_functions_share_the_runtime_the_builder_started() {
    on_a_fresh_thread(|| {
        let runtime = runite::Builder::new()
            .build()
            .expect("runtime should start");

        // Queued through the crate-root function, drained by the built
        // runtime: there is one event loop per thread, not one per handle.
        let seen = Rc::new(std::cell::Cell::new(0u32));
        let sink = Rc::clone(&seen);
        runite::spawn(async move {
            runite::time::sleep(Duration::from_millis(1)).await;
            sink.set(7);
        });
        runtime.run();

        assert_eq!(seen.get(), 7);
    });
}

#[test]
fn dropping_the_runtime_leaves_the_thread_running() {
    on_a_fresh_thread(|| {
        {
            let runtime = runite::Builder::new()
                .build()
                .expect("runtime should start");
            assert_eq!(runtime.block_on(async { 1u32 }), 1);
        }

        // The runtime is the thread's, not the handle's, so the free entry
        // points keep working after the handle is gone.
        assert_eq!(runite::block_on(async { 1u32 + 1 }), 2);
    });
}

#[test]
fn a_second_build_on_one_thread_is_refused() {
    on_a_fresh_thread(|| {
        let first = runite::Builder::new()
            .build()
            .expect("runtime should start");

        let error = runite::Builder::new()
            .build()
            .expect_err("a thread's runtime can only be configured once");
        assert_eq!(error.kind(), ErrorKind::AlreadyExists);

        // Refusal is not damage: the runtime that already exists still runs.
        assert_eq!(first.block_on(async { 3u32 }), 3);
    });
}

#[test]
fn a_dropped_runtime_does_not_free_the_thread_for_a_rebuild() {
    on_a_fresh_thread(|| {
        {
            let _runtime = runite::Builder::new()
                .build()
                .expect("runtime should start");
        }

        let error = runite::Builder::new()
            .build()
            .expect_err("dropping the handle does not remove the thread's runtime");
        assert_eq!(error.kind(), ErrorKind::AlreadyExists);
    });
}

#[test]
fn building_after_the_thread_has_already_used_the_runtime_is_refused() {
    on_a_fresh_thread(|| {
        // Any entry point installs the runtime lazily, which fixes its
        // configuration; a builder afterwards would have nothing to configure.
        runite::spawn(async {});
        runite::run();

        let error = runite::Builder::new()
            .build()
            .expect_err("the thread's runtime was already started implicitly");
        assert_eq!(error.kind(), ErrorKind::AlreadyExists);
    });
}

/// `shutdown` removes the thread's runtime, so it also removes the reason
/// `build` had to refuse — which is the only way to re-configure a thread that
/// has already started one.
#[test]
fn shutting_the_runtime_down_makes_the_thread_buildable_again() {
    on_a_fresh_thread(|| {
        {
            let first = runite::Builder::new()
                .build()
                .expect("runtime should start");
            assert_eq!(first.block_on(async { 1u32 }), 1);
        }

        runite::shutdown();

        let second = runite::Builder::new()
            .build()
            .expect("shutdown released the thread, so it can be configured again");
        assert_eq!(second.block_on(async { 2u32 }), 2);
    });
}

#[test]
fn a_built_runtime_drives_every_loop_entry_point() {
    on_a_fresh_thread(|| {
        let runtime = runite::Builder::new()
            .build()
            .expect("runtime should start");

        let ran = Rc::new(std::cell::Cell::new(0u32));

        let counter = Rc::clone(&ran);
        runite::queue_macrotask(move || counter.set(counter.get() + 1));
        runtime.run_ready_tasks();

        let counter = Rc::clone(&ran);
        runite::queue_macrotask(move || counter.set(counter.get() + 1));
        runtime.run_until_stalled();

        let counter = Rc::clone(&ran);
        runite::queue_macrotask(move || counter.set(counter.get() + 1));
        runtime.run();

        assert_eq!(ran.get(), 3);
    });
}

#[cfg(target_os = "linux")]
mod linux {
    use super::{ErrorKind, on_a_fresh_thread};
    use runite::os::linux::BuilderExt;

    #[test]
    fn a_small_ring_still_runs_io() {
        on_a_fresh_thread(|| {
            let runtime = runite::Builder::new()
                .ring_entries(8)
                .build()
                .expect("an 8-entry ring should be available");
            let contents = runtime
                .block_on(runite::fs::read_to_string("Cargo.toml"))
                .expect("Cargo.toml should be readable");
            assert!(contents.contains("runite"));
        });
    }

    /// `IORING_MAX_ENTRIES` is a valid request. Whether the machine has the
    /// locked memory for it is a different question, and one this test must
    /// not depend on — a ring that large exceeds a default `RLIMIT_MEMLOCK` —
    /// so it pins only that validation lets the value through.
    #[test]
    fn the_largest_permitted_ring_is_not_rejected_as_invalid() {
        on_a_fresh_thread(|| {
            let built = runite::Builder::new().ring_entries(32_768).build();
            match built {
                Ok(runtime) => assert_eq!(runtime.block_on(async { 4u32 }), 4),
                Err(error) => assert_ne!(
                    error.kind(),
                    ErrorKind::InvalidInput,
                    "IORING_MAX_ENTRIES is in range: {error}"
                ),
            }
        });
    }

    /// The kernel would round these up or clamp them silently. A caller sizing
    /// a ring against a locked-memory budget must not be quietly given more
    /// than it asked for, so they are errors instead.
    ///
    /// `1` and `65_536` are powers of two on the wrong side of the accepted
    /// range, so between them and the test above the two bounds are pinned:
    /// moving either constant fails one of these.
    #[test]
    fn a_ring_size_the_kernel_would_adjust_is_rejected() {
        for entries in [0, 1, 3, 100, 1000, 32_769, 65_536, u32::MAX] {
            let error = on_a_fresh_thread(move || {
                runite::Builder::new()
                    .ring_entries(entries)
                    .build()
                    .map(|_| ())
                    .expect_err("the kernel would not use this size verbatim")
            });
            assert_eq!(
                error.kind(),
                ErrorKind::InvalidInput,
                "ring_entries({entries}) should be rejected as invalid input"
            );
        }
    }

    /// A rejected configuration must not have started anything, or the caller
    /// could not correct it and try again.
    #[test]
    fn a_rejected_configuration_leaves_the_thread_unstarted() {
        on_a_fresh_thread(|| {
            runite::Builder::new()
                .ring_entries(100)
                .build()
                .expect_err("100 is not a power of two");

            let runtime = runite::Builder::new()
                .ring_entries(128)
                .build()
                .expect("the corrected size should still be buildable");
            assert_eq!(runtime.block_on(async { 5u32 }), 5);
        });
    }

    #[test]
    fn the_last_ring_size_set_wins() {
        on_a_fresh_thread(|| {
            runite::Builder::new()
                .ring_entries(100)
                .ring_entries(64)
                .build()
                .expect("the overriding size is valid, so the invalid one is gone");
        });
    }
}
