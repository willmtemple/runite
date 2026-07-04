//! # Build pipeline: orchestrating subprocesses like promises
//!
//! Dev tools are interactive applications too — and they're mostly *process
//! orchestration*: run these compiles in parallel (but not too parallel),
//! collect every step's output and exit status, keep the terminal informed,
//! stop before linking if anything failed.
//!
//! That's a `Promise.all`-with-a-concurrency-limit problem, and it maps
//! directly onto runite:
//!
//! - [`runite::process::Command::output`] runs one step and captures
//!   `Output { status, stdout, stderr }` — like `std::process`, a non-zero
//!   exit is **data, not an error**, so a failing compile doesn't abort the
//!   orchestrator; it becomes part of the report.
//! - A [`runite::task::JoinSet`] holds the in-flight steps; refilling it from
//!   a queue whenever a step finishes gives bounded concurrency in a dozen
//!   lines, no semaphore type needed.
//! - Steps run truly concurrently (the children are separate processes), while
//!   the orchestrator itself stays a single, simple event loop.
//!
//! One step in this pipeline fails on purpose, so you can see failure
//! reporting: its stderr is captured and quoted, the link phase is skipped,
//! and the summary counts it — all without a single `?` firing on it.
//!
//! Run it: `cargo run --example build_pipeline`

use std::collections::VecDeque;
use std::time::Instant;

use runite::process::{Command, Output};
use runite::task::JoinSet;

struct Step {
    name: &'static str,
    script: &'static str,
}

/// The "build graph": a compile phase (parallel, bounded) then a link phase.
/// Each step is a real subprocess. `parser.c` is scripted to fail.
const COMPILE_STEPS: &[Step] = &[
    Step { name: "lexer.c", script: "sleep 0.12; echo 'lexer: 412 lines ok'" },
    Step { name: "parser.c", script: "sleep 0.08; echo 'parser.c:88: unbalanced brace' >&2; exit 1" },
    Step { name: "eval.c", script: "sleep 0.15; echo 'eval: 890 lines ok'" },
    Step { name: "main.c", script: "sleep 0.05; echo 'main: 120 lines ok'" },
];
const CONCURRENCY: usize = 2;

async fn run_step(step: &'static Step) -> (&'static Step, std::io::Result<Output>) {
    let output = Command::new("sh").arg("-c").arg(step.script).output().await;
    (step, output)
}

fn report(step: &Step, took: std::time::Duration, output: &Output) -> bool {
    if output.status.success() {
        let stdout = String::from_utf8_lossy(&output.stdout);
        println!("  ok   {:<10} {took:>10.1?}  {}", step.name, stdout.trim());
        true
    } else {
        let stderr = String::from_utf8_lossy(&output.stderr);
        println!(
            "  FAIL {:<10} {took:>10.1?}  (exit {:?}) {}",
            step.name,
            output.status.code(),
            stderr.trim(),
        );
        false
    }
}

#[runite::main]
async fn main() -> std::io::Result<()> {
    let started = Instant::now();
    println!("compiling {} units, {CONCURRENCY} at a time:", COMPILE_STEPS.len());

    // Bounded-concurrency executor: keep at most CONCURRENCY steps in the
    // JoinSet; every completion pulls the next step off the queue. This is the
    // entire "semaphore" — the set's size *is* the permit count.
    let mut queue: VecDeque<&'static Step> = COMPILE_STEPS.iter().collect();
    let mut in_flight = JoinSet::new();
    let mut failures = 0u32;

    loop {
        while in_flight.len() < CONCURRENCY {
            match queue.pop_front() {
                Some(step) => {
                    let launched = Instant::now();
                    in_flight.spawn(async move {
                        let (step, output) = run_step(step).await;
                        (step, launched.elapsed(), output)
                    });
                }
                None => break,
            }
        }

        match in_flight.join_next().await {
            Some(finished) => {
                let (step, took, output) = finished.expect("step task should not be aborted");
                // The subprocess *failing* is a normal result; only failing to
                // run it at all (spawn error) propagates as an io::Error.
                if !report(step, took, &output?) {
                    failures += 1;
                }
            }
            None => break, // queue empty and nothing in flight
        }
    }

    // The link phase depends on every compile: skip it if anything failed.
    if failures == 0 {
        let output = Command::new("sh")
            .arg("-c")
            .arg("echo 'linked app (4 objects)'")
            .output()
            .await?;
        println!("link: {}", String::from_utf8_lossy(&output.stdout).trim());
    } else {
        println!("link: skipped ({failures} unit(s) failed)");
    }

    println!(
        "pipeline finished in {:.1?} — {} succeeded, {failures} failed",
        started.elapsed(),
        COMPILE_STEPS.len() as u32 - failures,
    );

    // This example *expects* the scripted parser.c failure; anything else is a
    // real problem worth failing the example over.
    assert_eq!(failures, 1, "exactly the scripted step should fail");
    Ok(())
}
