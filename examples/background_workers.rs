//! # Background workers: keep the main loop responsive, always
//!
//! The first rule of event-loop programming — in a browser, in Node, and in
//! runite — is that *the loop must keep turning*. CPU-heavy work goes to
//! another thread (a Web Worker, a `worker_threads` pool, `spawn_blocking`),
//! and results come back to the loop as ordinary events.
//!
//! This example proves the loop stays live while real CPU work happens
//! elsewhere. It runs a heartbeat interval on the main loop and measures its
//! **jitter** (how late each tick fires) while four compute jobs grind on the
//! blocking pool. Then run it again with `--blocking` to see the failure mode:
//! the same work done inline starves the heartbeat, and the jitter explodes to
//! roughly the duration of the compute.
//!
//! ```text
//! cargo run --example background_workers            # offloaded: jitter ~0ms
//! cargo run --example background_workers -- --blocking  # inline: jitter ~whole job
//! ```
//!
//! The results flow back over an ordinary `mpsc` channel — the runtime routes
//! the cross-thread wake to this loop — and land in plain `Rc<RefCell<..>>`
//! state, because once a result is back on the loop, no locks are needed.
//!
//! (`spawn_blocking` borrows a pool thread for one closure. When a component
//! needs a whole private event loop of its own — its own timers and I/O —
//! use `spawn_worker`, runite's equivalent of spinning up a worker thread
//! with its own runtime; see `channel_showcase.rs`.)

use std::cell::RefCell;
use std::rc::Rc;
use std::time::{Duration, Instant};

use runite::channel::mpsc;
use runite::time::interval;

/// Deliberately CPU-bound: no `.await` inside, nothing for the scheduler to
/// interleave. This is the kind of function that must not run on the loop.
fn crunch(job: u32) -> u64 {
    let mut hash: u64 = 0xcbf2_9ce4_8422_2325;
    for round in 0..25_000_000u64 {
        hash ^= round.wrapping_add(u64::from(job));
        hash = hash.wrapping_mul(0x0000_0100_0000_01b3);
    }
    hash
}

#[derive(Default)]
struct Progress {
    finished: u32,
    ticks: u32,
    worst_jitter: Duration,
}

#[runite::main]
async fn main() {
    let inline = std::env::args().any(|arg| arg == "--blocking");
    let jobs = 4u32;
    let progress = Rc::new(RefCell::new(Progress::default()));

    // The heartbeat: ticks every 25ms and records how late each tick fired.
    // On a healthy loop the lateness is microseconds. If anything hogs the
    // thread, ticks pile up and the measured jitter gives it away.
    let heartbeat = {
        let progress = Rc::clone(&progress);
        runite::spawn(async move {
            let period = Duration::from_millis(25);
            let mut ticker = interval(period);
            let mut last = Instant::now();
            loop {
                ticker.tick().await;
                let now = Instant::now();
                let jitter = now.duration_since(last).saturating_sub(period);
                last = now;
                let mut progress = progress.borrow_mut();
                progress.ticks += 1;
                progress.worst_jitter = progress.worst_jitter.max(jitter);
                let done = progress.finished;
                print!("\r[heartbeat {:>3}] jobs done: {done}/{jobs} ", progress.ticks);
                use std::io::Write as _;
                let _ = std::io::stdout().flush();
                if done == jobs {
                    break;
                }
            }
        })
    };

    // Give the heartbeat a head start: `spawn` only queues the task, and it
    // cannot take its first turn until this task yields. Without this, the
    // `--blocking` mode would finish all its compute inside main's very first
    // poll — before the heartbeat even starts — and the jitter measurement
    // would be meaningless.
    runite::time::sleep(Duration::from_millis(30)).await;

    let started = Instant::now();
    let (results_tx, mut results_rx) = mpsc::channel::<(u32, u64)>(8);

    if inline {
        // ANTI-PATTERN, on purpose: run the compute directly on the loop.
        // Watch the heartbeat freeze — no ticks fire while crunch() runs,
        // because nothing else can run. This is jank, quantified.
        for job in 0..jobs {
            let digest = crunch(job);
            results_tx.send((job, digest)).await.expect("send result");
        }
    } else {
        // The right way: each job runs on the shared blocking pool and posts
        // its result back to the loop as a message.
        for job in 0..jobs {
            let results_tx = results_tx.clone();
            runite::spawn_blocking(move || {
                let digest = crunch(job);
                // `try_send` from the pool thread: the runtime wakes the
                // receiving task over its cross-thread notification path.
                let _ = results_tx.try_send((job, digest));
            })
            .expect("blocking pool should accept the job")
            // Detach: completion is reported via the channel instead.
            ;
        }
    }
    drop(results_tx);

    // Collect results on the loop and update shared state — no locks, the
    // heartbeat task and this task interleave turn by turn.
    while let Some((job, digest)) = results_rx.recv().await {
        progress.borrow_mut().finished += 1;
        println!("\rjob {job} finished (digest {digest:#018x})");
    }

    let _ = heartbeat.await;
    let progress = progress.borrow();
    println!(
        "\n{} jobs in {:?}; heartbeat ticked {} times, worst jitter {:?}",
        jobs,
        started.elapsed(),
        progress.ticks,
        progress.worst_jitter,
    );
    if inline {
        println!("(--blocking mode: the jitter above IS the jank users would feel)");
    } else {
        println!("(offloaded: the loop never missed a beat while the pool did the work)");
    }
}
