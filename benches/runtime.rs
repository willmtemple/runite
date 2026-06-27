//! Core scheduler/primitive throughput benchmarks.
//!
//! These measure in-process runtime mechanics (task scheduling, cooperative
//! yielding, channel hand-off, timer churn) with no syscalls on the hot path,
//! so they isolate executor overhead.

mod common;

use std::cell::Cell;
use std::rc::Rc;
use std::time::Duration;

use common::time_on_runtime;
use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use runite::channel::{mpsc, oneshot};
use runite::time::sleep;

/// Cost of spawning a local future and awaiting its `JoinHandle` to completion.
fn bench_spawn_join(c: &mut Criterion) {
    c.bench_function("spawn_join", |b| {
        b.iter_custom(|iters| {
            time_on_runtime(move || async move {
                for _ in 0..iters {
                    let handle = runite::spawn(async { 1u64 });
                    let _ = handle.await;
                }
            })
        });
    });
}

/// Cost of a cooperative yield back to the scheduler.
fn bench_yield(c: &mut Criterion) {
    c.bench_function("yield_now", |b| {
        b.iter_custom(|iters| {
            time_on_runtime(move || async move {
                for _ in 0..iters {
                    runite::yield_now().await;
                }
            })
        });
    });
}

/// Round-trip latency of a freshly constructed oneshot channel.
fn bench_oneshot(c: &mut Criterion) {
    c.bench_function("oneshot_roundtrip", |b| {
        b.iter_custom(|iters| {
            time_on_runtime(move || async move {
                for i in 0..iters {
                    let (tx, mut rx) = oneshot::channel::<u64>();
                    let _ = tx.send(i);
                    let _ = rx.recv().await;
                }
            })
        });
    });
}

/// Throughput of an in-process bounded mpsc channel producer/consumer pair.
fn bench_mpsc_pingpong(c: &mut Criterion) {
    c.bench_function("mpsc_pingpong", |b| {
        b.iter_custom(|iters| {
            time_on_runtime(move || async move {
                let (tx, mut rx) = mpsc::channel::<u64>(16);
                let producer = runite::spawn(async move {
                    for i in 0..iters {
                        if tx.send(i).await.is_err() {
                            break;
                        }
                    }
                });
                for _ in 0..iters {
                    if rx.recv().await.is_none() {
                        break;
                    }
                }
                let _ = producer.await;
            })
        });
    });
}

/// Timer churn: scheduling and firing a large number of short sleeps.
fn bench_timer_churn(c: &mut Criterion) {
    let mut group = c.benchmark_group("timer");
    group.measurement_time(Duration::from_secs(8));
    group.bench_function("zero_sleep", |b| {
        b.iter_custom(|iters| {
            time_on_runtime(move || async move {
                for _ in 0..iters {
                    sleep(Duration::ZERO).await;
                }
            })
        });
    });
    group.finish();
}

/// Scheduler scaling: cost of spawning a batch of trivial tasks and draining
/// every `JoinHandle`. All tasks are spawned before any is awaited, so this
/// stresses the ready-queue as the executor fans a wide batch in and out.
/// Reported per element (one spawned task) across batch sizes.
fn bench_spawn_many(c: &mut Criterion) {
    let mut group = c.benchmark_group("spawn_many");
    for size in [100u64, 1_000, 10_000] {
        group.throughput(Throughput::Elements(size));
        group.bench_with_input(BenchmarkId::from_parameter(size), &size, |b, &size| {
            b.iter_custom(|iters| {
                time_on_runtime(move || async move {
                    for _ in 0..iters {
                        let handles: Vec<_> =
                            (0..size).map(|i| runite::spawn(async move { i })).collect();
                        for handle in handles {
                            let _ = handle.await;
                        }
                    }
                })
            });
        });
    }
    group.finish();
}

/// Bulk mpsc throughput: stream a large run of values through a bounded channel
/// with a dedicated producer task while the consumer drains on the main task.
/// Unlike `mpsc_pingpong` (which alternates one-in/one-out on a depth-16
/// channel), this measures sustained hand-off when the queue stays primed.
fn bench_mpsc_throughput(c: &mut Criterion) {
    const SIZE: u64 = 10_000;
    let mut group = c.benchmark_group("mpsc_throughput");
    group.throughput(Throughput::Elements(SIZE));
    group.bench_function("stream_10k", |b| {
        b.iter_custom(|iters| {
            time_on_runtime(move || async move {
                for _ in 0..iters {
                    let (tx, mut rx) = mpsc::channel::<u64>(64);
                    let producer = runite::spawn(async move {
                        for i in 0..SIZE {
                            if tx.send(i).await.is_err() {
                                break;
                            }
                        }
                    });
                    let mut received = 0u64;
                    while rx.recv().await.is_some() {
                        received += 1;
                        if received == SIZE {
                            break;
                        }
                    }
                    let _ = producer.await;
                }
            })
        });
    });
    group.finish();
}

/// Queueable hand-off cost: enqueue a fixed batch of synchronous callbacks via
/// [`queue_microtask`](runite::queue_microtask) /
/// [`queue_macrotask`](runite::queue_macrotask), then park on a sentinel that
/// runs after the batch. This isolates the per-callback enqueue + dispatch
/// overhead of runite's JS-style microtask/macrotask scheduling (vs. `spawn`,
/// which builds a full task). Reported per enqueued callback.
///
/// We must park on a real wakeup (a `oneshot`) rather than spin on `yield_now`:
/// a task wakeup is itself a microtask, and the loop fully drains the microtask
/// queue before servicing any macrotask, so busy-yielding would starve the
/// macrotask batch forever.
fn bench_queueables(c: &mut Criterion) {
    const BATCH: u64 = 1_000;
    let mut group = c.benchmark_group("queueable");
    group.throughput(Throughput::Elements(BATCH));

    group.bench_function("microtask", |b| {
        b.iter_custom(|iters| {
            time_on_runtime(move || async move {
                let counter = Rc::new(Cell::new(0u64));
                for _ in 0..iters {
                    counter.set(0);
                    let (tx, mut rx) = oneshot::channel::<()>();
                    for _ in 0..BATCH {
                        let counter = Rc::clone(&counter);
                        runite::queue_microtask(move || counter.set(counter.get() + 1));
                    }
                    runite::queue_microtask(move || {
                        let _ = tx.send(());
                    });
                    let _ = rx.recv().await;
                    debug_assert_eq!(counter.get(), BATCH);
                }
            })
        });
    });

    group.bench_function("macrotask", |b| {
        b.iter_custom(|iters| {
            time_on_runtime(move || async move {
                let counter = Rc::new(Cell::new(0u64));
                for _ in 0..iters {
                    counter.set(0);
                    let (tx, mut rx) = oneshot::channel::<()>();
                    for _ in 0..BATCH {
                        let counter = Rc::clone(&counter);
                        runite::queue_macrotask(move || counter.set(counter.get() + 1));
                    }
                    runite::queue_macrotask(move || {
                        let _ = tx.send(());
                    });
                    let _ = rx.recv().await;
                    debug_assert_eq!(counter.get(), BATCH);
                }
            })
        });
    });

    group.finish();
}

criterion_group!(
    benches,
    bench_spawn_join,
    bench_yield,
    bench_oneshot,
    bench_mpsc_pingpong,
    bench_timer_churn,
    bench_spawn_many,
    bench_mpsc_throughput,
    bench_queueables,
);
criterion_main!(benches);
