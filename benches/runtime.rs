//! Core scheduler/primitive throughput benchmarks.
//!
//! These measure in-process runtime mechanics (task scheduling, cooperative
//! yielding, channel hand-off, timer churn) with no syscalls on the hot path,
//! so they isolate executor overhead.

mod common;

use std::time::Duration;

use common::time_on_runtime;
use criterion::{Criterion, criterion_group, criterion_main};
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

criterion_group!(
    benches,
    bench_spawn_join,
    bench_yield,
    bench_oneshot,
    bench_mpsc_pingpong,
    bench_timer_churn,
);
criterion_main!(benches);
