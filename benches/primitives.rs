//! Throughput benchmarks for higher-level synchronization and task primitives:
//! [`RwLock`](runite::sync::RwLock), [`JoinSet`](runite::task::JoinSet),
//! [`broadcast`](runite::channel::broadcast), and
//! [`interval`](runite::time::interval).
//!
//! Like the rest of the suite these run on a single runtime thread (futures are
//! `!Send`) and time only the awaited work, so they isolate primitive overhead
//! from runtime spin-up.

mod common;

use std::time::Duration;

use common::time_on_runtime;
use criterion::{Criterion, Throughput, black_box, criterion_group, criterion_main};
use runite::channel::broadcast;
use runite::sync::RwLock;
use runite::task::JoinSet;
use runite::time::interval;

/// Uncontended `RwLock` acquire/release cost for both read and write guards.
/// There is never a waiter, so this measures the fast-path lock bookkeeping
/// rather than scheduler hand-off between contending tasks.
fn bench_rwlock(c: &mut Criterion) {
    let mut group = c.benchmark_group("rwlock");

    group.bench_function("read_uncontended", |b| {
        b.iter_custom(|iters| {
            time_on_runtime(move || async move {
                let lock = RwLock::new(0u64);
                let mut sum = 0u64;
                for _ in 0..iters {
                    let guard = lock.read().await;
                    sum = sum.wrapping_add(*guard);
                }
                black_box(sum);
            })
        });
    });

    group.bench_function("write_uncontended", |b| {
        b.iter_custom(|iters| {
            time_on_runtime(move || async move {
                let lock = RwLock::new(0u64);
                for _ in 0..iters {
                    let mut guard = lock.write().await;
                    *guard = guard.wrapping_add(1);
                }
                black_box(*lock.read().await);
            })
        });
    });

    group.finish();
}

/// `JoinSet` fan-out/fan-in: spawn a batch of trivial tasks into the set, then
/// drain every result via `join_next`. Measures the set's per-task tracking and
/// completion plumbing. Reported per spawned task.
fn bench_joinset(c: &mut Criterion) {
    const SIZE: u64 = 1_000;
    let mut group = c.benchmark_group("joinset");
    group.throughput(Throughput::Elements(SIZE));
    group.bench_function("spawn_drain_1k", |b| {
        b.iter_custom(|iters| {
            time_on_runtime(move || async move {
                for _ in 0..iters {
                    let mut set = JoinSet::new();
                    for i in 0..SIZE {
                        set.spawn(async move { i });
                    }
                    let mut drained = 0u64;
                    while let Some(result) = set.join_next().await {
                        let _ = result;
                        drained += 1;
                    }
                    debug_assert_eq!(drained, SIZE);
                }
            })
        });
    });
    group.finish();
}

/// One-to-one broadcast throughput. The ring is sized to the batch so no sends
/// are overwritten (no `Lagged`), isolating fan-out send + receive cost from
/// lag recovery. Reported per delivered message.
fn bench_broadcast(c: &mut Criterion) {
    const SIZE: u64 = 10_000;
    let mut group = c.benchmark_group("broadcast");
    group.throughput(Throughput::Elements(SIZE));
    group.bench_function("one_to_one_10k", |b| {
        b.iter_custom(|iters| {
            time_on_runtime(move || async move {
                for _ in 0..iters {
                    let (tx, mut rx) = broadcast::channel::<u64>(SIZE as usize);
                    for i in 0..SIZE {
                        let _ = tx.send(i);
                    }
                    let mut received = 0u64;
                    while received < SIZE {
                        match rx.recv().await {
                            Ok(_) => received += 1,
                            Err(_) => break,
                        }
                    }
                    debug_assert_eq!(received, SIZE);
                }
            })
        });
    });
    group.finish();
}

/// Interval tick churn on a zero-period interval. The first tick is immediate
/// and each later tick yields to the scheduler, so this measures the per-tick
/// timer rearm + dispatch overhead. Reported per tick.
fn bench_interval(c: &mut Criterion) {
    const SIZE: u64 = 1_000;
    let mut group = c.benchmark_group("interval");
    group.throughput(Throughput::Elements(SIZE));
    group.bench_function("tick_churn_1k", |b| {
        b.iter_custom(|iters| {
            time_on_runtime(move || async move {
                for _ in 0..iters {
                    let mut ticker = interval(Duration::ZERO);
                    for _ in 0..SIZE {
                        ticker.tick().await;
                    }
                }
            })
        });
    });
    group.finish();
}

criterion_group!(
    benches,
    bench_rwlock,
    bench_joinset,
    bench_broadcast,
    bench_interval,
);
criterion_main!(benches);
