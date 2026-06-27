//! End-to-end concurrency tests: cross-thread worker scheduling and channels
//! driven through the public API.

mod common;

use common::block_on;
use runite::channel::{mpsc, oneshot};
use runite::time::sleep;
use std::time::Duration;

#[test]
fn worker_streams_messages_to_parent() {
    let received = block_on(|| async {
        let (sender, mut receiver) = mpsc::unbounded_channel::<u64>();

        let _worker = runite::spawn_worker(
            move || {
                runite::queue_macrotask(move || {
                    for value in 0..5u64 {
                        sender.send(value).expect("worker send should succeed");
                    }
                });
            },
            || {},
        );

        let mut collected = Vec::new();
        while let Some(value) = receiver.recv().await {
            collected.push(value);
        }
        collected
    });

    assert_eq!(received, vec![0, 1, 2, 3, 4]);
}

#[test]
fn oneshot_resolves_across_local_tasks() {
    let answer = block_on(|| async {
        let (sender, mut receiver) = oneshot::channel::<u32>();

        runite::spawn(async move {
            sleep(Duration::from_millis(2)).await;
            let _ = sender.send(42);
        });

        receiver.recv().await.expect("oneshot should resolve")
    });

    assert_eq!(answer, 42);
}

#[test]
fn bounded_channel_preserves_order_under_backpressure() {
    let received = block_on(|| async {
        // Capacity 1 forces the producer to block on each send until the
        // consumer drains the previous value.
        let (sender, mut receiver) = mpsc::channel::<usize>(8);

        let producer = runite::spawn(async move {
            for value in 0..16usize {
                sender.send(value).await.expect("send should succeed");
            }
        });

        let mut collected = Vec::new();
        while collected.len() < 16 {
            let value = receiver.recv().await.expect("recv should yield a value");
            collected.push(value);
        }

        producer.await.expect("producer task should not be aborted");
        collected
    });

    assert_eq!(received, (0..16).collect::<Vec<_>>());
}
