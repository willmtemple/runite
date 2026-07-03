mod common;

use std::future::{Future, poll_fn};
use std::pin::pin;
use std::sync::mpsc as std_mpsc;
use std::task::Poll;

use common::block_on;
use runite::channel::{broadcast, mpsc, oneshot, watch};

#[test]
fn oneshot_send_receive_and_drop_paths() {
    let value = block_on(|| async {
        let (sender, mut receiver) = oneshot::channel();
        sender.send("ready").expect("receiver is alive");
        receiver.recv().await.expect("value should be delivered")
    });
    assert_eq!(value, "ready");

    let dropped_sender = block_on(|| async {
        let (sender, mut receiver) = oneshot::channel::<usize>();
        drop(sender);
        receiver.recv().await
    });
    assert_eq!(dropped_sender, Err(oneshot::RecvError));

    let (sender, receiver) = oneshot::channel();
    drop(receiver);
    assert_eq!(sender.send(7), Err(oneshot::SendError(7)));
}

#[test]
fn oneshot_try_recv_close_and_cross_thread_send() {
    let (sender, mut receiver) = oneshot::channel();
    assert_eq!(receiver.try_recv(), Err(oneshot::TryRecvError::Empty));
    receiver.close();
    assert!(sender.is_closed());
    assert_eq!(sender.send(11), Err(oneshot::SendError(11)));
    assert_eq!(receiver.try_recv(), Err(oneshot::TryRecvError::Closed));

    let received = block_on(|| async {
        let (sender, mut receiver) = oneshot::channel();
        let (ready_tx, ready_rx) = std_mpsc::channel();
        let thread = std::thread::spawn(move || {
            ready_rx.recv().expect("runtime should poll recv");
            sender.send(42).expect("receiver should wait");
        });

        let mut recv = pin!(receiver.recv());
        let mut signaled = false;
        let value = poll_fn(|cx| match recv.as_mut().poll(cx) {
            Poll::Ready(result) => Poll::Ready(result),
            Poll::Pending => {
                if !signaled {
                    ready_tx.send(()).expect("worker should wait for signal");
                    signaled = true;
                }
                Poll::Pending
            }
        })
        .await
        .expect("cross-thread send should wake receiver");

        thread.join().expect("sender thread should finish");
        value
    });

    assert_eq!(received, 42);
}

#[test]
fn watch_borrow_update_coalescing_and_modification_paths() {
    let observed = block_on(|| async {
        let (sender, mut receiver) = watch::channel(0);

        assert_eq!(*sender.borrow(), 0);
        assert_eq!(*receiver.borrow(), 0);
        assert!(!sender.send_if_modified(|value| {
            *value = 10;
            false
        }));

        sender.send(1).expect("receiver is alive");
        let borrowed_without_update = *receiver.borrow();
        receiver
            .changed()
            .await
            .expect("borrow should not mark version observed");
        sender.send_modify(|value| *value += 1);
        assert!(sender.send_if_modified(|value| {
            *value += 1;
            true
        }));
        receiver.changed().await.expect("changes should coalesce");
        let updated = *receiver.borrow_and_update();
        sender.send(4).expect("receiver is alive");
        receiver.changed().await.expect("next change should arrive");
        (
            borrowed_without_update,
            updated,
            *receiver.borrow_and_update(),
        )
    });

    assert_eq!(observed, (1, 3, 4));
}

#[test]
fn watch_multiple_receivers_no_receiver_sends_and_drop_paths() {
    let (sender, receiver) = watch::channel(String::from("initial"));
    let mut cloned = receiver.clone();
    let mut subscribed = sender.subscribe();
    assert_eq!(sender.receiver_count(), 3);

    sender
        .send(String::from("next"))
        .expect("receivers are alive");
    let seen = block_on(move || async move {
        cloned.changed().await.expect("clone sees next value");
        subscribed
            .changed()
            .await
            .expect("subscriber sees next value");
        let cloned_value = cloned.borrow().clone();
        let subscribed_value = subscribed.borrow().clone();
        (cloned_value, subscribed_value)
    });
    assert_eq!(seen, (String::from("next"), String::from("next")));

    drop(receiver);
    assert_eq!(sender.receiver_count(), 0);
    assert_eq!(
        sender.send(String::from("rejected")),
        Err(watch::SendError(String::from("rejected")))
    );
    sender.send_modify(|value| value.push_str("-modified"));
    assert!(sender.send_if_modified(|value| {
        value.push_str("-if");
        true
    }));
    assert_eq!(&*sender.borrow(), "next-modified-if");

    let closed = block_on(|| async {
        let (sender, mut receiver) = watch::channel(1);
        drop(sender);
        receiver.changed().await
    });
    assert_eq!(closed, Err(watch::RecvError));
}

#[test]
fn watch_cross_thread_change_wakes_waiting_receiver() {
    let value = block_on(|| async {
        let (sender, mut receiver) = watch::channel(0);
        let (ready_tx, ready_rx) = std_mpsc::channel();
        let thread = std::thread::spawn(move || {
            ready_rx.recv().expect("runtime should poll changed");
            sender.send(99).expect("receiver should wait");
        });

        {
            let mut changed = pin!(receiver.changed());
            let mut signaled = false;
            poll_fn(|cx| match changed.as_mut().poll(cx) {
                Poll::Ready(result) => Poll::Ready(result),
                Poll::Pending => {
                    if !signaled {
                        ready_tx.send(()).expect("worker should wait for signal");
                        signaled = true;
                    }
                    Poll::Pending
                }
            })
            .await
            .expect("cross-thread send should wake changed");
        }

        thread.join().expect("sender thread should finish");
        *receiver.borrow_and_update()
    });

    assert_eq!(value, 99);
}

/// Regression: a waiter registered at an old version and completed but never
/// re-polled (its `changed()` future abandoned while the receiver's internal
/// wait slot persists) must not fire a spurious `changed()` and regress the
/// receiver's version after `borrow_and_update` has advanced past it.
#[test]
fn watch_stale_completion_does_not_regress_version() {
    use std::time::Duration;

    let parked = block_on(|| async {
        let (sender, mut receiver) = watch::channel(0u32);

        // Register a waiter (version 0), then let it time out so the `changed()`
        // future is dropped while the receiver's internal wait slot persists.
        let _ =
            runite::time::timeout(Duration::from_millis(10), receiver.changed()).await;

        // Complete the persisted (never re-polled) waiter, then advance past it.
        sender.send(1).unwrap();
        sender.send(2).unwrap();
        assert_eq!(*receiver.borrow_and_update(), 2);

        // `changed()` must park: nothing is newer than version 2. If it instead
        // resolves off the stale version-1 completion (regressing the receiver's
        // version), the timeout returns `Ok` instead of elapsing.
        runite::time::timeout(Duration::from_millis(10), receiver.changed())
            .await
            .is_err()
    });

    assert!(parked, "changed() must park, not fire a stale completion");
}

#[test]
fn broadcast_fanout_lag_resubscribe_and_close() {
    let observed = block_on(|| async {
        let (sender, mut first) = broadcast::channel(2);
        let mut second = sender.subscribe();
        assert_eq!(sender.receiver_count(), 2);
        assert!(first.is_empty());

        assert_eq!(sender.send(1), Ok(2));
        assert_eq!(sender.send(2), Ok(2));
        assert_eq!(first.len(), 2);
        assert_eq!(second.len(), 2);
        assert_eq!(first.recv().await, Ok(1));

        assert_eq!(sender.send(3), Ok(2));
        assert_eq!(sender.send(4), Ok(2));
        assert_eq!(first.recv().await, Err(broadcast::RecvError::Lagged(1)));
        let first_tail = vec![first.recv().await.unwrap(), first.recv().await.unwrap()];
        assert_eq!(second.recv().await, Err(broadcast::RecvError::Lagged(2)));
        let second_tail = vec![second.recv().await.unwrap(), second.recv().await.unwrap()];

        let mut fresh = first.resubscribe();
        assert!(fresh.is_empty());
        assert_eq!(sender.send(5), Ok(3));
        let fresh_value = fresh.recv().await.unwrap();

        drop(sender);
        let closed = fresh.recv().await.unwrap_err();
        (first_tail, second_tail, fresh_value, closed)
    });

    assert_eq!(
        observed,
        (vec![3, 4], vec![3, 4], 5, broadcast::RecvError::Closed)
    );

    let (sender, receiver) = broadcast::channel(1);
    drop(receiver);
    assert_eq!(sender.receiver_count(), 0);
    assert_eq!(sender.send(6), Err(broadcast::SendError(6)));
}

#[test]
fn broadcast_buffer_drains_before_closed_after_all_senders_drop() {
    let observed = block_on(|| async {
        let (sender, mut receiver) = broadcast::channel(4);
        sender.send("a").unwrap();
        sender.send("b").unwrap();
        drop(sender);
        vec![
            receiver.recv().await.map_err(|_| "closed"),
            receiver.recv().await.map_err(|_| "closed"),
            receiver.recv().await.map_err(|_| "closed"),
        ]
    });

    assert_eq!(observed, vec![Ok("a"), Ok("b"), Err("closed")]);
}

#[test]
fn mpsc_try_paths_backpressure_and_drop_paths() {
    let backpressured = block_on(|| async {
        let (sender, mut receiver) = mpsc::channel(1);
        sender.try_send(1).expect("first send fits");
        assert_eq!(sender.try_send(2), Err(mpsc::TrySendError::Full(2)));

        let send_sender = sender.clone();
        {
            let mut send = pin!(send_sender.send(2));
            poll_fn(|cx| match send.as_mut().poll(cx) {
                Poll::Ready(result) => panic!("send should wait for capacity: {result:?}"),
                Poll::Pending => Poll::Ready(()),
            })
            .await;

            assert_eq!(receiver.try_recv(), Ok(1));
            send.await.expect("capacity should be released");
        }
        assert_eq!(receiver.recv().await, Some(2));
        drop(sender);
        drop(send_sender);
        receiver.recv().await
    });
    assert_eq!(backpressured, None);

    let (sender, mut receiver) = mpsc::channel(1);
    assert_eq!(receiver.try_recv(), Err(mpsc::TryRecvError::Empty));
    receiver.close();
    assert!(sender.is_closed());
    assert_eq!(sender.try_send(3), Err(mpsc::TrySendError::Closed(3)));
    assert_eq!(receiver.try_recv(), Err(mpsc::TryRecvError::Disconnected));

    let send_error = block_on(|| async {
        let (sender, mut receiver) = mpsc::channel(1);
        receiver.close();
        sender.send(4).await.expect_err("send should fail")
    });
    assert_eq!(send_error, mpsc::SendError(4));
}

#[test]
fn mpsc_fifo_across_senders_and_cross_thread_producer() {
    let received = block_on(|| async {
        let (sender, mut receiver) = mpsc::unbounded_channel();
        let other = sender.clone();
        sender.send(("a", 1)).unwrap();
        other.send(("b", 2)).unwrap();
        sender.send(("a", 3)).unwrap();
        drop(sender);
        drop(other);

        let mut values = Vec::new();
        while let Some(value) = receiver.recv().await {
            values.push(value);
        }
        values
    });
    assert_eq!(received, vec![("a", 1), ("b", 2), ("a", 3)]);

    let cross_thread = block_on(|| async {
        let (sender, mut receiver) = mpsc::unbounded_channel();
        let (ready_tx, ready_rx) = std_mpsc::channel();
        let thread = std::thread::spawn(move || {
            ready_rx.recv().expect("runtime should poll recv");
            sender.send("worker").expect("receiver should wait");
        });

        let mut recv = pin!(receiver.recv());
        let mut signaled = false;
        let value = poll_fn(|cx| match recv.as_mut().poll(cx) {
            Poll::Ready(result) => Poll::Ready(result),
            Poll::Pending => {
                if !signaled {
                    ready_tx.send(()).expect("worker should wait for signal");
                    signaled = true;
                }
                Poll::Pending
            }
        })
        .await;

        thread.join().expect("sender thread should finish");
        value
    });

    assert_eq!(cross_thread, Some("worker"));
}
