//! Event-loop ordering tests pinning runite's scheduling semantics to the
//! JavaScript model it deliberately mirrors.
//!
//! The thesis of this runtime is that JS's microtask/macrotask event loop is a
//! good model for interactive, single-threaded async work. These tests encode
//! the contract that makes that true:
//!
//! | Operation | JS analog | Resumes as |
//! |---|---|---|
//! | `yield_now` | `Promise.resolve().then(..)` | microtask |
//! | channel/`Notify` wake | promise resolution | microtask |
//! | initiating I/O | issuing the request | inline (microtask) |
//! | **I/O completion** (CQE / readiness) | I/O callback (poll phase) | **macro turn** |
//! | timer expiry (`sleep`) | `setTimeout` callback | macro turn |
//!
//! The crucial invariant is that an **I/O completion never preempts the
//! microtask queue** — it always defers to a macro turn, exactly as a
//! filesystem/network callback does in Node's poll phase. This is what keeps a
//! busy microtask producer from being the only way the loop ever makes
//! progress, and what prevents timers/I-O from starving each other abnormally.
//!
//! Each test runs the loop on its own OS thread (mirroring the time-module
//! tests) and inspects the recorded order after the loop drains.

use std::cell::RefCell;
use std::rc::Rc;
use std::time::Duration;

use runite::channel::oneshot;
use runite::net::{TcpListener, TcpStream};
use runite::time::sleep;
use runite::{queue_macrotask, queue_microtask, run, spawn, yield_now};

type Order = Rc<RefCell<Vec<&'static str>>>;

/// Runs `build` on a dedicated runtime thread, drives the loop to completion,
/// and returns the recorded order. `build` spawns tasks / queues callbacks that
/// push labels into the shared [`Order`].
fn record_order(build: impl FnOnce(&Order) + Send + 'static) -> Vec<&'static str> {
    std::thread::spawn(move || {
        let order: Order = Rc::new(RefCell::new(Vec::new()));
        build(&order);
        run();
        Rc::try_unwrap(order)
            .expect("all tasks should have completed and dropped their handles")
            .into_inner()
    })
    .join()
    .expect("runtime thread panicked")
}

/// Establishes a loopback TCP connection and returns `(server, client)` with
/// **no bytes buffered**. Reading `server` therefore parks on driver readiness
/// rather than completing inline from the socket buffer — which is exactly what
/// we need to exercise the I/O-completion (macro-turn) wake path. The caller
/// keeps `client` alive (or hands it to a writer) to hold the connection open.
async fn connected_pair() -> (TcpStream, TcpStream) {
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let addr = listener.local_addr().expect("local_addr");
    let accept = spawn(async move { listener.accept().await.expect("accept").0 });
    let client = TcpStream::connect(addr).await.expect("connect");
    let server = accept.await.expect("join accept task");
    (server, client)
}

/// Spawns a task that writes a single byte through `client` and then drops it.
/// On loopback the data byte is delivered ahead of the subsequent FIN, so a
/// one-byte read on the peer succeeds. Writing from a *separate* task (rather
/// than pre-buffering before the read) guarantees the reader is first polled on
/// an empty socket and genuinely parks on the driver.
fn spawn_byte_writer(mut client: TcpStream) {
    spawn(async move {
        client.write_all(b"x").await.expect("write pending byte");
    });
}

#[test]
fn microtask_drains_before_macrotask() {
    let order = record_order(|order| {
        {
            let order = Rc::clone(order);
            queue_macrotask(move || order.borrow_mut().push("macrotask"));
        }
        {
            let order = Rc::clone(order);
            queue_microtask(move || order.borrow_mut().push("microtask"));
        }
    });
    assert_eq!(order.as_slice(), ["microtask", "macrotask"]);
}

#[test]
fn yield_now_is_a_microtask_and_beats_a_pending_macrotask() {
    let order = record_order(|order| {
        let order = Rc::clone(order);
        spawn(async move {
            {
                let order = Rc::clone(&order);
                queue_macrotask(move || order.borrow_mut().push("macrotask"));
            }
            // `yield_now` is the moral equivalent of `Promise.resolve()`: a
            // microtask that does NOT wait for a macro turn. The continuation
            // therefore runs before the macrotask queued above.
            yield_now().await;
            order.borrow_mut().push("after_yield");
        });
    });
    assert_eq!(order.as_slice(), ["after_yield", "macrotask"]);
}

#[test]
fn inprocess_channel_wake_is_a_microtask_and_beats_a_pending_macrotask() {
    let order = record_order(|order| {
        let (tx, mut rx) = oneshot::channel::<()>();
        {
            let order = Rc::clone(order);
            spawn(async move {
                let _ = rx.recv().await;
                order.borrow_mut().push("after_recv");
            });
        }
        {
            let order = Rc::clone(order);
            spawn(async move {
                {
                    let order = Rc::clone(&order);
                    queue_macrotask(move || order.borrow_mut().push("macrotask"));
                }
                // Resolving an in-process primitive wakes the waiter as a
                // microtask, like resolving a Promise — so the receiver's
                // continuation runs before the pending macrotask.
                let _ = tx.send(());
            });
        }
    });
    assert_eq!(order.as_slice(), ["after_recv", "macrotask"]);
}

#[test]
fn sleep_completion_takes_a_macro_turn() {
    let order = record_order(|order| {
        {
            let order = Rc::clone(order);
            queue_macrotask(move || order.borrow_mut().push("macrotask"));
        }
        {
            let order = Rc::clone(order);
            spawn(async move {
                sleep(Duration::ZERO).await;
                order.borrow_mut().push("after_sleep");
            });
        }
    });
    // A timer resolves on a macro turn (like `setTimeout`), so the macrotask
    // queued before the sleep runs first.
    assert_eq!(order.as_slice(), ["macrotask", "after_sleep"]);
}

#[test]
fn io_completion_takes_a_macro_turn() {
    let order = record_order(|order| {
        let order = Rc::clone(order);
        spawn(async move {
            let (mut server, client) = connected_pair().await;
            // Read on an *empty* socket so this future parks on the driver; a
            // separate task supplies the byte. The readiness wake is therefore a
            // real I/O completion, which must take a macro turn.
            spawn_byte_writer(client);
            {
                let order = Rc::clone(&order);
                queue_macrotask(move || order.borrow_mut().push("macrotask"));
            }
            let mut buf = [0u8; 1];
            server
                .read_exact(&mut buf)
                .await
                .expect("read pending byte");
            order.borrow_mut().push("after_io");
        });
    });
    // The I/O completion must defer to a macro turn (like a Node poll-phase
    // callback). The macrotask was enqueued at poll time, before the readiness
    // wake was reaped, so it runs first. Before the fix, the same-thread
    // completion woke the future as a microtask and "after_io" raced ahead.
    assert_eq!(order.as_slice(), ["macrotask", "after_io"]);
}

#[test]
fn io_completion_does_not_preempt_a_microtask_chain() {
    const CHAIN: usize = 16;
    const LABELS: [&str; CHAIN] = [
        "m0", "m1", "m2", "m3", "m4", "m5", "m6", "m7", "m8", "m9", "m10", "m11", "m12", "m13",
        "m14", "m15",
    ];

    fn pump(order: Order, next: usize) {
        if next >= CHAIN {
            return;
        }
        order.borrow_mut().push(LABELS[next]);
        let order = Rc::clone(&order);
        queue_microtask(move || pump(order, next + 1));
    }

    let order = record_order(|order| {
        let order = Rc::clone(order);
        spawn(async move {
            let (mut server, client) = connected_pair().await;
            // Read on an empty socket so the future parks on driver readiness;
            // a separate task supplies the byte, making the resumption a genuine
            // I/O completion (a macro turn) rather than an inline read.
            spawn_byte_writer(client);
            // Kick off a self-perpetuating microtask chain, then park on the read.
            pump(Rc::clone(&order), 0);
            let mut buf = [0u8; 1];
            server
                .read_exact(&mut buf)
                .await
                .expect("read pending byte");
            order.borrow_mut().push("after_io");
        });
    });
    // Microtasks have absolute priority and run to exhaustion before any macro
    // turn. Because the I/O completion is a macro turn, "after_io" must come
    // strictly after the entire microtask chain — never interleaved.
    let mut expected: Vec<&'static str> = LABELS.to_vec();
    expected.push("after_io");
    assert_eq!(order, expected);
}
