//! Broadcast and watch channel demo.
//!
//! Broadcast channels fan out every sent message to each active receiver. Watch
//! channels hold the latest value and let receivers await `changed()` before
//! borrowing the new state.

use runite::channel::{broadcast, watch};
use runite::{spawn, yield_now};

#[runite::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let (broadcast_tx, mut first_rx) = broadcast::channel(8);
    let mut second_rx = broadcast_tx.subscribe();

    let first_task = spawn(async move {
        let mut seen = Vec::new();
        for _ in 0..3 {
            seen.push(first_rx.recv().await?);
        }
        Ok::<Vec<&'static str>, broadcast::RecvError>(seen)
    });
    let second_task = spawn(async move {
        let mut seen = Vec::new();
        for _ in 0..3 {
            seen.push(second_rx.recv().await?);
        }
        Ok::<Vec<&'static str>, broadcast::RecvError>(seen)
    });

    for message in ["alpha", "beta", "gamma"] {
        let receivers = broadcast_tx.send(message)?;
        println!("[broadcast sender] sent {message:?} to {receivers} receivers");
    }

    let first_seen = first_task.await??;
    let second_seen = second_task.await??;
    println!("[broadcast rx1] {first_seen:?}");
    println!("[broadcast rx2] {second_seen:?}");
    assert_eq!(first_seen, second_seen);

    let (watch_tx, mut watch_rx) = watch::channel(String::from("booting"));
    let watcher = spawn(async move {
        println!("[watch rx] initial value: {}", *watch_rx.borrow());
        watch_rx.changed().await?;
        Ok::<String, watch::RecvError>(watch_rx.borrow_and_update().clone())
    });

    yield_now().await;
    watch_tx.send(String::from("ready"))?;
    let latest = watcher.await??;
    println!("[watch rx] changed to: {latest}");

    watch_tx.send_modify(|value| value.push_str(" + serving traffic"));
    println!("[watch sender] latest value: {}", *watch_tx.borrow());

    Ok(())
}
