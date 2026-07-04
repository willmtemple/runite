//! # Chat server: shared mutable state with no locks at all
//!
//! A small collaborative-session backend — the shape behind a chat room, a
//! game lobby, or a multiplayer document. Every connected client can see and
//! affect every other client, which means *shared mutable state*, which on a
//! work-stealing runtime means `Arc<Mutex<HashMap<...>>>` and `Send + 'static`
//! bounds on everything.
//!
//! Here the entire room is:
//!
//! ```text
//! Rc<RefCell<HashMap<usize, Peer>>>
//! ```
//!
//! No `Arc`, no `Mutex`, no lock ordering, no poisoning, no `Send` bounds.
//! Every connection task runs on this one event loop, tasks only yield at
//! `.await` points, and `RefCell` borrows never cross an `.await` — so Rust
//! can verify at compile time what a locking design only promises at runtime.
//! This is the browser/Node concurrency model applied to a socket server.
//!
//! Also shown: **graceful shutdown**. Ctrl-C flips a `watch` channel; the
//! accept loop (a `select!` over accept-vs-shutdown) stops taking new
//! connections, everyone gets a farewell message, and the server drains with
//! a deadline.
//!
//! Run it for real:            `cargo run --example chat_server`
//!   then in other terminals:  `nc 127.0.0.1 <printed port>`
//!   commands: `/who`, `/quit`, Ctrl-C on the server to shut down.
//! Or watch the scripted demo: `cargo run --example chat_server -- --demo`

use std::cell::RefCell;
use std::collections::HashMap;
use std::net::SocketAddr;
use std::rc::Rc;
use std::time::Duration;

use runite::channel::{mpsc, watch};
use runite::io::{AsyncWriteExt, BufReader};
use runite::net::{TcpListener, TcpStream};
use runite::time::{sleep, timeout};
use runite::{select, spawn};

struct Peer {
    name: String,
    /// Outbox for this peer. Broadcasting is a synchronous walk of the room
    /// doing `try_send` — the per-peer writer task does the actual awaited
    /// I/O, so we never hold a `RefCell` borrow across an `.await`.
    outbox: mpsc::Sender<String>,
}

/// The whole server state. Plain single-threaded sharing.
type Room = Rc<RefCell<HashMap<usize, Peer>>>;

fn broadcast(room: &Room, line: &str) {
    println!("{line}");
    for peer in room.borrow().values() {
        // A peer that has fallen 32 messages behind just misses one; a slow
        // reader must not be able to stall the room.
        let _ = peer.outbox.try_send(format!("{line}\n"));
    }
}

/// Serve one client: register it, echo its lines to the room, clean up.
async fn serve(room: Room, id: usize, stream: TcpStream) {
    let (read_half, mut write_half) = stream.into_split();
    let name = format!("guest-{id}");

    // Register. Mutating the shared room is just... mutating it.
    let (outbox, mut inbox) = mpsc::channel::<String>(32);
    room.borrow_mut().insert(id, Peer { name: name.clone(), outbox });

    // Writer: drains this peer's outbox onto the socket. Ends when the peer
    // is removed from the room (all senders to the inbox are gone).
    let writer = spawn(async move {
        while let Some(message) = inbox.recv().await {
            if write_half.write_all(message.as_bytes()).await.is_err() {
                break;
            }
        }
        let _ = write_half.shutdown().await;
    });

    broadcast(&room, &format!("* {name} joined"));

    // Reader: one line per turn of the loop.
    let mut lines = BufReader::new(read_half);
    let mut line = String::new();
    loop {
        line.clear();
        match lines.read_line(&mut line).await {
            Ok(0) | Err(_) => break, // disconnected
            Ok(_) => match line.trim() {
                "" => {}
                "/quit" => break,
                "/who" => {
                    let names: Vec<String> =
                        room.borrow().values().map(|p| p.name.clone()).collect();
                    if let Some(peer) = room.borrow().get(&id) {
                        let _ = peer.outbox.try_send(format!("* here: {}\n", names.join(", ")));
                    }
                }
                text => broadcast(&room, &format!("{name}: {text}")),
            },
        }
    }

    // Deregister (dropping the outbox sender ends the writer) and announce.
    room.borrow_mut().remove(&id);
    broadcast(&room, &format!("* {name} left"));
    let _ = writer.await;
}

/// Accept until told to shut down, then say goodbye and drain.
async fn run_server(
    room: Room,
    listener: TcpListener,
    mut shutdown: watch::Receiver<bool>,
) -> std::io::Result<()> {
    let mut next_id = 1usize;
    loop {
        // Two futures race: a new connection or the shutdown flag. `select!`
        // drops the loser; a half-completed accept is cancelled in the driver.
        // (The futures are created first so the macro moves the *futures*, not
        // the listener/receiver themselves, letting the loop reuse them.)
        let accept = listener.accept();
        let changed = shutdown.changed();
        let accepted = select! {
            accepted = accept => Some(accepted),
            _ = changed => None,
        };
        match accepted {
            Some(accepted) => {
                let (stream, _peer_addr) = accepted?;
                let id = next_id;
                next_id += 1;
                spawn(serve(Rc::clone(&room), id, stream));
            }
            None => break,
        }
    }

    // Graceful drain: tell everyone, then wait (bounded) for the room to
    // empty as clients disconnect. If someone lingers past the deadline, we
    // exit anyway — shutdown must not hang on a stuck client.
    broadcast(&room, "* server: shutting down, goodbye");
    let drained = timeout(Duration::from_secs(2), async {
        while !room.borrow().is_empty() {
            sleep(Duration::from_millis(25)).await;
        }
    })
    .await;
    match drained {
        Ok(()) => println!("* all clients drained"),
        Err(_) => println!("* drain deadline reached with clients still connected"),
    }
    Ok(())
}

/// A scripted client for `--demo` mode.
async fn demo_client(addr: SocketAddr, script: &[&str]) -> std::io::Result<()> {
    let mut stream = TcpStream::connect(addr).await?;
    for line in script {
        sleep(Duration::from_millis(60)).await;
        stream.write_all(format!("{line}\n").as_bytes()).await?;
    }
    sleep(Duration::from_millis(60)).await;
    stream.shutdown(std::net::Shutdown::Both).await
}

#[runite::main]
async fn main() -> std::io::Result<()> {
    let room: Room = Rc::new(RefCell::new(HashMap::new()));
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let addr = listener.local_addr()?;
    let (shutdown_tx, shutdown_rx) = watch::channel(false);

    if std::env::args().any(|arg| arg == "--demo") {
        // Scripted session: two clients chat, one asks who's here, both leave,
        // then the demo driver requests shutdown — same code path as Ctrl-C.
        spawn(async move {
            let alice = spawn(demo_client(addr, &["hello room", "/who", "anyone here?"]));
            sleep(Duration::from_millis(30)).await;
            let bob = spawn(demo_client(addr, &["hi!", "gotta run", "/quit"]));
            let _ = alice.await;
            let _ = bob.await;
            sleep(Duration::from_millis(100)).await;
            let _ = shutdown_tx.send(true);
        });
    } else {
        println!("chat server listening — connect with: nc 127.0.0.1 {}", addr.port());
        println!("Ctrl-C to shut down gracefully");
        // Ctrl-C flips the same watch channel the demo uses.
        spawn(async move {
            if runite::signal::ctrl_c().await.is_ok() {
                let _ = shutdown_tx.send(true);
            }
        });
    }

    run_server(room, listener, shutdown_rx).await
}
