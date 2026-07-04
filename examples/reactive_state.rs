//! # Reactive state: why the JavaScript event-loop model matters
//!
//! This example is the answer to "why does runite copy the browser's
//! microtask/macrotask scheduling?" — because deterministic flush points are
//! what make *reactive UI programming* tractable.
//!
//! The pattern demonstrated here is the one every UI framework converges on:
//!
//!   1. Application state lives in one place (the `Model`).
//!   2. Events (timers, I/O, user input) arrive as **macrotasks** and mutate
//!      the model — often several times per event.
//!   3. The first mutation in a turn schedules a **render microtask**.
//!      Further mutations in the same turn see the render already scheduled
//!      and do nothing.
//!   4. The microtask checkpoint runs *after the event handler finishes and
//!      before the next event is dispatched*, so the render sees a settled,
//!      fully-consistent model — and runs **once**, no matter how many
//!      mutations the event made.
//!
//! In JavaScript this is `queueMicrotask(render)` guarded by a dirty flag —
//! the coalescing trick underneath every signals/observable implementation.
//! It only works because the platform *guarantees* microtasks flush between
//! macrotasks. runite makes the same guarantee, so the same pattern works in
//! Rust — and because tasks never leave this thread, the model is a plain
//! `Rc<RefCell<Model>>`: no `Arc`, no `Mutex`, no `Send` bounds, and a data
//! race is a compile error rather than a runtime hazard.
//!
//! Run it: `cargo run --example reactive_state`
//! It prints one render per event-loop turn, then a summary showing how many
//! mutations were coalesced into how many renders.

use std::cell::{Cell, RefCell};
use std::rc::Rc;
use std::time::Duration;

use runite::channel::mpsc;
use runite::time::sleep;

/// The application state. Note what this is *not*: not `Arc<Mutex<...>>`,
/// not `Send`, not sprinkled with atomics. Every task that touches it runs on
/// this thread, so plain shared mutability is safe — the same reason a JS app
/// can use a plain object for its store.
#[derive(Default)]
struct Model {
    clock_ticks: u32,
    feed: Vec<String>,
    unread: u32,
    status: &'static str,
}

/// The reactive core: mutate-then-schedule-render, with coalescing.
struct App {
    model: RefCell<Model>,
    render_scheduled: Cell<bool>,
    mutations: Cell<u32>,
    renders: Cell<u32>,
}

impl App {
    fn new() -> Rc<Self> {
        Rc::new(Self {
            model: RefCell::new(Model {
                status: "starting",
                ..Model::default()
            }),
            render_scheduled: Cell::new(false),
            mutations: Cell::new(0),
            renders: Cell::new(0),
        })
    }

    /// Apply a mutation to the model and make sure a render is pending.
    ///
    /// This is the whole trick. The first mutation of a turn queues the render
    /// as a *microtask*; the runtime guarantees it runs after the current event
    /// handler returns and before the next macrotask (timer, I/O event) is
    /// dispatched. Every later mutation in the same turn finds
    /// `render_scheduled == true` and schedules nothing. One turn, one render,
    /// no matter how chatty the event handler was.
    fn mutate(self: &Rc<Self>, change: impl FnOnce(&mut Model)) {
        change(&mut self.model.borrow_mut());
        self.mutations.set(self.mutations.get() + 1);

        if !self.render_scheduled.replace(true) {
            let app = Rc::clone(self);
            runite::queue_microtask(move || app.render());
        }
    }

    /// The "paint". Runs at the microtask checkpoint with the model settled.
    fn render(&self) {
        self.render_scheduled.set(false);
        let frame = self.renders.get() + 1;
        self.renders.set(frame);

        let model = self.model.borrow();
        println!(
            "render #{frame:<2} clock={:<2} feed={:<2} unread={} status={:?}",
            model.clock_ticks,
            model.feed.len(),
            model.unread,
            model.status,
        );
    }
}

#[runite::main]
async fn main() {
    let app = App::new();

    // Event source 1: a clock. Each tick is one macrotask that performs TWO
    // mutations — and still produces exactly one render.
    let clock = {
        let app = Rc::clone(&app);
        runite::spawn(async move {
            for _ in 0..5 {
                sleep(Duration::from_millis(30)).await;
                app.mutate(|m| m.clock_ticks += 1);
                app.mutate(|m| m.status = "ticking");
            }
        })
    };

    // Event source 2: a simulated network feed delivered over a channel. Each
    // message triggers THREE mutations (append, unread count, status) — again
    // coalesced into one render per delivery.
    let (tx, mut rx) = mpsc::channel::<String>(8);
    let producer = runite::spawn(async move {
        for name in ["alice", "bob", "carol"] {
            sleep(Duration::from_millis(45)).await;
            tx.send(format!("message from {name}")).await.expect("send");
        }
    });
    let consumer = {
        let app = Rc::clone(&app);
        runite::spawn(async move {
            while let Some(message) = rx.recv().await {
                app.mutate(|m| m.feed.push(message));
                app.mutate(|m| m.unread += 1);
                app.mutate(|m| m.status = "new mail");
            }
        })
    };

    let _ = clock.await;
    let _ = producer.await;
    let _ = consumer.await;

    // One final settled frame.
    app.mutate(|m| m.status = "idle");
    runite::yield_now().await; // let the last render microtask flush

    let mutations = app.mutations.get();
    let renders = app.renders.get();
    println!("---");
    println!("{mutations} mutations coalesced into {renders} renders");
    assert!(
        renders < mutations,
        "coalescing should always produce fewer renders than mutations"
    );
}
