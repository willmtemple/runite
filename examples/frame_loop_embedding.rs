//! # Embedding runite in a host frame loop
//!
//! GUI frameworks and game engines own the thread: *they* run the outer loop,
//! and everything else gets a slice of each frame. runite is built to be
//! embedded in exactly that position — `run_until_stalled()` pumps the
//! runtime until no more work is ready *without blocking*, then hands the
//! thread straight back to the host.
//!
//! The shape, per frame:
//!
//! ```text
//! loop {                       // owned by the framework, not by runite
//!     dispatch_frame_event();  // like requestAnimationFrame callbacks
//!     runite::run_until_stalled();  // async tasks advance; microtasks flush
//!     render(&scene);          // paint from settled state
//!     sleep_until_vsync();
//! }
//! ```
//!
//! Because `run_until_stalled` drains the microtask queue before returning,
//! the render *always observes settled state* — the same guarantee a browser
//! gives you when it paints after the microtask checkpoint. And because tasks
//! live on this thread, the scene graph is a plain `Rc<RefCell<Scene>>` that
//! both the host's render and the async tasks touch directly.
//!
//! Three things animate the scene here:
//!   - a **frame-driven** task (awaits each frame event — `requestAnimationFrame`)
//!   - a **timer-driven** task (`sleep`, fires mid-run regardless of frames)
//!   - a **one-shot load** task (simulated fetch that flips a label)
//!
//! Run it: `cargo run --example frame_loop_embedding`

use std::cell::RefCell;
use std::rc::Rc;
use std::time::Duration;

use runite::channel::mpsc;
use runite::time::sleep;

const FRAMES: u32 = 30;
const FRAME_TIME: Duration = Duration::from_millis(20);
const WIDTH: usize = 24;

#[derive(Default)]
struct Scene {
    ball: usize,
    direction: i32,
    banner: &'static str,
    pulses: u32,
}

fn render(frame: u32, scene: &Scene) {
    let mut lane = vec![b'.'; WIDTH];
    lane[scene.ball] = b'o';
    println!(
        "frame {frame:>2} |{}| {:<12} pulses={}",
        String::from_utf8_lossy(&lane),
        scene.banner,
        scene.pulses,
    );
}

fn main() {
    let scene = Rc::new(RefCell::new(Scene {
        direction: 1,
        banner: "loading...",
        ..Scene::default()
    }));

    // The host's frame event, delivered over a channel: this is
    // requestAnimationFrame. Tasks that want to run once per frame await it.
    let (raf_tx, mut raf_rx) = mpsc::channel::<u32>(1);

    // Task 1 — frame-driven animation: one step per frame event, so its speed
    // is tied to the host's frame rate, exactly like a rAF callback.
    let animation = {
        let scene = Rc::clone(&scene);
        runite::spawn(async move {
            while let Some(_frame) = raf_rx.recv().await {
                let mut scene = scene.borrow_mut();
                let next = scene.ball as i32 + scene.direction;
                if next < 0 || next >= WIDTH as i32 {
                    scene.direction = -scene.direction;
                }
                scene.ball = (scene.ball as i32 + scene.direction) as usize;
            }
        })
    };

    // Task 2 — timer-driven: advances on wall-clock time, not frames. The
    // host's sleep between frames is when the driver notices expired timers;
    // the next pump fires them.
    {
        let scene = Rc::clone(&scene);
        runite::spawn(async move {
            for _ in 0..5 {
                sleep(Duration::from_millis(90)).await;
                scene.borrow_mut().pulses += 1;
            }
        });
    }

    // Task 3 — a simulated async load that flips UI state when it lands.
    {
        let scene = Rc::clone(&scene);
        runite::spawn(async move {
            sleep(Duration::from_millis(250)).await; // pretend network fetch
            scene.borrow_mut().banner = "data loaded";
        });
    }

    // ---- The host loop. Note this is NOT #[runite::main]: the frame loop
    // owns the thread, and runite is a guest inside it. ----
    for frame in 0..FRAMES {
        // Deliver this frame's rAF event (try_send: if the animation task is
        // somehow behind, skipping a frame beats stalling the host).
        let _ = raf_tx.try_send(frame);

        // Pump: runs every ready task, timer callback, and I/O completion,
        // flushes all microtasks, and returns without blocking the thread.
        runite::run_until_stalled();

        // Paint. Every mutation the pump produced is visible; none is torn.
        render(frame, &scene.borrow());

        std::thread::sleep(FRAME_TIME); // stand-in for vsync
    }

    // Host is done: close the frame channel and let the runtime finish any
    // straggler work (the animation task ends when the channel closes).
    drop(raf_tx);
    runite::run_until_stalled();
    drop(animation);

    let scene = scene.borrow();
    println!("---");
    println!(
        "{} frames; banner={:?}; {} timer pulses fired while the host owned the thread",
        FRAMES, scene.banner, scene.pulses
    );
    assert_eq!(
        scene.banner, "data loaded",
        "the async load should have landed"
    );
    assert!(
        scene.pulses >= 4,
        "timer task should have pulsed during the run"
    );
}
