//! # Command center: an interactive terminal app that never blocks on you
//!
//! The essential shape of an interactive application: one event loop, several
//! event sources, shared state, and *nothing* is allowed to block the loop.
//! You type commands at a prompt while background jobs run and report in —
//! the same architecture as a REPL with async completions, a chat client, or
//! any TUI.
//!
//! What to notice:
//!
//! - **The prompt task and every job share one `Rc<RefCell<JobBoard>>`.**
//!   They interleave on the event loop, never preempt each other mid-borrow,
//!   and need no `Mutex` — this is the Node/browser concurrency model, with
//!   Rust checking the borrows.
//! - **Input is async.** `runite::stdin()` reads lines without parking the
//!   thread, so jobs make progress *between your keystrokes*.
//! - **`quit` while jobs are running abandons them** — `#[runite::main]` ends
//!   when `main`'s future resolves, just like a JS process exiting or Tokio's
//!   `#[tokio::main]`. The example prints what got abandoned so the semantics
//!   are visible.
//!
//! Run it interactively:   `cargo run --example command_center`
//! Or watch the scripted demo: `cargo run --example command_center -- --demo`

use std::cell::RefCell;
use std::rc::Rc;
use std::time::{Duration, Instant};

use runite::time::sleep;

#[derive(Clone, Copy, PartialEq)]
enum JobState {
    Running,
    Done(Duration),
}

struct Job {
    name: String,
    started: Instant,
    state: JobState,
}

/// Shared, single-threaded application state. Every task on this loop may
/// borrow and mutate it directly.
#[derive(Default)]
struct JobBoard {
    jobs: Vec<Job>,
}

impl JobBoard {
    fn print_status(&self) {
        if self.jobs.is_empty() {
            println!("  (no jobs yet — try `job build 800`)");
            return;
        }
        for (id, job) in self.jobs.iter().enumerate() {
            match job.state {
                JobState::Running => {
                    println!("  #{id} {:<12} running ({:?})", job.name, job.started.elapsed())
                }
                JobState::Done(took) => println!("  #{id} {:<12} done in {took:?}", job.name),
            }
        }
    }

    fn unfinished(&self) -> Vec<&str> {
        self.jobs
            .iter()
            .filter(|job| job.state == JobState::Running)
            .map(|job| job.name.as_str())
            .collect()
    }
}

/// Start a background job: it sleeps for `duration` (standing in for real
/// work — a network call, a subprocess, a file crawl), then marks itself done.
/// The job holds its own `Rc` to the board and mutates it directly when it
/// finishes; no message-passing ceremony is needed to get data back to "the
/// main thread", because everything already *is* the main thread.
fn start_job(board: &Rc<RefCell<JobBoard>>, name: String, duration: Duration) {
    let id = {
        let mut board = board.borrow_mut();
        board.jobs.push(Job {
            name: name.clone(),
            started: Instant::now(),
            state: JobState::Running,
        });
        board.jobs.len() - 1
    };
    println!("started job #{id} `{name}` ({duration:?})");

    let board = Rc::clone(board);
    runite::spawn(async move {
        sleep(duration).await;
        let mut board = board.borrow_mut();
        let took = board.jobs[id].started.elapsed();
        board.jobs[id].state = JobState::Done(took);
        // A completion notification lands whenever the prompt is idle —
        // the input read in progress is not disturbed.
        println!("\u{2713} job #{id} `{}` finished in {took:?}", board.jobs[id].name);
    });
}

/// One command. Returns `false` when the app should exit.
fn dispatch(board: &Rc<RefCell<JobBoard>>, line: &str) -> bool {
    let mut words = line.split_whitespace();
    match words.next() {
        Some("job") => {
            let name = words.next().unwrap_or("work").to_string();
            let millis = words.next().and_then(|w| w.parse().ok()).unwrap_or(1000u64);
            start_job(board, name, Duration::from_millis(millis));
        }
        Some("status") => board.borrow().print_status(),
        Some("help") => {
            println!("  job <name> <millis>  start a background job");
            println!("  status               show the job board");
            println!("  quit                 exit");
        }
        Some("quit") => return false,
        Some(other) => println!("unknown command {other:?} (try `help`)"),
        None => {}
    }
    true
}

async fn interactive(board: Rc<RefCell<JobBoard>>) -> std::io::Result<()> {
    println!("command center — type `help` for commands");
    let mut input = runite::stdin()?;
    // This await parks *this task*, not the thread: jobs continue to run and
    // print their completions while we wait for the next line. The loop ends
    // on `quit` or EOF (e.g. piped input running out).
    while let Some(line) = input.read_line().await? {
        if !dispatch(&board, line.trim()) {
            break;
        }
    }
    Ok(())
}

async fn demo(board: Rc<RefCell<JobBoard>>) {
    let script: &[(&str, u64)] = &[
        ("job compile 300", 50),
        ("job test 150", 50),
        ("status", 250),
        // by now `test` has finished and printed its notification
        ("status", 200),
        ("job deploy 5000", 50),
        ("quit", 0),
    ];
    for (line, pause_after) in script {
        println!("> {line}");
        if !dispatch(&board, line) {
            break;
        }
        sleep(Duration::from_millis(*pause_after)).await;
    }
}

#[runite::main]
async fn main() -> std::io::Result<()> {
    let board = Rc::new(RefCell::new(JobBoard::default()));

    if std::env::args().any(|arg| arg == "--demo") {
        demo(Rc::clone(&board)).await;
    } else {
        interactive(Rc::clone(&board)).await?;
    }

    // `main` returning ends the program even if spawned tasks are pending —
    // the same contract as JS (`process` exits with the loop non-empty is
    // impossible, but nothing waits for detached promises) and Tokio's main.
    let unfinished = board.borrow().unfinished().join(", ");
    if unfinished.is_empty() {
        println!("goodbye — all jobs complete");
    } else {
        println!("goodbye — abandoning unfinished jobs: {unfinished}");
    }
    Ok(())
}
