//! # Channel tour: mpsc and oneshot, on and across threads
//!
//! Demonstrates bounded mpsc, oneshot request/response, and using channels to
//! talk to a `spawn_worker` thread (runite's equivalent of a worker_threads
//! instance: a second, fully independent event loop). A feature tour; the
//! numbered log lines assert the exact expected ordering.
//!
//! Run it: `cargo run --example channel_showcase`

use runite::channel::{mpsc, oneshot};
use runite::{spawn, spawn_worker, time::sleep};
use std::fmt;
use std::sync::OnceLock;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

static START: OnceLock<Instant> = OnceLock::new();
static ACTUAL_ORDER: AtomicUsize = AtomicUsize::new(1);

macro_rules! log_event {
    ($expected:literal, $($arg:tt)*) => {{
        log_event_impl($expected, format_args!($($arg)*));
    }};
}

fn log_event_impl(expected: usize, message: fmt::Arguments<'_>) {
    let actual = ACTUAL_ORDER.fetch_add(1, Ordering::SeqCst);
    let elapsed = START
        .get()
        .expect("showcase start time should be initialized")
        .elapsed()
        .as_millis();
    println!(
        "[actual {actual:02} | expected {expected:02} | +{elapsed:04}ms | ts {}] {message}",
        unix_timestamp_millis(),
    );
}

fn unix_timestamp_millis() -> String {
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock should be after the Unix epoch");
    format!("{}.{:03}", now.as_secs(), now.subsec_millis())
}

enum WorkerEvent {
    Log(String),
    PresentRequest {
        frame: &'static str,
        ack: oneshot::Sender<&'static str>,
    },
}

#[runite::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    START.get_or_init(Instant::now);

    let (job_tx, mut job_rx) = mpsc::channel::<&'static str>(1);
    let (event_tx, mut event_rx) = mpsc::unbounded_channel::<WorkerEvent>();

    let worker = spawn_worker(
        move || {
            spawn(async move {
                while let Some(job) = job_rx.recv().await {
                    event_tx
                        .send(WorkerEvent::Log(format!(
                            "[worker] accepted job `{job}` from main thread"
                        )))
                        .unwrap_or_else(|_| {
                            panic!("worker should be able to report accepted jobs")
                        });

                    sleep(Duration::from_millis(20)).await;
                    if job == "upload-frame" {
                        let (ack_tx, mut ack_rx) = oneshot::channel();
                        event_tx
                            .send(WorkerEvent::PresentRequest {
                                frame: job,
                                ack: ack_tx,
                            })
                            .unwrap_or_else(|_| {
                                panic!("worker should be able to request presentation")
                            });
                        let ack = ack_rx
                            .recv()
                            .await
                            .expect("main thread should acknowledge frame");
                        event_tx
                            .send(WorkerEvent::Log(format!(
                                "[worker] got oneshot ack `{ack}` for `{job}`"
                            )))
                            .unwrap_or_else(|_| {
                                panic!("worker should be able to report ack reception")
                            });
                    }
                }

                event_tx
                    .send(WorkerEvent::Log(
                        "[worker] bounded command channel closed; worker is done".into(),
                    ))
                    .unwrap_or_else(|_| panic!("worker should be able to report shutdown"));
            });
        },
        || log_event!(12, "[main] worker exited"),
    );

    spawn(async move {
        log_event!(1, "[main] bounded mpsc send: enqueue `prepare-scene`");
        job_tx
            .send("prepare-scene")
            .await
            .expect("prepare-scene should be sent");

        log_event!(
            2,
            "[main] bounded mpsc send: enqueue `upload-frame` (fits once worker drains capacity)"
        );
        job_tx
            .send("upload-frame")
            .await
            .expect("upload-frame should be sent");

        log_event!(
            3,
            "[main] bounded mpsc send: enqueue `flush-stats` (waits for capacity/backpressure)"
        );
        job_tx
            .send("flush-stats")
            .await
            .expect("flush-stats should be sent");

        log_event!(
            5,
            "[main] drop bounded sender to close worker command stream"
        );
        drop(job_tx);
    });

    let mut event_count = 0usize;
    while let Some(event) = event_rx.recv().await {
        event_count += 1;
        match event {
            WorkerEvent::Log(message) => {
                let expected = match event_count {
                    1 => 4,
                    2 => 6,
                    4 => 9,
                    5 => 10,
                    6 => 11,
                    _ => 10 + event_count,
                };
                log_event_impl(expected, format_args!("{message}"));
            }
            WorkerEvent::PresentRequest { frame, ack } => {
                log_event!(
                    7,
                    "[main] unbounded mpsc recv: worker requests presentation for `{frame}`"
                );
                ack.send("presented")
                    .expect("main thread should be able to answer oneshot");
                log_event!(8, "[main] oneshot send: acknowledged frame presentation");
            }
        }
    }

    let _ = worker;
    Ok(())
}
