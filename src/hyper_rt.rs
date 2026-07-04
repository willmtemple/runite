//! Hyper runtime glue: an executor and a timer backed by runite.
//!
//! Hyper 1.x is runtime-agnostic: beyond the transport traits (implemented for
//! [`TcpStream`](crate::net::TcpStream) — and, on Unix, `net::unix::UnixStream`
//! — under this same `hyper` feature), some of its machinery needs a way to
//! *spawn* internal futures and a way to *sleep*:
//!
//! - HTTP/2 (client and server) multiplexes streams onto one connection and
//!   spawns per-stream futures through a [`hyper::rt::Executor`] —
//!   [`RuniteExecutor`] here.
//! - Timed options — `http1::Builder::header_read_timeout`, HTTP/2 keep-alive,
//!   and friends — need a [`hyper::rt::Timer`]; without one they panic at
//!   runtime with "no timer configured". [`RuniteTimer`] provides it.
//!
//! Both are zero-sized handles onto the *current runtime thread*: hyper calls
//! them from within connection tasks that already run on this thread's event
//! loop, so everything a connection spawns or schedules stays local, in keeping
//! with the event-loop-per-thread model.
//!
//! # Examples
//!
//! Serving HTTP/2 with hyper on a runite thread:
//!
//! ```no_run
//! use runite::hyper_rt::{RuniteExecutor, RuniteTimer};
//!
//! # async fn example(stream: runite::net::TcpStream) -> Result<(), Box<dyn std::error::Error>> {
//! let service = hyper::service::service_fn(|_req| async {
//!     Ok::<_, std::convert::Infallible>(hyper::Response::new(String::from("hi")))
//! });
//!
//! hyper::server::conn::http2::Builder::new(RuniteExecutor)
//!     .timer(RuniteTimer)
//!     .serve_connection(stream, service)
//!     .await?;
//! # Ok(())
//! # }
//! ```

use core::future::Future;
use core::pin::Pin;
use core::task::{Context, Poll};
use std::time::{Duration, Instant};

use hyper::rt::{Sleep, Timer};

use crate::op::completion::{CompletionFuture, completion_for_current_thread};

/// A [`hyper::rt::Executor`] that spawns hyper's internal futures onto the
/// current runite thread.
///
/// Hyper's connection machinery (most importantly the per-stream futures of an
/// HTTP/2 connection) hands futures to this executor from within connection
/// tasks already running on a runite event loop, so [`crate::spawn`] places
/// them on the same thread. The futures are detached; hyper manages their
/// lifetimes through its own connection state.
///
/// Calling `execute` on a thread with no runite runtime installed would
/// lazily initialize one there (see [`crate::spawn`]); in normal use hyper
/// only invokes it on the thread driving the connection.
#[derive(Clone, Copy, Debug, Default)]
pub struct RuniteExecutor;

impl<F> hyper::rt::Executor<F> for RuniteExecutor
where
    F: Future + 'static,
    F::Output: 'static,
{
    fn execute(&self, future: F) {
        // Detached: dropping the JoinHandle does not cancel the task.
        drop(crate::spawn(future));
    }
}

/// A [`hyper::rt::Timer`] backed by runite's timer wheel.
///
/// Unlocks hyper's timed options (`header_read_timeout`, HTTP/2 keep-alive,
/// etc.), which panic at runtime when no timer is configured.
///
/// Hyper requires its sleep futures to be `Send + Sync`, while runite's own
/// [`Sleep`](crate::time::Sleep) is thread-local. The sleeps produced here are
/// therefore built on the runtime's thread-safe completion primitive: the
/// timer is registered on the runtime thread that first polls the sleep (for
/// hyper, the thread driving the connection), and completing or cancelling it
/// routes through that owner thread. Dropping a sleep on its owning thread
/// cancels the underlying timer immediately; dropping it on another thread is
/// best-effort — the timer fires at its deadline as a no-op, briefly keeping
/// that loop alive.
#[derive(Clone, Copy, Debug, Default)]
pub struct RuniteTimer;

impl Timer for RuniteTimer {
    fn sleep(&self, duration: Duration) -> Pin<Box<dyn Sleep>> {
        Box::pin(RuniteSleep::new(Instant::now() + duration))
    }

    fn sleep_until(&self, deadline: Instant) -> Pin<Box<dyn Sleep>> {
        Box::pin(RuniteSleep::new(deadline))
    }
}

/// `Send + Sync` sleep future for [`RuniteTimer`].
///
/// Lazily arms a runtime timeout on first poll (which must happen on a runite
/// thread — for hyper, the connection's thread). The completion pair carries
/// the wake back to whichever task polls this future, and its cancel hook
/// tears the timeout down if the sleep is dropped early.
struct RuniteSleep {
    deadline: Instant,
    wait: Option<CompletionFuture<()>>,
}

impl RuniteSleep {
    fn new(deadline: Instant) -> Self {
        Self {
            deadline,
            wait: None,
        }
    }
}

impl Future for RuniteSleep {
    type Output = ();

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<()> {
        let this = self.get_mut();

        if this.wait.is_none() {
            let remaining = this.deadline.saturating_duration_since(Instant::now());
            if remaining.is_zero() {
                return Poll::Ready(());
            }

            // Register on the current (runtime) thread: a completion pair plus
            // a runtime timeout that completes it at the deadline.
            let (future, handle) = completion_for_current_thread::<()>();
            let complete = handle.clone();
            let timeout = crate::time::set_timeout(remaining, move || {
                complete.complete(());
            });
            // Dropping the sleep before the deadline runs this hook: cancel the
            // timeout (same-thread: removed immediately; cross-thread: a no-op,
            // and the later firing completes an already-finished handle) and
            // resolve the completion's bookkeeping.
            let cancel_handle = handle.clone();
            handle.set_cancel(move || {
                timeout.cancel();
                cancel_handle.finish(None);
            });

            this.wait = Some(future);
        }

        Pin::new(this.wait.as_mut().expect("sleep wait future must exist")).poll(cx)
    }
}

impl Sleep for RuniteSleep {}
