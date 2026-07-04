//! Windows console control events.
//!
//! Windows delivers console control events (Ctrl-C, Ctrl-Break, window close,
//! user logoff, system shutdown) by invoking a handler routine registered with
//! `SetConsoleCtrlHandler` on a dedicated console-spawned thread. Unlike a
//! POSIX signal handler, that routine runs in a normal thread context, so
//! delivery here is direct: the handler walks the process-wide listener
//! registry, bumps each matching stream's generation counter, and queues a
//! wake macrotask onto the stream's owning runtime thread.
//!
//! Like the Unix backend, streams are thread-affine (`!Send`), events coalesce
//! by kind, and delivery is best-effort: closed runtime threads are skipped
//! and a wake can be dropped if a thread's macrotask queue is full.
//!
//! # Examples
//!
//! ```no_run
//! runite::spawn(async {
//!     let mut interrupts = runite::signal::windows::ctrl_c()
//!         .expect("Ctrl-C handler should install");
//!     interrupts.recv().await;
//!     eprintln!("received shutdown request");
//! });
//!
//! runite::run();
//! ```

use std::future::poll_fn;
use std::io;
use std::marker::PhantomData;
use std::rc::Rc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, OnceLock, Weak};
use std::task::{Poll, Waker};

use windows_sys::Win32::System::Console::{
    CTRL_BREAK_EVENT, CTRL_C_EVENT, CTRL_CLOSE_EVENT, CTRL_LOGOFF_EVENT, CTRL_SHUTDOWN_EVENT,
    SetConsoleCtrlHandler,
};

use crate::platform::current::runtime::ThreadHandle;

/// One listener stream's shared state, reachable from the handler thread.
struct StreamState {
    event: u32,
    thread: ThreadHandle,
    generation: AtomicU64,
    waker: Mutex<Option<Waker>>,
}

impl StreamState {
    /// Bumps the generation and queues a wake on the owning runtime thread.
    /// Runs on the console handler thread.
    fn notify(self: &Arc<Self>) {
        self.generation.fetch_add(1, Ordering::AcqRel);
        let state = Arc::clone(self);
        // Best-effort: a closed thread can no longer observe the event, and a
        // full queue drops the wake (the generation bump still lets the next
        // poll observe it).
        let _ = self.thread.queue_macrotask(move || {
            if let Some(waker) = state
                .waker
                .lock()
                .expect("ctrl waker mutex poisoned")
                .take()
            {
                waker.wake();
            }
        });
    }
}

static REGISTRY: OnceLock<Mutex<Vec<Weak<StreamState>>>> = OnceLock::new();

fn registry() -> &'static Mutex<Vec<Weak<StreamState>>> {
    REGISTRY.get_or_init(|| Mutex::new(Vec::new()))
}

/// Process-wide console control handler.
///
/// Returns nonzero ("handled") when at least one live listener observed the
/// event, which stops the default processing (for `CTRL_C_EVENT`, process
/// termination). With no listeners the default disposition proceeds.
unsafe extern "system" fn ctrl_handler(event: u32) -> i32 {
    let listeners = {
        let mut registry = registry().lock().expect("ctrl registry mutex poisoned");
        registry.retain(|state| state.upgrade().is_some());
        registry
            .iter()
            .filter_map(Weak::upgrade)
            .filter(|state| state.event == event)
            .collect::<Vec<_>>()
    };

    let mut delivered = false;
    for state in listeners {
        state.notify();
        delivered = true;
    }

    i32::from(delivered)
}

fn install_handler() -> io::Result<()> {
    static INSTALL: OnceLock<io::Result<()>> = OnceLock::new();
    INSTALL
        .get_or_init(|| {
            // SAFETY: the handler is a `'static` function that only touches
            // process-global synchronized state.
            let installed = unsafe { SetConsoleCtrlHandler(Some(ctrl_handler), 1) };
            if installed == 0 {
                Err(io::Error::last_os_error())
            } else {
                Ok(())
            }
        })
        .as_ref()
        .map(|_| ())
        .map_err(|error| io::Error::new(error.kind(), error.to_string()))
}

fn new_stream(event: u32) -> io::Result<Arc<StreamState>> {
    install_handler()?;

    let state = Arc::new(StreamState {
        event,
        thread: crate::current_thread_handle(),
        generation: AtomicU64::new(0),
        waker: Mutex::new(None),
    });
    registry()
        .lock()
        .expect("ctrl registry mutex poisoned")
        .push(Arc::downgrade(&state));

    // Keep the owning event loop alive while the stream exists, matching the
    // Unix `Signal` semantics.
    state.thread.begin_async_operation();
    Ok(state)
}

macro_rules! ctrl_stream {
    (
        $(#[$meta:meta])*
        $name:ident, $factory:ident, $event:expr, $event_name:literal
    ) => {
        $(#[$meta])*
        ///
        /// The stream is tied to the runtime thread on which it was created and
        /// is intentionally `!Send`. Events coalesce by kind: several identical
        /// events arriving before the stream is polled may be observed as one.
        /// Dropping the stream unregisters it and lets the runtime exit if no
        /// other async operations are live; the process-wide console handler
        /// remains installed.
        pub struct $name {
            last_seen: u64,
            state: Arc<StreamState>,
            _not_send: PhantomData<Rc<()>>,
        }

        #[doc = concat!("Registers interest in `", $event_name, "` on the current runtime thread.")]
        ///
        /// Repeated calls share the process-wide console handler and return
        /// independent stream handles.
        pub fn $factory() -> io::Result<$name> {
            let state = new_stream($event)?;
            Ok($name {
                last_seen: state.generation.load(Ordering::Acquire),
                state,
                _not_send: PhantomData,
            })
        }

        impl $name {
            /// Waits for the next console control event observed by this
            /// stream.
            ///
            /// The current implementation returns `Some(())` when an event is
            /// observed and never produces `None`. The `Option` leaves room
            /// for a future closed-stream state without changing the method
            /// signature.
            pub async fn recv(&mut self) -> Option<()> {
                poll_fn(|cx| {
                    let current = self.state.generation.load(Ordering::Acquire);
                    if current != self.last_seen {
                        self.last_seen = current;
                        return Poll::Ready(Some(()));
                    }

                    let mut waker = self
                        .state
                        .waker
                        .lock()
                        .expect("ctrl stream waker mutex poisoned");
                    *waker = Some(cx.waker().clone());

                    let current = self.state.generation.load(Ordering::Acquire);
                    if current != self.last_seen {
                        self.last_seen = current;
                        *waker = None;
                        Poll::Ready(Some(()))
                    } else {
                        Poll::Pending
                    }
                })
                .await
            }
        }

        impl Drop for $name {
            fn drop(&mut self) {
                self.state.thread.finish_async_operation();
            }
        }
    };
}

ctrl_stream!(
    /// Stream of `CTRL_C_EVENT` console interrupts (Ctrl-C).
    CtrlC,
    ctrl_c,
    CTRL_C_EVENT,
    "CTRL_C_EVENT"
);

ctrl_stream!(
    /// Stream of `CTRL_BREAK_EVENT` console interrupts (Ctrl-Break).
    CtrlBreak,
    ctrl_break,
    CTRL_BREAK_EVENT,
    "CTRL_BREAK_EVENT"
);

ctrl_stream!(
    /// Stream of `CTRL_CLOSE_EVENT` notifications (console window closing).
    CtrlClose,
    ctrl_close,
    CTRL_CLOSE_EVENT,
    "CTRL_CLOSE_EVENT"
);

ctrl_stream!(
    /// Stream of `CTRL_LOGOFF_EVENT` notifications (user logging off).
    CtrlLogoff,
    ctrl_logoff,
    CTRL_LOGOFF_EVENT,
    "CTRL_LOGOFF_EVENT"
);

ctrl_stream!(
    /// Stream of `CTRL_SHUTDOWN_EVENT` notifications (system shutting down).
    CtrlShutdown,
    ctrl_shutdown,
    CTRL_SHUTDOWN_EVENT,
    "CTRL_SHUTDOWN_EVENT"
);
