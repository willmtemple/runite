//! Windows console control events.
//!
//! Windows delivers Ctrl-C and Ctrl-Break by invoking a handler routine
//! registered with `SetConsoleCtrlHandler` on a dedicated console-spawned
//! thread. Unlike a POSIX signal handler, that routine runs in a normal thread
//! context, so delivery here is direct: the handler walks the process-wide
//! listener registry, bumps each matching stream's generation counter, and
//! queues a capacity-bypassing internal wake onto the stream's owning runtime
//! thread.
//!
//! Like the Unix backend, streams are thread-affine (`!Send`) and events
//! coalesce by kind. A saturated user macrotask queue cannot discard a console
//! wake. The handler reports an event as handled only after a live stream's
//! wake has been accepted by the runtime's durable internal path.
//!
//! `CTRL_CLOSE_EVENT`, `CTRL_LOGOFF_EVENT`, and `CTRL_SHUTDOWN_EVENT` are
//! intentionally not exposed as async streams. Windows may terminate the
//! process as soon as a handler for those events returns, so waking an async
//! task cannot provide an honest cleanup guarantee. Programs needing those
//! lifecycle notifications should use the applicable synchronous Windows
//! service or window-message API.
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
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, OnceLock, Weak};
use std::task::{Poll, Waker};

use windows_sys::Win32::System::Console::{CTRL_BREAK_EVENT, CTRL_C_EVENT, SetConsoleCtrlHandler};

use crate::platform::current::runtime::ThreadHandle;

/// One listener stream's shared state, reachable from the handler thread.
struct StreamState {
    event: u32,
    thread: ThreadHandle,
    generation: AtomicU64,
    wake_scheduled: AtomicBool,
    active: AtomicBool,
    waker: Mutex<Option<Waker>>,
}

struct WakeScheduleReset<'a> {
    scheduled: &'a AtomicBool,
    armed: bool,
}

impl Drop for WakeScheduleReset<'_> {
    fn drop(&mut self) {
        if self.armed {
            self.scheduled.store(false, Ordering::Release);
        }
    }
}

impl StreamState {
    /// Bumps the generation and durably queues a coalesced wake on the owning
    /// runtime thread. Runs on the console handler thread.
    fn notify(self: &Arc<Self>) -> bool {
        if !self.active.load(Ordering::Acquire) || self.thread.is_closed() {
            return false;
        }

        self.generation.fetch_add(1, Ordering::AcqRel);
        if self.wake_scheduled.swap(true, Ordering::AcqRel) {
            return self.active.load(Ordering::Acquire) && !self.thread.is_closed();
        }

        let mut reset = WakeScheduleReset {
            scheduled: &self.wake_scheduled,
            armed: true,
        };
        let state = Arc::clone(self);
        if self
            .thread
            .queue_internal_wake(move || {
                state.wake_scheduled.store(false, Ordering::Release);
                if !state.active.load(Ordering::Acquire) {
                    return;
                }

                if let Some(waker) = state
                    .waker
                    .lock()
                    .expect("ctrl waker mutex poisoned")
                    .take()
                {
                    waker.wake();
                }
            })
            .is_err()
        {
            return false;
        }

        reset.armed = false;
        self.active.load(Ordering::Acquire) && !self.thread.is_closed()
    }
}

struct HandlerRegistry {
    listeners: Vec<Weak<StreamState>>,
}

impl HandlerRegistry {
    fn new() -> Self {
        Self {
            listeners: Vec::new(),
        }
    }
}

/// Serializes process handler installation and removal without participating
/// in callback dispatch. The Windows API may wait for an active callback, so
/// `SetConsoleCtrlHandler` must never run while the callback-visible registry
/// mutex is held.
struct HandlerLifecycle {
    installed: bool,
}

impl HandlerLifecycle {
    fn new() -> Self {
        Self { installed: false }
    }
}

static REGISTRY: OnceLock<Mutex<HandlerRegistry>> = OnceLock::new();
static HANDLER_LIFECYCLE: OnceLock<Mutex<HandlerLifecycle>> = OnceLock::new();

fn registry() -> &'static Mutex<HandlerRegistry> {
    REGISTRY.get_or_init(|| Mutex::new(HandlerRegistry::new()))
}

fn handler_lifecycle() -> &'static Mutex<HandlerLifecycle> {
    HANDLER_LIFECYCLE.get_or_init(|| Mutex::new(HandlerLifecycle::new()))
}

#[cfg(test)]
type TestHook = Box<dyn FnOnce() + Send + 'static>;

#[cfg(test)]
struct TestHooks {
    after_stream_published: Mutex<Option<TestHook>>,
    before_handler_registry: Mutex<Option<TestHook>>,
    before_handler_uninstall: Mutex<Option<TestHook>>,
}

#[cfg(test)]
impl TestHooks {
    fn new() -> Self {
        Self {
            after_stream_published: Mutex::new(None),
            before_handler_registry: Mutex::new(None),
            before_handler_uninstall: Mutex::new(None),
        }
    }
}

#[cfg(test)]
fn test_hooks() -> &'static TestHooks {
    static HOOKS: OnceLock<TestHooks> = OnceLock::new();
    HOOKS.get_or_init(TestHooks::new)
}

#[cfg(test)]
fn run_test_hook(slot: &Mutex<Option<TestHook>>) {
    let hook = slot
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .take();
    if let Some(hook) = hook {
        hook();
    }
}

/// Process-wide console control handler.
///
/// Returns nonzero ("handled") only when at least one live listener has a
/// durable pending wake. With no such listener the default disposition
/// proceeds.
unsafe extern "system" fn ctrl_handler(event: u32) -> i32 {
    std::panic::catch_unwind(|| ctrl_handler_inner(event)).unwrap_or_default()
}

fn ctrl_handler_inner(event: u32) -> i32 {
    #[cfg(test)]
    run_test_hook(&test_hooks().before_handler_registry);

    let listeners = {
        let mut registry = registry()
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let mut listeners = Vec::new();
        registry.listeners.retain(|listener| {
            let Some(state) = listener.upgrade() else {
                return false;
            };
            if state.event == event {
                listeners.push(state);
            }
            true
        });
        listeners
    };

    let mut handled = false;
    for state in listeners {
        handled |= state.notify();
    }
    i32::from(handled)
}

fn install_handler(lifecycle: &mut HandlerLifecycle) -> io::Result<()> {
    if lifecycle.installed {
        return Ok(());
    }

    // SAFETY: the handler is a `'static` function that only touches
    // process-global synchronized state.
    let installed = unsafe { SetConsoleCtrlHandler(Some(ctrl_handler), 1) };
    if installed == 0 {
        Err(io::Error::last_os_error())
    } else {
        lifecycle.installed = true;
        Ok(())
    }
}

fn uninstall_handler(lifecycle: &mut HandlerLifecycle) {
    if !lifecycle.installed {
        return;
    }

    // SAFETY: this removes the exact `'static` handler function installed by
    // `install_handler`.
    let removed = unsafe { SetConsoleCtrlHandler(Some(ctrl_handler), 0) };
    if removed != 0 {
        lifecycle.installed = false;
    } else {
        let error = io::Error::last_os_error();
        tracing::error!(
            target: crate::trace_targets::SIGNAL,
            event = "uninstall_handler_failed",
            code = error.raw_os_error().unwrap_or(0),
            %error,
            "failed to uninstall console control handler"
        );
    }
}

fn unregister(state: &Arc<StreamState>) {
    state.active.store(false, Ordering::Release);
    state
        .waker
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .take();

    let state_ptr = Arc::as_ptr(state);
    let mut lifecycle = handler_lifecycle()
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let registry_is_empty = {
        let mut registry = registry()
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        registry
            .listeners
            .retain(|listener| listener.as_ptr() != state_ptr && listener.strong_count() != 0);
        registry.listeners.is_empty()
    };

    if registry_is_empty {
        #[cfg(test)]
        run_test_hook(&test_hooks().before_handler_uninstall);
        uninstall_handler(&mut lifecycle);
    }
}

fn new_stream(event: u32) -> io::Result<(Arc<StreamState>, u64)> {
    let thread = crate::current_thread_handle();
    thread.begin_async_operation();

    let state = Arc::new(StreamState {
        event,
        thread,
        generation: AtomicU64::new(0),
        wake_scheduled: AtomicBool::new(false),
        active: AtomicBool::new(true),
        waker: Mutex::new(None),
    });
    // No handler can reach `state` before registry publication, so this is the
    // exact baseline from which the new stream must observe later events.
    let initial_generation = state.generation.load(Ordering::Acquire);

    let registered = {
        let mut lifecycle = handler_lifecycle()
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let registered = install_handler(&mut lifecycle);
        if registered.is_ok() {
            registry()
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .listeners
                .push(Arc::downgrade(&state));
        }
        registered
    };
    if let Err(error) = registered {
        state.active.store(false, Ordering::Release);
        state.thread.finish_async_operation();
        return Err(error);
    }

    #[cfg(test)]
    run_test_hook(&test_hooks().after_stream_published);

    Ok((state, initial_generation))
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
        /// other async operations are live. Dropping the last stream also
        /// removes runite's process-wide console handler.
        pub struct $name {
            last_seen: u64,
            state: Arc<StreamState>,
            _not_send: PhantomData<Rc<()>>,
        }

        impl std::fmt::Debug for $name {
            fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.debug_struct(stringify!($name)).finish_non_exhaustive()
            }
        }

        #[doc = concat!("Registers interest in `", $event_name, "` on the current runtime thread.")]
        ///
        /// Repeated calls share the process-wide console handler and return
        /// independent stream handles.
        pub fn $factory() -> io::Result<$name> {
            let (state, last_seen) = new_stream($event)?;
            Ok($name {
                last_seen,
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
                unregister(&self.state);
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

#[cfg(test)]
mod tests {
    use super::*;
    use std::future::Future;
    use std::process::{Command, Stdio};
    use std::sync::mpsc;
    use std::thread;
    use std::time::{Duration, Instant};

    const LIFECYCLE_HELPER_ENV: &str = "RUNITE_WINDOWS_HANDLER_LIFECYCLE_HELPER";
    const BARRIER_TIMEOUT: Duration = Duration::from_secs(2);
    const TEST_TIMEOUT: Duration = Duration::from_secs(10);

    #[test]
    fn handler_lifecycle_and_delivery_guarantees() {
        if std::env::var_os(LIFECYCLE_HELPER_ENV).is_none() {
            let mut child = Command::new(
                std::env::current_exe().expect("unit test executable should be available"),
            )
            .args([
                "--exact",
                "signal::windows::tests::handler_lifecycle_and_delivery_guarantees",
                "--nocapture",
                "--test-threads=1",
            ])
            .env(LIFECYCLE_HELPER_ENV, "1")
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .expect("handler lifecycle subprocess should start");

            let deadline = Instant::now() + TEST_TIMEOUT;
            loop {
                if child
                    .try_wait()
                    .expect("handler lifecycle subprocess status should be readable")
                    .is_some()
                {
                    break;
                }
                if Instant::now() >= deadline {
                    child
                        .kill()
                        .expect("timed-out handler lifecycle subprocess should be killed");
                    let output = child
                        .wait_with_output()
                        .expect("timed-out subprocess output should be readable");
                    panic!(
                        "handler lifecycle subprocess timed out\nstdout:\n{}\nstderr:\n{}",
                        String::from_utf8_lossy(&output.stdout),
                        String::from_utf8_lossy(&output.stderr),
                    );
                }
                thread::sleep(Duration::from_millis(10));
            }

            let output = child
                .wait_with_output()
                .expect("handler lifecycle subprocess output should be readable");
            assert!(
                output.status.success(),
                "handler lifecycle subprocess failed with {}\nstdout:\n{}\nstderr:\n{}",
                output.status,
                String::from_utf8_lossy(&output.stdout),
                String::from_utf8_lossy(&output.stderr),
            );
            return;
        }

        {
            let lifecycle = handler_lifecycle()
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            let registry = registry()
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            assert!(!lifecycle.installed);
            assert!(registry.listeners.is_empty());
        }

        let (published_tx, published_rx) = mpsc::sync_channel(0);
        let (handled_tx, handled_rx) = mpsc::sync_channel(0);
        *test_hooks()
            .after_stream_published
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(Box::new(move || {
            published_tx
                .send(())
                .expect("publication barrier receiver should remain live");
            assert_eq!(
                handled_rx.recv_timeout(BARRIER_TIMEOUT),
                Ok(1),
                "the handler should deliver during stream construction"
            );
        }));
        let handler = thread::spawn(move || {
            published_rx
                .recv_timeout(BARRIER_TIMEOUT)
                .expect("stream should reach its publication barrier");
            let handled = ctrl_handler_inner(CTRL_C_EVENT);
            handled_tx
                .send(handled)
                .expect("construction hook should receive handler result");
            handled
        });

        let mut raced_stream = ctrl_c().expect("Ctrl-C stream should survive publication race");
        assert_eq!(handler.join().expect("handler thread should not panic"), 1);
        assert_ne!(
            raced_stream.last_seen,
            raced_stream.state.generation.load(Ordering::Acquire),
            "an event delivered after publication must remain observable"
        );
        let mut receive = Box::pin(raced_stream.recv());
        let mut cx = std::task::Context::from_waker(std::task::Waker::noop());
        assert!(matches!(
            receive.as_mut().poll(&mut cx),
            Poll::Ready(Some(()))
        ));
        drop(receive);
        drop(raced_stream);
        crate::run_ready_tasks();

        let ctrl_c_stream = ctrl_c().expect("Ctrl-C stream should install the handler");
        let ctrl_break_stream = ctrl_break().expect("Ctrl-Break stream should share the handler");
        {
            let lifecycle = handler_lifecycle()
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            let registry = registry()
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            assert!(lifecycle.installed);
            assert_eq!(registry.listeners.len(), 2);
        }

        drop(ctrl_c_stream);
        assert!(
            handler_lifecycle()
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .installed,
            "the handler should remain while one receiver is live"
        );

        drop(ctrl_break_stream);
        {
            let lifecycle = handler_lifecycle()
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            let registry = registry()
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            assert!(!lifecycle.installed);
            assert!(registry.listeners.is_empty());
        }

        let ctrl_break = ctrl_break().expect("Ctrl-Break stream should install the handler");
        let (entered_tx, entered_rx) = mpsc::sync_channel(0);
        let (continue_tx, continue_rx) = mpsc::sync_channel(0);
        *test_hooks()
            .before_handler_registry
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(Box::new(move || {
            entered_tx
                .send(())
                .expect("unregister test should observe handler entry");
            continue_rx
                .recv_timeout(BARRIER_TIMEOUT)
                .expect("unregister should release the handler");
        }));

        let (done_tx, done_rx) = mpsc::sync_channel(0);
        let handler = thread::spawn(move || {
            let handled = ctrl_handler_inner(CTRL_BREAK_EVENT);
            done_tx
                .send(handled)
                .expect("unregister hook should receive handler completion");
            handled
        });
        entered_rx
            .recv_timeout(BARRIER_TIMEOUT)
            .expect("handler should pause before locking the registry");

        *test_hooks()
            .before_handler_uninstall
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(Box::new(move || {
            continue_tx
                .send(())
                .expect("paused handler should remain live");
            assert_eq!(
                done_rx.recv_timeout(BARRIER_TIMEOUT),
                Ok(0),
                "dispatch must acquire the registry before handler removal waits"
            );
        }));
        drop(ctrl_break);
        assert_eq!(handler.join().expect("handler thread should not panic"), 0);

        let ctrl_c_stream = ctrl_c().expect("Ctrl-C stream should reinstall the handler");
        ctrl_c_stream
            .state
            .thread
            .shared
            .closed
            .store(true, Ordering::Release);
        assert_eq!(
            ctrl_handler_inner(CTRL_C_EVENT),
            0,
            "an event without a durable target must not be reported as handled"
        );
        drop(ctrl_c_stream);
    }
}
