//! Shared test bodies parameterised over a per-platform `Runtime`.
//!
//! Both `platform/linux/runtime.rs` and `platform/macos_aarch64/runtime.rs`
//! ship a small `mod tests` that pins these helpers to their concrete
//! marker type. Keeping the bodies here means the integration scenarios stay
//! in lockstep across platforms.

#![allow(dead_code)]

use std::any::Any;
use std::cell::{Cell, RefCell};
use std::collections::VecDeque;
use std::future::Future;
use std::io;
use std::marker::PhantomData;
use std::panic::resume_unwind;
use std::pin::Pin;
use std::rc::Rc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Condvar, Mutex, Weak, mpsc};
use std::task::{Context, Poll, Wake, Waker};
use std::thread;
use std::time::Duration;

use super::state::{teardown_thread, try_with_installed_thread};
use super::{
    DriverBackend, IntervalHandle, Notifier, ReadyEvents, Runtime, RuntimeConfig, SendTask,
    block_on, current_thread_handle, interval, queue_future, queue_microtask, queue_task, run,
    spawn_worker, timeout, yield_now,
};
use crate::op::completion::completion_for_current_thread;

/// Thread-safe append-only trace used by deterministic tests.
#[derive(Clone)]
pub(crate) struct EventTrace<E> {
    inner: Arc<(Mutex<Vec<E>>, Condvar)>,
}

impl<E> Default for EventTrace<E> {
    fn default() -> Self {
        Self {
            inner: Arc::new((Mutex::new(Vec::new()), Condvar::new())),
        }
    }
}

impl<E> EventTrace<E> {
    pub(crate) fn new() -> Self {
        Self::default()
    }

    pub(crate) fn record(&self, event: E) {
        let (events, changed) = &*self.inner;
        events.lock().expect("event trace poisoned").push(event);
        changed.notify_all();
    }

    pub(crate) fn len(&self) -> usize {
        self.inner.0.lock().expect("event trace poisoned").len()
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.inner
            .0
            .lock()
            .expect("event trace poisoned")
            .is_empty()
    }

    pub(crate) fn wait_for_len(&self, len: usize, timeout: Duration) -> bool {
        let (events, changed) = &*self.inner;
        let events = events.lock().expect("event trace poisoned");
        let (events, _) = changed
            .wait_timeout_while(events, timeout, |events| events.len() < len)
            .expect("event trace poisoned");
        events.len() >= len
    }

    pub(crate) fn wait_until(
        &self,
        timeout: Duration,
        mut predicate: impl FnMut(&[E]) -> bool,
    ) -> bool {
        let (events, changed) = &*self.inner;
        let events = events.lock().expect("event trace poisoned");
        let (events, _) = changed
            .wait_timeout_while(events, timeout, |events| !predicate(events))
            .expect("event trace poisoned");
        predicate(&events)
    }
}

impl<E: Clone> EventTrace<E> {
    pub(crate) fn snapshot(&self) -> Vec<E> {
        self.inner.0.lock().expect("event trace poisoned").clone()
    }
}

/// Common lifecycle vocabulary for operation-state tests.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum OperationEvent {
    Submitted(u64),
    Started(u64),
    Completed(u64),
    Cancelled(u64),
    Dropped(u64),
    Custom(&'static str),
}

pub(crate) type OperationTrace = EventTrace<OperationEvent>;

/// Observation handle paired with a [`DropSpy`].
#[derive(Clone)]
pub(crate) struct DropSpyHandle {
    drops: Arc<AtomicUsize>,
    trace: EventTrace<String>,
}

impl DropSpyHandle {
    pub(crate) fn count(&self) -> usize {
        self.drops.load(Ordering::Acquire)
    }

    pub(crate) fn trace(&self) -> Vec<String> {
        self.trace.snapshot()
    }
}

/// Records exactly one event when the value is dropped.
pub(crate) struct DropSpy {
    label: String,
    drops: Arc<AtomicUsize>,
    trace: EventTrace<String>,
}

impl DropSpy {
    pub(crate) fn new(label: impl Into<String>) -> (Self, DropSpyHandle) {
        let drops = Arc::new(AtomicUsize::new(0));
        let trace = EventTrace::new();
        (
            Self {
                label: label.into(),
                drops: Arc::clone(&drops),
                trace: trace.clone(),
            },
            DropSpyHandle { drops, trace },
        )
    }
}

impl Drop for DropSpy {
    fn drop(&mut self) {
        self.drops.fetch_add(1, Ordering::AcqRel);
        self.trace.record(self.label.clone());
    }
}

struct ReentrantWake {
    callback: Arc<dyn Fn() + Send + Sync>,
    wakes: AtomicUsize,
}

impl Wake for ReentrantWake {
    fn wake(self: Arc<Self>) {
        self.wakes.fetch_add(1, Ordering::AcqRel);
        (self.callback)();
    }

    fn wake_by_ref(self: &Arc<Self>) {
        self.wakes.fetch_add(1, Ordering::AcqRel);
        (self.callback)();
    }
}

/// Waker whose callback runs synchronously inside `wake`/`wake_by_ref`.
pub(crate) struct ReentrantWaker {
    state: Arc<ReentrantWake>,
}

impl ReentrantWaker {
    pub(crate) fn new(callback: impl Fn() + Send + Sync + 'static) -> Self {
        Self {
            state: Arc::new(ReentrantWake {
                callback: Arc::new(callback),
                wakes: AtomicUsize::new(0),
            }),
        }
    }

    pub(crate) fn waker(&self) -> Waker {
        Waker::from(Arc::clone(&self.state))
    }

    pub(crate) fn wake_count(&self) -> usize {
        self.state.wakes.load(Ordering::Acquire)
    }
}

/// Cooperative cancellation signal for in-process test helpers.
#[derive(Clone, Default)]
pub(crate) struct TestCancellation {
    inner: Arc<(Mutex<bool>, Condvar)>,
}

impl TestCancellation {
    fn cancel(&self) {
        let (cancelled, changed) = &*self.inner;
        *cancelled
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = true;
        changed.notify_all();
    }

    pub(crate) fn is_cancelled(&self) -> bool {
        *self
            .inner
            .0
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    pub(crate) fn wait_cancelled(&self) {
        let (cancelled, changed) = &*self.inner;
        let cancelled = cancelled
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let _cancelled = changed
            .wait_while(cancelled, |cancelled| !*cancelled)
            .unwrap_or_else(std::sync::PoisonError::into_inner);
    }
}

/// Runs cooperative work on a thread, cancelling and joining it on timeout.
///
/// The task must observe the supplied cancellation signal before performing
/// an unbounded wait. Tests for code that cannot cooperate must use process
/// isolation instead; this helper never detaches its worker thread.
pub(crate) fn completes_within<T>(
    timeout: Duration,
    task: impl FnOnce(TestCancellation) -> T + Send + 'static,
) -> T
where
    T: Send + 'static,
{
    let cancellation = TestCancellation::default();
    let worker_cancellation = cancellation.clone();
    let (sender, receiver) = mpsc::sync_channel(1);
    let worker = thread::Builder::new()
        .name("runite-test-helper".into())
        .spawn(move || {
            let output = task(worker_cancellation);
            let _ = sender.send(());
            output
        })
        .expect("test helper thread should spawn");

    let status = receiver.recv_timeout(timeout);
    let timed_out = matches!(status, Err(mpsc::RecvTimeoutError::Timeout));
    if timed_out {
        cancellation.cancel();
    }

    let output = match worker.join() {
        Ok(output) => output,
        Err(payload) => resume_unwind(payload),
    };
    match status {
        Ok(()) => output,
        Err(mpsc::RecvTimeoutError::Timeout) => {
            panic!("operation did not complete within {timeout:?}")
        }
        Err(mpsc::RecvTimeoutError::Disconnected) => {
            panic!("completion thread exited without publishing a result")
        }
    }
}

/// Joins a test thread on drop, signalling cancellation first when configured.
pub(crate) struct TrackedThread<T> {
    thread: Option<thread::JoinHandle<T>>,
    cancel: Option<Box<dyn FnOnce() + Send>>,
}

impl<T> TrackedThread<T> {
    pub(crate) fn new(thread: thread::JoinHandle<T>) -> Self {
        Self {
            thread: Some(thread),
            cancel: None,
        }
    }

    pub(crate) fn cancellable(
        thread: thread::JoinHandle<T>,
        cancel: impl FnOnce() + Send + 'static,
    ) -> Self {
        Self {
            thread: Some(thread),
            cancel: Some(Box::new(cancel)),
        }
    }

    pub(crate) fn join(mut self) -> thread::Result<T> {
        self.cancel();
        self.thread
            .take()
            .expect("test thread should only be joined once")
            .join()
    }

    fn cancel(&mut self) {
        if let Some(cancel) = self.cancel.take() {
            cancel();
        }
    }
}

impl<T> Drop for TrackedThread<T> {
    fn drop(&mut self) {
        self.cancel();
        if let Some(thread) = self.thread.take() {
            let _ = thread.join();
        }
    }
}

#[derive(Clone, Default)]
pub(crate) struct ExecutionGate {
    inner: Arc<(Mutex<ExecutionGateState>, Condvar)>,
}

#[derive(Default)]
struct ExecutionGateState {
    arrived: bool,
    released: bool,
    completed: bool,
}

impl ExecutionGate {
    pub(crate) fn arrive_and_wait(&self) {
        let (state, changed) = &*self.inner;
        let mut state = state.lock().expect("execution gate poisoned");
        state.arrived = true;
        changed.notify_all();
        while !state.released {
            state = changed.wait(state).expect("execution gate poisoned");
        }
    }

    pub(crate) fn mark_completed(&self) {
        let (state, changed) = &*self.inner;
        state.lock().expect("execution gate poisoned").completed = true;
        changed.notify_all();
    }

    pub(crate) fn release(&self) {
        let (state, changed) = &*self.inner;
        state.lock().expect("execution gate poisoned").released = true;
        changed.notify_all();
    }

    pub(crate) fn release_on_drop(&self) -> ExecutionGateRelease {
        ExecutionGateRelease {
            gate: Some(self.clone()),
        }
    }

    pub(crate) fn wait_until_arrived(&self, timeout: Duration) -> bool {
        self.wait_for(timeout, |state| state.arrived)
    }

    pub(crate) fn wait_until_completed(&self, timeout: Duration) -> bool {
        self.wait_for(timeout, |state| state.completed)
    }

    fn wait_for(&self, timeout: Duration, condition: impl Fn(&ExecutionGateState) -> bool) -> bool {
        let (state, changed) = &*self.inner;
        let state = state.lock().expect("execution gate poisoned");
        let (state, _) = changed
            .wait_timeout_while(state, timeout, |state| !condition(state))
            .expect("execution gate poisoned");
        condition(&state)
    }
}

/// Panic-safe release for a thread or singleton worker parked at a gate.
#[must_use = "dropping this guard is what releases the execution gate"]
pub(crate) struct ExecutionGateRelease {
    gate: Option<ExecutionGate>,
}

impl ExecutionGateRelease {
    pub(crate) fn release(mut self) {
        self.release_inner();
    }

    fn release_inner(&mut self) {
        if let Some(gate) = self.gate.take() {
            gate.release();
        }
    }
}

impl Drop for ExecutionGateRelease {
    fn drop(&mut self) {
        self.release_inner();
    }
}

#[derive(Clone, Debug)]
struct MockIoError {
    kind: io::ErrorKind,
    message: Arc<str>,
}

impl MockIoError {
    fn new(kind: io::ErrorKind, message: impl Into<Arc<str>>) -> Self {
        Self {
            kind,
            message: message.into(),
        }
    }

    fn to_io_error(&self) -> io::Error {
        io::Error::new(self.kind, self.message.to_string())
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum MockRuntimeEvent {
    DriverCreated(usize),
    DriverBound(usize),
    DriverBindPanicked(usize),
    DriverPolled(usize),
    DriverReady(usize, ReadyEvents),
    DriverWaitStarted(usize),
    DriverWaitFinished(usize),
    TimerRearmed(usize, Option<Duration>),
    WakeDrained(usize, u64),
    TimerDrained(usize, u64),
    NotifyAttempted(usize),
    NotifySucceeded(usize),
    NotifyFailed(usize, io::ErrorKind),
    CompletionDispatched(usize, String),
    DriverUnbound(usize),
    DriverDropped(usize),
    NotifierDropped(usize),
    ThreadSpawnAttempted,
    ThreadSpawnFailed(io::ErrorKind),
    ThreadStarted,
    ThreadFinished,
    ThreadJoined,
}

struct MockCompletion {
    label: String,
    task: Option<SendTask>,
}

struct MockReady {
    events: ReadyEvents,
    wakes: u64,
    timers: u64,
    completion: Option<MockCompletion>,
}

enum MockPoll {
    Ready(MockReady),
    Error(MockIoError),
}

struct MockDriverState {
    polls: VecDeque<MockPoll>,
    waits: VecDeque<Result<(), MockIoError>>,
    notifications: VecDeque<Result<(), MockIoError>>,
    notification_failure: Option<MockIoError>,
    bind_panics: bool,
    driver_drop_action: Option<SendTask>,
    notify_wakes: bool,
    pending_wakes: u64,
    pending_timers: u64,
    waiting: usize,
    now: Duration,
    timer_rearms: Vec<Option<Duration>>,
}

impl Default for MockDriverState {
    fn default() -> Self {
        Self {
            polls: VecDeque::new(),
            waits: VecDeque::new(),
            notifications: VecDeque::new(),
            notification_failure: None,
            bind_panics: false,
            driver_drop_action: None,
            notify_wakes: true,
            pending_wakes: 0,
            pending_timers: 0,
            waiting: 0,
            now: Duration::ZERO,
            timer_rearms: Vec::new(),
        }
    }
}

struct MockDriverInner {
    id: usize,
    factory: Weak<MockRuntimeFactory>,
    trace: EventTrace<MockRuntimeEvent>,
    state: Mutex<MockDriverState>,
    changed: Condvar,
}

/// Controller for one mock driver/notifier pair.
#[derive(Clone)]
pub(crate) struct MockDriverControl {
    inner: Arc<MockDriverInner>,
}

impl MockDriverControl {
    fn new(
        id: usize,
        factory: Weak<MockRuntimeFactory>,
        trace: EventTrace<MockRuntimeEvent>,
    ) -> Self {
        Self {
            inner: Arc::new(MockDriverInner {
                id,
                factory,
                trace,
                state: Mutex::new(MockDriverState::default()),
                changed: Condvar::new(),
            }),
        }
    }

    pub(crate) fn id(&self) -> usize {
        self.inner.id
    }

    pub(crate) fn set_time(&self, now: Duration) {
        self.inner.state.lock().expect("mock driver poisoned").now = now;
    }

    pub(crate) fn advance_time(&self, amount: Duration) {
        let mut state = self.inner.state.lock().expect("mock driver poisoned");
        state.now = state.now.saturating_add(amount);
    }

    pub(crate) fn queue_ready(&self, events: ReadyEvents) {
        self.queue_mock_ready(MockReady {
            events,
            wakes: u64::from(events.wake),
            timers: u64::from(events.timer),
            completion: None,
        });
    }

    pub(crate) fn wake_runtime(&self, count: u64) {
        assert!(count > 0, "wake count must be non-zero");
        self.queue_mock_ready(MockReady {
            events: ReadyEvents {
                wake: true,
                ..ReadyEvents::default()
            },
            wakes: count,
            timers: 0,
            completion: None,
        });
    }

    pub(crate) fn fire_timer(&self, count: u64) {
        assert!(count > 0, "timer count must be non-zero");
        self.queue_mock_ready(MockReady {
            events: ReadyEvents {
                timer: true,
                ..ReadyEvents::default()
            },
            wakes: 0,
            timers: count,
            completion: None,
        });
    }

    pub(crate) fn queue_completion(
        &self,
        label: impl Into<String>,
        task: impl FnOnce() + Send + 'static,
    ) {
        self.queue_mock_ready(MockReady {
            // A queued completion is exactly what `ReadyEvents::io` names: the
            // driver dispatched an I/O completion this poll.
            events: ReadyEvents {
                io: true,
                ..ReadyEvents::default()
            },
            wakes: 0,
            timers: 0,
            completion: Some(MockCompletion {
                label: label.into(),
                task: Some(Box::new(task)),
            }),
        });
    }

    pub(crate) fn fail_next_poll(&self, kind: io::ErrorKind, message: impl Into<Arc<str>>) {
        let mut state = self.inner.state.lock().expect("mock driver poisoned");
        state
            .polls
            .push_back(MockPoll::Error(MockIoError::new(kind, message)));
        self.inner.changed.notify_all();
    }

    pub(crate) fn release_next_wait(&self) {
        self.queue_wait_result(Ok(()));
    }

    pub(crate) fn fail_next_wait(&self, kind: io::ErrorKind, message: impl Into<Arc<str>>) {
        self.queue_wait_result(Err(MockIoError::new(kind, message)));
    }

    pub(crate) fn fail_next_notify(&self, kind: io::ErrorKind, message: impl Into<Arc<str>>) {
        self.inner
            .state
            .lock()
            .expect("mock driver poisoned")
            .notifications
            .push_back(Err(MockIoError::new(kind, message)));
    }

    pub(crate) fn fail_notifications(&self, kind: io::ErrorKind, message: impl Into<Arc<str>>) {
        self.inner
            .state
            .lock()
            .expect("mock driver poisoned")
            .notification_failure = Some(MockIoError::new(kind, message));
    }

    pub(crate) fn allow_notifications(&self) {
        self.inner
            .state
            .lock()
            .expect("mock driver poisoned")
            .notification_failure = None;
    }

    pub(crate) fn panic_on_bind(&self) {
        self.inner
            .state
            .lock()
            .expect("mock driver poisoned")
            .bind_panics = true;
    }

    pub(crate) fn on_driver_drop(&self, action: impl FnOnce() + Send + 'static) {
        self.inner
            .state
            .lock()
            .expect("mock driver poisoned")
            .driver_drop_action = Some(Box::new(action));
    }

    pub(crate) fn succeed_next_notify(&self) {
        self.inner
            .state
            .lock()
            .expect("mock driver poisoned")
            .notifications
            .push_back(Ok(()));
    }

    pub(crate) fn set_notify_wakes(&self, enabled: bool) {
        self.inner
            .state
            .lock()
            .expect("mock driver poisoned")
            .notify_wakes = enabled;
    }

    pub(crate) fn wait_until_waiting(&self, timeout: Duration) -> bool {
        let state = self.inner.state.lock().expect("mock driver poisoned");
        let (state, _) = self
            .inner
            .changed
            .wait_timeout_while(state, timeout, |state| state.waiting == 0)
            .expect("mock driver poisoned");
        state.waiting > 0
    }

    pub(crate) fn cancel_wait_on_drop(&self) -> MockWaitCancellation {
        MockWaitCancellation {
            control: Some(self.clone()),
        }
    }

    pub(crate) fn timer_rearms(&self) -> Vec<Option<Duration>> {
        self.inner
            .state
            .lock()
            .expect("mock driver poisoned")
            .timer_rearms
            .clone()
    }

    fn worker_shutdown_requested(&self) -> bool {
        self.inner
            .factory
            .upgrade()
            .is_none_or(|factory| factory.worker_shutdown.load(Ordering::Acquire))
    }

    fn wake_for_worker_shutdown(&self) {
        let _state = self
            .inner
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        self.inner.changed.notify_all();
    }

    fn queue_mock_ready(&self, ready: MockReady) {
        self.inner
            .state
            .lock()
            .expect("mock driver poisoned")
            .polls
            .push_back(MockPoll::Ready(ready));
        self.inner.changed.notify_all();
    }

    fn queue_wait_result(&self, result: Result<(), MockIoError>) {
        self.inner
            .state
            .lock()
            .expect("mock driver poisoned")
            .waits
            .push_back(result);
        self.inner.changed.notify_all();
    }

    fn now(&self) -> Duration {
        self.inner.state.lock().expect("mock driver poisoned").now
    }
}

/// Forces a mock driver wait to fail if its controller exits early.
#[must_use = "the guard must remain armed until the controller wakes the driver"]
pub(crate) struct MockWaitCancellation {
    control: Option<MockDriverControl>,
}

impl MockWaitCancellation {
    pub(crate) fn disarm(mut self) {
        self.control.take();
    }
}

impl Drop for MockWaitCancellation {
    fn drop(&mut self) {
        if let Some(control) = self.control.take() {
            control.fail_next_wait(
                io::ErrorKind::Interrupted,
                "mock controller exited before waking the driver",
            );
        }
    }
}

struct MockDriver {
    control: MockDriverControl,
}

impl DriverBackend for MockDriver {
    fn poll(&self) -> io::Result<Option<ReadyEvents>> {
        self.control
            .inner
            .trace
            .record(MockRuntimeEvent::DriverPolled(self.control.id()));
        let outcome = self
            .control
            .inner
            .state
            .lock()
            .expect("mock driver poisoned")
            .polls
            .pop_front();

        match outcome {
            None => Ok(None),
            Some(MockPoll::Error(error)) => Err(error.to_io_error()),
            Some(MockPoll::Ready(mut ready)) => {
                {
                    let mut state = self
                        .control
                        .inner
                        .state
                        .lock()
                        .expect("mock driver poisoned");
                    state.pending_wakes = state.pending_wakes.saturating_add(ready.wakes);
                    state.pending_timers = state.pending_timers.saturating_add(ready.timers);
                }
                if let Some(mut completion) = ready.completion.take() {
                    self.control
                        .inner
                        .trace
                        .record(MockRuntimeEvent::CompletionDispatched(
                            self.control.id(),
                            completion.label,
                        ));
                    completion
                        .task
                        .take()
                        .expect("mock completion should contain a task")();
                }
                self.control
                    .inner
                    .trace
                    .record(MockRuntimeEvent::DriverReady(
                        self.control.id(),
                        ready.events,
                    ));
                Ok(Some(ready.events))
            }
        }
    }

    fn wait(&self) -> io::Result<()> {
        let mut state = self
            .control
            .inner
            .state
            .lock()
            .expect("mock driver poisoned");
        state.waiting += 1;
        self.control
            .inner
            .trace
            .record(MockRuntimeEvent::DriverWaitStarted(self.control.id()));
        self.control.inner.changed.notify_all();

        while state.polls.is_empty()
            && state.waits.is_empty()
            && !self.control.worker_shutdown_requested()
        {
            state = self
                .control
                .inner
                .changed
                .wait(state)
                .expect("mock driver poisoned");
        }

        state.waiting -= 1;
        let shutting_down = self.control.worker_shutdown_requested();
        let result = if shutting_down {
            Err(io::Error::new(
                io::ErrorKind::Interrupted,
                "mock runtime harness is shutting down",
            ))
        } else {
            state
                .waits
                .pop_front()
                .unwrap_or(Ok(()))
                .map_err(|error| error.to_io_error())
        };
        drop(state);
        self.control
            .inner
            .trace
            .record(MockRuntimeEvent::DriverWaitFinished(self.control.id()));
        result
    }

    fn rearm_timer(&self, deadline: Option<Duration>) -> io::Result<()> {
        self.control
            .inner
            .state
            .lock()
            .expect("mock driver poisoned")
            .timer_rearms
            .push(deadline);
        self.control
            .inner
            .trace
            .record(MockRuntimeEvent::TimerRearmed(self.control.id(), deadline));
        Ok(())
    }

    fn drain_wake(&self) -> Option<u64> {
        let count = {
            let mut state = self
                .control
                .inner
                .state
                .lock()
                .expect("mock driver poisoned");
            std::mem::take(&mut state.pending_wakes)
        };
        if count == 0 {
            None
        } else {
            self.control
                .inner
                .trace
                .record(MockRuntimeEvent::WakeDrained(self.control.id(), count));
            Some(count)
        }
    }

    fn drain_timer(&self) -> Option<u64> {
        let count = {
            let mut state = self
                .control
                .inner
                .state
                .lock()
                .expect("mock driver poisoned");
            std::mem::take(&mut state.pending_timers)
        };
        if count == 0 {
            None
        } else {
            self.control
                .inner
                .trace
                .record(MockRuntimeEvent::TimerDrained(self.control.id(), count));
            Some(count)
        }
    }

    fn bind_current_thread(&self) {
        let bind_panics = self
            .control
            .inner
            .state
            .lock()
            .expect("mock driver poisoned")
            .bind_panics;
        if bind_panics {
            self.control
                .inner
                .trace
                .record(MockRuntimeEvent::DriverBindPanicked(self.control.id()));
            panic!("scripted mock driver bind panic");
        }

        let factory = self
            .control
            .inner
            .factory
            .upgrade()
            .expect("mock runtime harness dropped before driver bind");
        BOUND_MOCK_DRIVERS.with(|drivers| {
            drivers.borrow_mut().push(BoundMockDriver {
                factory,
                control: self.control.clone(),
            });
        });
        self.control
            .inner
            .trace
            .record(MockRuntimeEvent::DriverBound(self.control.id()));
    }

    fn unbind_current_thread(&self) {
        BOUND_MOCK_DRIVERS.with(|drivers| {
            let bound = drivers
                .borrow_mut()
                .pop()
                .expect("mock driver should be bound before unbind");
            assert!(
                Arc::ptr_eq(&bound.control.inner, &self.control.inner),
                "mock drivers must be unbound in stack order"
            );
        });
        self.control
            .inner
            .trace
            .record(MockRuntimeEvent::DriverUnbound(self.control.id()));
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

impl Drop for MockDriver {
    fn drop(&mut self) {
        self.control
            .inner
            .trace
            .record(MockRuntimeEvent::DriverDropped(self.control.id()));
        let action = self
            .control
            .inner
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .driver_drop_action
            .take();
        if let Some(action) = action {
            action();
        }
    }
}

struct MockNotifier {
    control: MockDriverControl,
}

impl Notifier for MockNotifier {
    fn notify(&self) -> io::Result<()> {
        self.control
            .inner
            .trace
            .record(MockRuntimeEvent::NotifyAttempted(self.control.id()));
        let (result, notify_wakes) = {
            let mut state = self
                .control
                .inner
                .state
                .lock()
                .expect("mock driver poisoned");
            (
                state
                    .notifications
                    .pop_front()
                    .unwrap_or_else(|| state.notification_failure.clone().map_or(Ok(()), Err)),
                state.notify_wakes,
            )
        };

        match result {
            Ok(()) => {
                self.control
                    .inner
                    .trace
                    .record(MockRuntimeEvent::NotifySucceeded(self.control.id()));
                if notify_wakes {
                    self.control.wake_runtime(1);
                }
                Ok(())
            }
            Err(error) => {
                self.control
                    .inner
                    .trace
                    .record(MockRuntimeEvent::NotifyFailed(
                        self.control.id(),
                        error.kind,
                    ));
                Err(error.to_io_error())
            }
        }
    }
}

impl Drop for MockNotifier {
    fn drop(&mut self) {
        self.control
            .inner
            .trace
            .record(MockRuntimeEvent::NotifierDropped(self.control.id()));
    }
}

enum ThreadSpawnAction {
    Run,
    Fail(MockIoError),
    Wait(ExecutionGate),
}

struct MockRuntimeFactory {
    planned_drivers: Mutex<VecDeque<MockDriverControl>>,
    created_drivers: Mutex<Vec<MockDriverControl>>,
    next_driver_id: AtomicUsize,
    thread_spawns: Mutex<VecDeque<ThreadSpawnAction>>,
    worker_start_gates: Mutex<Vec<ExecutionGate>>,
    worker_reapers: Mutex<Vec<thread::JoinHandle<()>>>,
    worker_shutdown: AtomicBool,
    trace: EventTrace<MockRuntimeEvent>,
}

impl MockRuntimeFactory {
    fn make_driver(self: &Arc<Self>) -> MockDriverControl {
        let id = self.next_driver_id.fetch_add(1, Ordering::Relaxed);
        MockDriverControl::new(id, Arc::downgrade(self), self.trace.clone())
    }

    fn take_driver(self: &Arc<Self>) -> MockDriverControl {
        let control = self
            .planned_drivers
            .lock()
            .expect("mock runtime factory poisoned")
            .pop_front()
            .unwrap_or_else(|| self.make_driver());
        self.created_drivers
            .lock()
            .expect("mock runtime factory poisoned")
            .push(control.clone());
        self.trace
            .record(MockRuntimeEvent::DriverCreated(control.id()));
        control
    }

    fn begin_worker_scope(&self) {
        let worker_reapers = self
            .worker_reapers
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        assert!(
            worker_reapers.is_empty(),
            "mock worker reapers must be joined before re-entering the harness"
        );
        assert!(
            self.worker_start_gates
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .is_empty(),
            "mock worker gates must be released before re-entering the harness"
        );
        self.worker_shutdown.store(false, Ordering::Release);
    }

    fn finish_worker_scope(&self) -> Option<Box<dyn Any + Send + 'static>> {
        let mut reaper_registry = self
            .worker_reapers
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let mut gate_registry = self
            .worker_start_gates
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if reaper_registry.is_empty() && gate_registry.is_empty() {
            return None;
        }
        self.worker_shutdown.store(true, Ordering::Release);
        let worker_reapers = std::mem::take(&mut *reaper_registry);
        let worker_start_gates = std::mem::take(&mut *gate_registry);
        drop(reaper_registry);
        drop(gate_registry);

        for gate in worker_start_gates {
            gate.release();
        }

        if !worker_reapers.is_empty() {
            let controls = self
                .created_drivers
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .clone();
            for control in controls {
                control.wake_for_worker_shutdown();
            }
        }
        let mut first_panic = None;
        for reaper in worker_reapers {
            if let Err(payload) = reaper.join()
                && first_panic.is_none()
            {
                first_panic = Some(payload);
            }
        }
        first_panic
    }

    fn spawn_thread(&self, task: SendTask) -> io::Result<thread::JoinHandle<()>> {
        self.trace.record(MockRuntimeEvent::ThreadSpawnAttempted);
        let action = self
            .thread_spawns
            .lock()
            .expect("mock runtime factory poisoned")
            .pop_front()
            .unwrap_or(ThreadSpawnAction::Run);
        if let ThreadSpawnAction::Fail(error) = action {
            self.trace
                .record(MockRuntimeEvent::ThreadSpawnFailed(error.kind));
            return Err(error.to_io_error());
        }

        let start_gate = match action {
            ThreadSpawnAction::Wait(gate) => Some(gate),
            ThreadSpawnAction::Run => None,
            ThreadSpawnAction::Fail(_) => unreachable!(),
        };
        if self.worker_shutdown.load(Ordering::Acquire) {
            let error = io::Error::new(
                io::ErrorKind::Interrupted,
                "mock runtime harness is shutting down",
            );
            self.trace
                .record(MockRuntimeEvent::ThreadSpawnFailed(error.kind()));
            return Err(error);
        }

        if let Some(gate) = &start_gate {
            self.worker_start_gates
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .push(gate.clone());
        }

        let trace = self.trace.clone();
        let worker_gate = start_gate.clone();
        thread::Builder::new()
            .name("runite-worker".into())
            .spawn(move || {
                trace.record(MockRuntimeEvent::ThreadStarted);
                if let Some(gate) = worker_gate {
                    gate.arrive_and_wait();
                }
                struct Finished(EventTrace<MockRuntimeEvent>);
                impl Drop for Finished {
                    fn drop(&mut self) {
                        self.0.record(MockRuntimeEvent::ThreadFinished);
                    }
                }
                let _finished = Finished(trace);
                task();
            })
    }

    fn spawn_reaper(&self, task: SendTask) -> io::Result<()> {
        if self.worker_shutdown.load(Ordering::Acquire) {
            return Err(io::Error::new(
                io::ErrorKind::Interrupted,
                "mock runtime harness is shutting down",
            ));
        }
        let trace = self.trace.clone();
        let reaper = thread::Builder::new()
            .name("runite-worker-reaper".into())
            .spawn(move || {
                task();
                trace.record(MockRuntimeEvent::ThreadJoined);
            })?;
        self.worker_reapers
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .push(reaper);
        Ok(())
    }
}

struct BoundMockDriver {
    factory: Arc<MockRuntimeFactory>,
    control: MockDriverControl,
}

thread_local! {
    static CONFIGURED_MOCK_RUNTIMES: RefCell<Vec<Arc<MockRuntimeFactory>>> =
        const { RefCell::new(Vec::new()) };
    static BOUND_MOCK_DRIVERS: RefCell<Vec<BoundMockDriver>> =
        const { RefCell::new(Vec::new()) };
}

fn active_mock_factory() -> io::Result<Arc<MockRuntimeFactory>> {
    if let Some(factory) =
        CONFIGURED_MOCK_RUNTIMES.with(|runtimes| runtimes.borrow().last().cloned())
    {
        return Ok(factory);
    }
    BOUND_MOCK_DRIVERS
        .with(|drivers| {
            drivers
                .borrow()
                .last()
                .map(|bound| Arc::clone(&bound.factory))
        })
        .ok_or_else(|| io::Error::other("MockRuntime used outside MockRuntimeHarness::enter"))
}

struct MockRuntimeScope {
    factory: Arc<MockRuntimeFactory>,
    _not_send: PhantomData<Rc<()>>,
}

impl MockRuntimeScope {
    fn enter(factory: Arc<MockRuntimeFactory>) -> Self {
        CONFIGURED_MOCK_RUNTIMES.with(|runtimes| {
            runtimes.borrow_mut().push(Arc::clone(&factory));
        });
        Self {
            factory,
            _not_send: PhantomData,
        }
    }
}

impl Drop for MockRuntimeScope {
    fn drop(&mut self) {
        CONFIGURED_MOCK_RUNTIMES.with(|runtimes| {
            let configured = runtimes
                .borrow_mut()
                .pop()
                .expect("mock runtime scope stack should not be empty");
            assert!(
                Arc::ptr_eq(&configured, &self.factory),
                "mock runtime scopes must be dropped in stack order"
            );
        });
    }
}

struct MockRuntimeCleanup {
    factory: Weak<MockRuntimeFactory>,
}

impl Drop for MockRuntimeCleanup {
    fn drop(&mut self) {
        let Some(factory) = self.factory.upgrade() else {
            return;
        };
        let worker_panic = factory.finish_worker_scope();
        let owns_installed = try_with_installed_thread(|state| {
            state.is_some_and(|state| {
                state
                    .driver
                    .as_any()
                    .downcast_ref::<MockDriver>()
                    .and_then(|driver| driver.control.inner.factory.upgrade())
                    .is_some_and(|installed| Arc::ptr_eq(&installed, &factory))
            })
        });
        if owns_installed {
            teardown_thread();
        }
        if let Some(payload) = worker_panic
            && !thread::panicking()
        {
            resume_unwind(payload);
        }
    }
}

/// Per-test factory for [`MockRuntime`].
pub(crate) struct MockRuntimeHarness {
    factory: Arc<MockRuntimeFactory>,
}

impl MockRuntimeHarness {
    pub(crate) fn new() -> Self {
        Self {
            factory: Arc::new(MockRuntimeFactory {
                planned_drivers: Mutex::new(VecDeque::new()),
                created_drivers: Mutex::new(Vec::new()),
                next_driver_id: AtomicUsize::new(0),
                thread_spawns: Mutex::new(VecDeque::new()),
                worker_start_gates: Mutex::new(Vec::new()),
                worker_reapers: Mutex::new(Vec::new()),
                worker_shutdown: AtomicBool::new(false),
                trace: EventTrace::new(),
            }),
        }
    }

    pub(crate) fn plan_driver(&self) -> MockDriverControl {
        let control = self.factory.make_driver();
        self.factory
            .planned_drivers
            .lock()
            .expect("mock runtime factory poisoned")
            .push_back(control.clone());
        control
    }

    pub(crate) fn created_driver(&self, index: usize) -> Option<MockDriverControl> {
        self.factory
            .created_drivers
            .lock()
            .expect("mock runtime factory poisoned")
            .get(index)
            .cloned()
    }

    pub(crate) fn trace(&self) -> EventTrace<MockRuntimeEvent> {
        self.factory.trace.clone()
    }

    pub(crate) fn fail_next_thread_spawn(&self, kind: io::ErrorKind, message: impl Into<Arc<str>>) {
        self.factory
            .thread_spawns
            .lock()
            .expect("mock runtime factory poisoned")
            .push_back(ThreadSpawnAction::Fail(MockIoError::new(kind, message)));
    }

    pub(crate) fn gate_next_thread_spawn(&self, gate: ExecutionGate) {
        self.factory
            .thread_spawns
            .lock()
            .expect("mock runtime factory poisoned")
            .push_back(ThreadSpawnAction::Wait(gate));
    }

    pub(crate) fn enter<T>(&self, test: impl FnOnce() -> T) -> T {
        self.factory.begin_worker_scope();
        let _cleanup = MockRuntimeCleanup {
            factory: Arc::downgrade(&self.factory),
        };
        let _scope = MockRuntimeScope::enter(Arc::clone(&self.factory));
        test()
    }
}

/// Marker runtime backed by [`MockDriverControl`].
pub(crate) struct MockRuntime;

impl Runtime for MockRuntime {
    fn create_driver_pair(
        _config: RuntimeConfig,
    ) -> io::Result<(Box<dyn DriverBackend>, Box<dyn Notifier>)> {
        let control = active_mock_factory()?.take_driver();
        Ok((
            Box::new(MockDriver {
                control: control.clone(),
            }),
            Box::new(MockNotifier { control }),
        ))
    }

    fn monotonic_now() -> io::Result<Duration> {
        BOUND_MOCK_DRIVERS
            .with(|drivers| drivers.borrow().last().map(|bound| bound.control.now()))
            .ok_or_else(|| io::Error::other("mock clock read before driver bind"))
    }

    fn spawn_worker_thread(task: SendTask) -> io::Result<thread::JoinHandle<()>> {
        active_mock_factory()?.spawn_thread(task)
    }

    fn spawn_worker_reaper(task: SendTask) -> io::Result<()> {
        active_mock_factory()?.spawn_reaper(task)
    }
}

#[cfg(unix)]
pub(crate) fn reuse_fd_number(target: std::os::fd::OwnedFd) -> io::Result<std::os::fd::OwnedFd> {
    use std::fs::File;
    use std::os::fd::{AsRawFd, FromRawFd, IntoRawFd};

    let source = File::open("/dev/null")?;
    let target = target.into_raw_fd();

    // SAFETY: `source` is open and `target` is the deliberately selected fd
    // number, which remains reserved by the consumed `OwnedFd` until `dup2`
    // atomically replaces it. A successful call creates the descriptor owned
    // by the return value.
    let reused = unsafe { libc::dup2(source.as_raw_fd(), target) };
    if reused < 0 {
        let error = io::Error::last_os_error();
        // SAFETY: `dup2` failed, so the consumed target descriptor is still
        // open and must be reclaimed before returning.
        drop(unsafe { std::os::fd::OwnedFd::from_raw_fd(target) });
        return Err(error);
    }
    // SAFETY: `reused` was just created by `dup2` and has no Rust owner.
    let reused = unsafe { std::os::fd::OwnedFd::from_raw_fd(reused) };
    let flags = unsafe { libc::fcntl(reused.as_raw_fd(), libc::F_GETFD) };
    if flags < 0
        || unsafe { libc::fcntl(reused.as_raw_fd(), libc::F_SETFD, flags | libc::FD_CLOEXEC) } < 0
    {
        return Err(io::Error::last_os_error());
    }
    Ok(reused)
}

pub fn runtime_executes_local_and_remote_work<R: Runtime>() {
    let log = Arc::new(Mutex::new(Vec::<String>::new()));
    let main_handle = current_thread_handle::<R>();

    {
        let log = Arc::clone(&log);
        queue_task::<R, _>(move || log.lock().unwrap().push("main task".into()));
    }
    {
        let log = Arc::clone(&log);
        queue_microtask::<R, _>(move || log.lock().unwrap().push("main microtask".into()));
    }
    {
        let log = Arc::clone(&log);
        queue_future::<R, _>(async move {
            log.lock().unwrap().push("main future start".into());
            yield_now().await;
            log.lock().unwrap().push("main future end".into());
        });
    }
    {
        let log = Arc::clone(&log);
        timeout::<R, _>(Duration::from_millis(5), move || {
            log.lock().unwrap().push("main timeout".into());
        });
    }
    {
        let log = Arc::clone(&log);
        let handle_slot: Rc<RefCell<Option<IntervalHandle>>> = Rc::new(RefCell::new(None));
        let handle_slot_clone = Rc::clone(&handle_slot);
        let tick_count = Rc::new(Cell::new(0usize));
        let tick_count_clone = Rc::clone(&tick_count);
        let interval_handle = interval::<R, _>(Duration::from_millis(3), move || {
            let next = tick_count_clone.get() + 1;
            tick_count_clone.set(next);
            log.lock().unwrap().push(format!("main interval {next}"));
            if next == 2 {
                let handle = handle_slot_clone.borrow_mut().take().unwrap();
                handle.cancel();
            }
        });
        *handle_slot.borrow_mut() = Some(interval_handle);
    }

    {
        let worker_log = Arc::clone(&log);
        let exit_log = Arc::clone(&log);
        let main_handle_for_worker = main_handle.clone();
        spawn_worker::<R, _, _>(
            move || {
                let log = Arc::clone(&worker_log);
                queue_task::<R, _>({
                    let log = Arc::clone(&log);
                    move || log.lock().unwrap().push("worker task".into())
                });
                queue_microtask::<R, _>({
                    let log = Arc::clone(&log);
                    move || log.lock().unwrap().push("worker microtask".into())
                });
                queue_future::<R, _>({
                    let log = Arc::clone(&log);
                    async move {
                        log.lock().unwrap().push("worker future start".into());
                        yield_now().await;
                        log.lock().unwrap().push("worker future end".into());
                    }
                });
                timeout::<R, _>(Duration::from_millis(7), move || {
                    let _ = main_handle_for_worker.queue_macrotask({
                        let log = Arc::clone(&log);
                        move || log.lock().unwrap().push("worker timeout to main".into())
                    });
                });
            },
            {
                let log = Arc::clone(&exit_log);
                move || log.lock().unwrap().push("worker exit".into())
            },
        );
    }

    run::<R>();

    let log = log.lock().unwrap();
    assert!(log.iter().any(|entry| entry == "main task"));
    assert!(log.iter().any(|entry| entry == "main microtask"));
    assert!(log.iter().any(|entry| entry == "main future start"));
    assert!(log.iter().any(|entry| entry == "main future end"));
    assert!(log.iter().any(|entry| entry == "main timeout"));
    assert!(log.iter().any(|entry| entry == "main interval 1"));
    assert!(log.iter().any(|entry| entry == "main interval 2"));
    assert!(log.iter().any(|entry| entry == "worker task"));
    assert!(log.iter().any(|entry| entry == "worker microtask"));
    assert!(log.iter().any(|entry| entry == "worker future start"));
    assert!(log.iter().any(|entry| entry == "worker future end"));
    assert!(log.iter().any(|entry| entry == "worker timeout to main"));
    assert!(log.iter().any(|entry| entry == "worker exit"));
}

pub fn runtime_waits_for_cross_thread_operation_completion<R: Runtime>() {
    let observed = Arc::new(Mutex::new(None::<usize>));
    let completion_thread = Arc::new(Mutex::new(None::<TrackedThread<()>>));

    {
        let observed = Arc::clone(&observed);
        let completion_thread = Arc::clone(&completion_thread);
        queue_task::<R, _>(move || {
            let (completion, source) = completion_for_current_thread::<usize>();

            let thread = TrackedThread::new(thread::spawn(move || {
                source.complete(7);
            }));
            *completion_thread.lock().unwrap() = Some(thread);

            queue_future::<R, _>(async move {
                let value = completion.await;
                *observed.lock().unwrap() = Some(value);
            });
        });
    }

    run::<R>();

    completion_thread
        .lock()
        .unwrap()
        .take()
        .expect("completion thread should be tracked")
        .join()
        .expect("completion thread should finish");
    assert_eq!(*observed.lock().unwrap(), Some(7));
}

pub fn zero_interval_fires_once_per_turn_without_spinning<R: Runtime>() {
    // interval(Duration::ZERO, ..) must not busy-spin the event loop.
    // Each tick is one macrotask turn.
    let count = Rc::new(Cell::new(0usize));
    let count_clone = Rc::clone(&count);
    let handle_slot: Rc<RefCell<Option<IntervalHandle>>> = Rc::new(RefCell::new(None));
    let handle_slot_clone = Rc::clone(&handle_slot);

    let handle = interval::<R, _>(Duration::ZERO, move || {
        let next = count_clone.get() + 1;
        count_clone.set(next);
        if next == 5 {
            let handle = handle_slot_clone.borrow_mut().take().unwrap();
            handle.cancel();
        }
    });
    *handle_slot.borrow_mut() = Some(handle);

    run::<R>();

    assert_eq!(count.get(), 5);
}

/// Answers `enabled` and nothing else, so a workload can be run with the turn
/// record either wanted or declined.
///
/// `register_callsite` is deliberately left at its default (`sometimes`): an
/// `always`/`never` answer would be cached per callsite and outlive the scoped
/// dispatcher, so the second workload would inherit the first one's interest.
struct TurnInterest {
    collect: bool,
}

impl tracing::Subscriber for TurnInterest {
    fn enabled(&self, metadata: &tracing::Metadata<'_>) -> bool {
        if self.collect {
            *metadata.level() <= tracing::Level::TRACE
        } else {
            *metadata.level() <= tracing::Level::WARN
        }
    }

    fn new_span(&self, _: &tracing::span::Attributes<'_>) -> tracing::span::Id {
        tracing::span::Id::from_u64(1)
    }

    fn record(&self, _: &tracing::span::Id, _: &tracing::span::Record<'_>) {}

    fn record_follows_from(&self, _: &tracing::span::Id, _: &tracing::span::Id) {}

    fn event(&self, _: &tracing::Event<'_>) {}

    fn enter(&self, _: &tracing::span::Id) {}

    fn exit(&self, _: &tracing::span::Id) {}
}

/// Runs a loop that parks in the driver and returns how many turn-record
/// samples it took.
fn turn_samples_for_parking_loop<R: Runtime>(collect: bool) -> u64 {
    tracing::subscriber::with_default(TurnInterest { collect }, || {
        super::scheduler::reset_turn_samples();
        // A pending timer is what makes `run` park rather than return idle, so
        // this exercises the park-timing branch as well as the two per-turn
        // samples.
        timeout::<R, _>(Duration::from_millis(5), || {});
        run::<R>();
        super::scheduler::turn_samples_taken()
    })
}

/// With nothing collecting, the turn machinery must sample no queue depth,
/// take no lock on the cross-thread queue, and not time the driver park.
///
/// This is the one property that has to be pinned rather than asserted in
/// prose: the record runs on every iteration of every runite loop, and the
/// cross-thread queue depth is read under the very mutex `enqueue_macro`
/// contends on. Checking that no event was emitted would prove nothing —
/// `tracing` filters the event on its own with the gate deleted entirely — so
/// this counts the work instead.
pub fn dormant_turn_records_cost_nothing<R: Runtime>() {
    // Positive control first: the same workload must reach the sample sites
    // when something *is* collecting, so the zero below means "gated off"
    // rather than "unreachable".
    let collected = turn_samples_for_parking_loop::<R>(true);
    assert!(
        collected >= 3,
        "a collecting loop samples twice per turn and once per park, saw {collected}"
    );

    let dormant = turn_samples_for_parking_loop::<R>(false);
    assert_eq!(
        dormant, 0,
        "a loop with no turn-record collector must take no samples at all"
    );
}

#[cfg(test)]
#[path = "test_support/tests.rs"]
mod tests;
