use core::fmt;
use core::future::Future;
use core::pin::Pin;
use core::sync::atomic::{AtomicU64, Ordering};
use core::task::{Context, Poll, Waker};
use std::collections::{HashMap, VecDeque};
use std::io;
use std::sync::{Arc, Mutex, OnceLock};
use std::task::Wake;

pub(crate) type IoFuture<T> = Pin<Box<dyn Future<Output = io::Result<T>> + 'static>>;

static NEXT_OPERATION_ID: AtomicU64 = AtomicU64::new(1);
const TRACKED_WRITE_BIT: u64 = 1 << 63;

pub(crate) fn next_operation_id() -> u64 {
    loop {
        let id = NEXT_OPERATION_ID.fetch_add(1, Ordering::Relaxed) & !TRACKED_WRITE_BIT;
        if id != 0 {
            return id;
        }
    }
}

struct PendingOperation<T> {
    future: IoFuture<T>,
    waiters: Arc<WaiterSet>,
    broadcast: Waker,
}

#[derive(Default)]
struct WaiterSet {
    waiters: Mutex<Vec<Waker>>,
}

impl WaiterSet {
    fn register(&self, waker: &Waker) {
        let mut waiters = self.waiters.lock().expect("I/O waiter set poisoned");
        if !waiters.iter().any(|waiter| waiter.will_wake(waker)) {
            waiters.push(waker.clone());
        }
    }

    fn wake_all(&self) {
        let waiters = std::mem::take(&mut *self.waiters.lock().expect("I/O waiter set poisoned"));
        for waiter in waiters {
            waiter.wake();
        }
    }
}

static LIVE_WRITE_OPERATIONS: OnceLock<Mutex<HashMap<u64, Arc<WaiterSet>>>> = OnceLock::new();

fn live_write_operations() -> &'static Mutex<HashMap<u64, Arc<WaiterSet>>> {
    LIVE_WRITE_OPERATIONS.get_or_init(|| Mutex::new(HashMap::new()))
}

/// Liveness token for a cancellation-capable logical write.
///
/// The high bit distinguishes tracked extension/adapter operations from raw
/// poll callers, whose contract requires polling the same operation to
/// completion. Dropping this token publishes cancellation and wakes any shared
/// cursor operation waiting behind it.
pub(crate) struct WriteOperation {
    generation: u64,
}

impl WriteOperation {
    pub(crate) fn new() -> Self {
        let generation = next_operation_id() | TRACKED_WRITE_BIT;
        live_write_operations()
            .lock()
            .expect("live write operation registry poisoned")
            .insert(generation, Arc::new(WaiterSet::default()));
        Self { generation }
    }

    pub(crate) fn generation(&self) -> u64 {
        self.generation
    }
}

impl Drop for WriteOperation {
    fn drop(&mut self) {
        let waiters = live_write_operations()
            .lock()
            .expect("live write operation registry poisoned")
            .remove(&self.generation);
        if let Some(waiters) = waiters {
            waiters.wake_all();
        }
    }
}

fn is_tracked_write(generation: u64) -> bool {
    generation & TRACKED_WRITE_BIT != 0
}

fn is_live_write(generation: u64) -> bool {
    !is_tracked_write(generation)
        || live_write_operations()
            .lock()
            .expect("live write operation registry poisoned")
            .contains_key(&generation)
}

fn register_write_cancellation_waker(generation: u64, waker: &Waker) {
    if !is_tracked_write(generation) {
        return;
    }
    let waiters = live_write_operations()
        .lock()
        .expect("live write operation registry poisoned")
        .get(&generation)
        .cloned();
    if let Some(waiters) = waiters {
        waiters.register(waker);
    }
}

struct BroadcastWake(Arc<WaiterSet>);

impl Wake for BroadcastWake {
    fn wake(self: Arc<Self>) {
        self.0.wake_all();
    }

    fn wake_by_ref(self: &Arc<Self>) {
        self.0.wake_all();
    }
}

impl<T> PendingOperation<T> {
    fn new(future: IoFuture<T>) -> Self {
        let waiters = Arc::new(WaiterSet::default());
        let broadcast = Waker::from(Arc::new(BroadcastWake(Arc::clone(&waiters))));
        Self {
            future,
            waiters,
            broadcast,
        }
    }

    fn poll(&mut self, cx: &mut Context<'_>) -> Poll<io::Result<T>> {
        self.waiters.register(cx.waker());
        let mut broadcast_cx = Context::from_waker(&self.broadcast);

        match self.future.as_mut().poll(&mut broadcast_cx) {
            Poll::Ready(result) => {
                self.waiters.wake_all();
                Poll::Ready(result)
            }
            Poll::Pending => Poll::Pending,
        }
    }
}

#[derive(Clone)]
struct SharedIoError {
    kind: io::ErrorKind,
    raw_os_error: Option<i32>,
    message: String,
}

impl SharedIoError {
    fn new(error: io::Error) -> Self {
        Self {
            kind: error.kind(),
            raw_os_error: error.raw_os_error(),
            message: error.to_string(),
        }
    }

    fn to_error(&self) -> io::Error {
        self.raw_os_error.map_or_else(
            || io::Error::new(self.kind, self.message.clone()),
            io::Error::from_raw_os_error,
        )
    }
}

#[derive(Default)]
struct DirectionShutdown {
    requested: bool,
    operation: Option<PendingOperation<()>>,
    result: Option<Result<(), SharedIoError>>,
}

impl DirectionShutdown {
    fn is_requested(&self) -> bool {
        self.requested
    }

    fn request(&mut self) {
        self.requested = true;
    }

    fn poll(
        &mut self,
        cx: &mut Context<'_>,
        start: impl FnOnce() -> IoFuture<()>,
    ) -> Poll<io::Result<()>> {
        self.request();
        if let Some(result) = &self.result {
            return Poll::Ready(match result {
                Ok(()) => Ok(()),
                Err(error) => Err(error.to_error()),
            });
        }

        if self.operation.is_none() {
            self.operation = Some(PendingOperation::new(start()));
        }

        let result = match self
            .operation
            .as_mut()
            .expect("pending shutdown operation must exist")
            .poll(cx)
        {
            Poll::Ready(result) => result,
            Poll::Pending => return Poll::Pending,
        };
        self.operation = None;
        self.result = Some(match result {
            Ok(()) => Ok(()),
            Err(error) => Err(SharedIoError::new(error)),
        });

        match self.result.as_ref().expect("shutdown result must exist") {
            Ok(()) => Poll::Ready(Ok(())),
            Err(error) => Poll::Ready(Err(error.to_error())),
        }
    }
}

struct ReadOverflow {
    data: Vec<u8>,
    pos: usize,
}

impl ReadOverflow {
    fn remaining(&self) -> usize {
        self.data.len() - self.pos
    }

    fn front(&self, max: usize) -> &[u8] {
        let len = max.min(self.remaining());
        &self.data[self.pos..self.pos + len]
    }

    fn advance(&mut self, len: usize) {
        self.pos += len.min(self.remaining());
    }

    fn is_drained(&self) -> bool {
        self.pos == self.data.len()
    }
}

/// Pending read plus bytes completed for an abandoned, larger caller buffer.
///
/// The submitted operation remains owned by the resource after its public
/// future is cancelled. A later caller can therefore finish that operation,
/// and any bytes that do not fit its buffer are retained here for subsequent
/// reads.
#[derive(Default)]
pub(crate) struct ReadState {
    operation: Option<PendingOperation<Vec<u8>>>,
    overflow: Option<Box<ReadOverflow>>,
    shutdown: DirectionShutdown,
}

impl ReadState {
    pub(crate) fn poll_slice(
        &mut self,
        cx: &mut Context<'_>,
        buf: &mut [u8],
        start: impl FnOnce(usize) -> IoFuture<Vec<u8>>,
    ) -> Poll<io::Result<usize>> {
        self.poll_with(cx, buf.len(), start, |bytes| {
            buf[..bytes.len()].copy_from_slice(bytes);
        })
    }

    pub(crate) fn poll_with(
        &mut self,
        cx: &mut Context<'_>,
        capacity: usize,
        start: impl FnOnce(usize) -> IoFuture<Vec<u8>>,
        mut put: impl FnMut(&[u8]),
    ) -> Poll<io::Result<usize>> {
        if capacity == 0 {
            return Poll::Ready(Ok(0));
        }
        if self.shutdown.is_requested() {
            return Poll::Ready(Ok(0));
        }

        if let Some(overflow) = self.overflow.as_mut() {
            let len = capacity.min(overflow.remaining());
            put(overflow.front(len));
            overflow.advance(len);
            if overflow.is_drained() {
                self.overflow = None;
            }
            return Poll::Ready(Ok(len));
        }

        if self.operation.is_none() {
            self.operation = Some(PendingOperation::new(start(capacity)));
        }

        let result = match self
            .operation
            .as_mut()
            .expect("pending read operation must exist")
            .poll(cx)
        {
            Poll::Ready(result) => result,
            Poll::Pending => return Poll::Pending,
        };
        self.operation = None;

        match result {
            Ok(data) => {
                let len = capacity.min(data.len());
                put(&data[..len]);
                if len < data.len() {
                    self.overflow = Some(Box::new(ReadOverflow { data, pos: len }));
                }
                Poll::Ready(Ok(len))
            }
            Err(error) => Poll::Ready(Err(error)),
        }
    }

    pub(crate) fn poll_reconcile(&mut self, cx: &mut Context<'_>) -> Poll<io::Result<usize>> {
        let mut rewind = self
            .overflow
            .take()
            .map_or(0, |overflow| overflow.remaining());

        if let Some(operation) = self.operation.as_mut() {
            match operation.poll(cx) {
                Poll::Ready(Ok(data)) => {
                    rewind = rewind.checked_add(data.len()).ok_or_else(|| {
                        io::Error::new(io::ErrorKind::InvalidData, "read rewind exceeds usize")
                    })?;
                    self.operation = None;
                }
                Poll::Ready(Err(_)) => {
                    self.operation = None;
                }
                Poll::Pending => return Poll::Pending,
            }
        }

        Poll::Ready(Ok(rewind))
    }

    pub(crate) fn poll_shutdown(
        &mut self,
        cx: &mut Context<'_>,
        start: impl FnOnce() -> IoFuture<()>,
    ) -> Poll<io::Result<()>> {
        self.operation = None;
        self.overflow = None;
        self.shutdown.poll(cx, start)
    }
}

impl fmt::Debug for ReadState {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ReadState")
            .field("pending", &self.operation.is_some())
            .field(
                "overflow_bytes",
                &self
                    .overflow
                    .as_ref()
                    .map_or(0, |overflow| overflow.remaining()),
            )
            .field("shutdown", &self.shutdown.is_requested())
            .finish()
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum WriteKind {
    Operation(u64),
    #[cfg(feature = "hyper")]
    Buffered,
}

/// Identifies the buffer a raw `poll_write` caller submitted.
///
/// Untracked callers (raw `AsyncWrite::poll_write`) all share one generation,
/// so the generation alone cannot tell a re-poll of the same logical write from
/// a brand-new write started after the previous future was abandoned. Recording
/// the buffer distinguishes them, so an abandoned operation's byte count is
/// never credited to different bytes.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct WriteIdentity {
    address: usize,
    len: usize,
}

impl WriteIdentity {
    fn of(buf: &[u8]) -> Self {
        Self {
            address: buf.as_ptr() as usize,
            len: buf.len(),
        }
    }
}

struct PendingWrite {
    kind: WriteKind,
    /// `Some` only for untracked generations; tracked writes are already unique.
    identity: Option<WriteIdentity>,
    operation: PendingOperation<usize>,
}

struct QueuedWrite {
    generation: u64,
    waker: Waker,
}

/// One operation at a time for a writable resource.
///
/// Tracked public write futures carry a distinct generation and liveness token.
/// Live generations queue in first-poll order, and a completion observed by a
/// different clone is retained for its owner rather than discarded. Dropping
/// the token removes cancelled queued/completed state. Untracked direct poll
/// callers retain the stricter contract that they must drive the same operation
/// to completion.
#[derive(Default)]
pub(crate) struct WriteState {
    operation: Option<PendingWrite>,
    queue: VecDeque<QueuedWrite>,
    completed: HashMap<u64, io::Result<usize>>,
    /// Completion of the single in-flight *untracked* write, kept with the
    /// buffer it was submitted for. Untracked callers share one generation, so
    /// the buffer is the only thing identifying whose result this is. Retaining
    /// it stops a drain from consuming an operation whose caller has not yet
    /// observed it -- otherwise that caller's contract-mandated re-poll finds
    /// no operation and submits the same bytes a second time.
    untracked_completed: Option<(WriteIdentity, io::Result<usize>)>,
    barrier_waiters: WaiterSet,
    buffered_error: Option<io::Error>,
    shutdown: DirectionShutdown,
}

impl WriteState {
    pub(crate) fn poll_write(
        &mut self,
        cx: &mut Context<'_>,
        generation: u64,
        buf: &[u8],
        start: impl FnOnce(Vec<u8>) -> IoFuture<usize>,
    ) -> Poll<io::Result<usize>> {
        if buf.is_empty() {
            return Poll::Ready(Ok(0));
        }

        self.cleanup_cancelled();
        if let Some(result) = self.completed.remove(&generation) {
            return Poll::Ready(result);
        }
        if let Some(result) = self.take_untracked_completion(generation, buf) {
            return Poll::Ready(result);
        }

        let known = self
            .operation
            .as_ref()
            .is_some_and(|operation| operation.kind == WriteKind::Operation(generation))
            || self
                .queue
                .iter()
                .any(|queued| queued.generation == generation);
        if self.shutdown.is_requested() && !known {
            return Poll::Ready(Err(io::Error::new(
                io::ErrorKind::BrokenPipe,
                "write direction is shut down",
            )));
        }

        self.enqueue_tracked(generation, cx.waker());
        let mut start = Some(start);
        loop {
            if let Some((kind, identity)) = self
                .operation
                .as_ref()
                .map(|operation| (operation.kind, operation.identity))
            {
                match self.poll_pending(cx) {
                    Poll::Ready((completed_kind, completed_identity, result)) => {
                        self.wake_ready_waiters();
                        // An untracked operation only belongs to this caller if
                        // it was submitted for these exact bytes. Otherwise the
                        // previous future was abandoned and a new logical write
                        // began: consume the stale completion and submit fresh
                        // rather than crediting its count to a different buffer.
                        if kind == WriteKind::Operation(generation)
                            && identity.is_none_or(|identity| identity == WriteIdentity::of(buf))
                        {
                            return Poll::Ready(result);
                        }
                        self.retain_completion(completed_kind, completed_identity, result);
                        if let Some(error) = self.buffered_error.take() {
                            return Poll::Ready(Err(error));
                        }
                        continue;
                    }
                    Poll::Pending => return Poll::Pending,
                }
            }

            self.cleanup_cancelled();
            if let Some(result) = self.completed.remove(&generation) {
                return Poll::Ready(result);
            }

            if let Some(front) = self.queue.front() {
                if front.generation != generation {
                    front.waker.wake_by_ref();
                    register_write_cancellation_waker(front.generation, cx.waker());
                    return Poll::Pending;
                }
                let _ = self.queue.pop_front();
            } else if is_tracked_write(generation) && !is_live_write(generation) {
                return Poll::Ready(Err(io::Error::new(
                    io::ErrorKind::Interrupted,
                    "write operation was cancelled",
                )));
            }

            let future = start.take().expect("write factory used once")(buf.to_vec());
            self.operation = Some(PendingWrite {
                kind: WriteKind::Operation(generation),
                identity: (!is_tracked_write(generation)).then(|| WriteIdentity::of(buf)),
                operation: PendingOperation::new(future),
            });
        }
    }

    #[cfg(feature = "hyper")]
    pub(crate) fn poll_buffered_write(
        &mut self,
        cx: &mut Context<'_>,
        buf: &[u8],
        start: impl FnOnce(Vec<u8>) -> IoFuture<usize>,
    ) -> Poll<io::Result<usize>> {
        if buf.is_empty() {
            return Poll::Ready(Ok(0));
        }
        if self.shutdown.is_requested() {
            return Poll::Ready(Err(io::Error::new(
                io::ErrorKind::BrokenPipe,
                "write direction is shut down",
            )));
        }

        match self.poll_drain(cx) {
            Poll::Ready(()) => {}
            Poll::Pending => return Poll::Pending,
        }
        if let Some(error) = self.buffered_error.take() {
            return Poll::Ready(Err(error));
        }

        let accepted = buf.len();
        let mut operation = PendingOperation::new(start(buf.to_vec()));
        match operation.poll(cx) {
            Poll::Ready(Ok(_)) => Poll::Ready(Ok(accepted)),
            Poll::Ready(Err(error)) => Poll::Ready(Err(error)),
            Poll::Pending => {
                self.operation = Some(PendingWrite {
                    kind: WriteKind::Buffered,
                    identity: None,
                    operation,
                });
                Poll::Ready(Ok(accepted))
            }
        }
    }

    pub(crate) fn poll_flush(&mut self, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        match self.poll_drain(cx) {
            Poll::Ready(()) => {
                match self
                    .buffered_error
                    .take()
                    .or_else(|| self.take_untracked_error())
                {
                    Some(error) => Poll::Ready(Err(error)),
                    None => Poll::Ready(Ok(())),
                }
            }
            Poll::Pending => Poll::Pending,
        }
    }

    pub(crate) fn poll_drain(&mut self, cx: &mut Context<'_>) -> Poll<()> {
        loop {
            self.cleanup_cancelled();
            if self.operation.is_some() {
                match self.poll_pending(cx) {
                    Poll::Ready((kind, identity, result)) => {
                        self.retain_completion(kind, identity, result);
                        self.wake_ready_waiters();
                        continue;
                    }
                    Poll::Pending => return Poll::Pending,
                }
            }

            if let Some(front) = self.queue.front() {
                self.barrier_waiters.register(cx.waker());
                register_write_cancellation_waker(front.generation, cx.waker());
                front.waker.wake_by_ref();
                return Poll::Pending;
            }
            return Poll::Ready(());
        }
    }

    pub(crate) fn poll_shutdown(
        &mut self,
        cx: &mut Context<'_>,
        start: impl FnOnce() -> IoFuture<()>,
    ) -> Poll<io::Result<()>> {
        self.shutdown.request();
        if self.poll_drain(cx).is_pending() {
            return Poll::Pending;
        }
        if let Some(error) = self.buffered_error.take() {
            return Poll::Ready(Err(error));
        }
        self.shutdown.poll(cx, start)
    }

    fn enqueue_tracked(&mut self, generation: u64, waker: &Waker) {
        if !is_tracked_write(generation) || !is_live_write(generation) {
            return;
        }
        if self
            .operation
            .as_ref()
            .is_some_and(|operation| operation.kind == WriteKind::Operation(generation))
            || self.completed.contains_key(&generation)
        {
            return;
        }
        if let Some(queued) = self
            .queue
            .iter_mut()
            .find(|queued| queued.generation == generation)
        {
            if !queued.waker.will_wake(waker) {
                queued.waker = waker.clone();
            }
            return;
        }
        self.queue.push_back(QueuedWrite {
            generation,
            waker: waker.clone(),
        });
    }

    fn cleanup_cancelled(&mut self) {
        self.completed
            .retain(|generation, _| is_live_write(*generation));
        let old_len = self.queue.len();
        self.queue.retain(|queued| is_live_write(queued.generation));
        if self.queue.len() != old_len {
            self.wake_ready_waiters();
        }
    }

    /// Claims a retained untracked completion if it belongs to this caller.
    ///
    /// Untracked callers share one generation, so ownership is decided by the
    /// buffer the operation was submitted for.
    fn take_untracked_completion(
        &mut self,
        generation: u64,
        buf: &[u8],
    ) -> Option<io::Result<usize>> {
        if is_tracked_write(generation) {
            return None;
        }
        let identity = WriteIdentity::of(buf);
        match self.untracked_completed.as_ref() {
            Some((retained, _)) if *retained == identity => {
                self.untracked_completed.take().map(|(_, result)| result)
            }
            _ => None,
        }
    }

    /// Takes a retained untracked *failure* so a flush can report it.
    ///
    /// A successful count stays retained: it still belongs to the caller whose
    /// buffer produced it, and that caller's re-poll must resolve to it rather
    /// than resubmit.
    fn take_untracked_error(&mut self) -> Option<io::Error> {
        match self.untracked_completed.as_ref() {
            Some((_, Err(_))) => match self.untracked_completed.take() {
                Some((_, Err(error))) => Some(error),
                _ => None,
            },
            _ => None,
        }
    }

    fn retain_completion(
        &mut self,
        kind: WriteKind,
        identity: Option<WriteIdentity>,
        result: io::Result<usize>,
    ) {
        match kind {
            WriteKind::Operation(generation)
                if is_tracked_write(generation) && is_live_write(generation) =>
            {
                self.completed.insert(generation, result);
            }
            #[cfg(feature = "hyper")]
            WriteKind::Buffered => {
                if let Err(error) = result {
                    self.buffered_error = Some(error);
                }
            }
            WriteKind::Operation(_) => {
                // Untracked. Keep it for whichever caller submitted this
                // buffer; dropping it here would both lose the error and let a
                // re-poll resubmit the same bytes.
                if let Some(identity) = identity {
                    self.untracked_completed = Some((identity, result));
                }
            }
        }
    }

    fn wake_ready_waiters(&self) {
        if let Some(front) = self.queue.front() {
            front.waker.wake_by_ref();
        }
        self.barrier_waiters.wake_all();
    }

    fn poll_pending(
        &mut self,
        cx: &mut Context<'_>,
    ) -> Poll<(WriteKind, Option<WriteIdentity>, io::Result<usize>)> {
        let Some(operation) = self.operation.as_mut() else {
            return Poll::Ready((WriteKind::Operation(0), None, Ok(0)));
        };
        let kind = operation.kind;
        let identity = operation.identity;
        match operation.operation.poll(cx) {
            Poll::Ready(result) => {
                self.operation = None;
                Poll::Ready((kind, identity, result))
            }
            Poll::Pending => Poll::Pending,
        }
    }
}

impl fmt::Debug for WriteState {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("WriteState")
            .field(
                "pending",
                &self.operation.as_ref().map(|operation| operation.kind),
            )
            .field("queued", &self.queue.len())
            .field("completed", &self.completed.len())
            .field("shutdown", &self.shutdown.is_requested())
            .finish()
    }
}

/// Serialized state for cursor-based resources such as files.
#[derive(Debug, Default)]
pub(crate) struct CursorState {
    read: ReadState,
    write: WriteState,
}

impl CursorState {
    pub(crate) fn poll_read_slice(
        &mut self,
        cx: &mut Context<'_>,
        buf: &mut [u8],
        start: impl FnOnce(usize) -> IoFuture<Vec<u8>>,
    ) -> Poll<io::Result<usize>> {
        if self.write.poll_drain(cx).is_pending() {
            return Poll::Pending;
        }
        self.read.poll_slice(cx, buf, start)
    }

    pub(crate) fn poll_write(
        &mut self,
        cx: &mut Context<'_>,
        generation: u64,
        buf: &[u8],
        rewind: impl FnOnce(usize) -> io::Result<()>,
        start: impl FnOnce(Vec<u8>) -> IoFuture<usize>,
    ) -> Poll<io::Result<usize>> {
        match self.reconcile_read(cx, rewind) {
            Poll::Ready(Ok(())) => self.write.poll_write(cx, generation, buf, start),
            Poll::Ready(Err(error)) => Poll::Ready(Err(error)),
            Poll::Pending => Poll::Pending,
        }
    }

    /// Drains writes this cursor still owns and reports their failure.
    ///
    /// The write state is shared by every clone of the handle, so a flush on
    /// one clone waits for a sibling's in-flight write as well.
    pub(crate) fn poll_flush(&mut self, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        self.write.poll_flush(cx)
    }

    pub(crate) fn poll_reconcile(
        &mut self,
        cx: &mut Context<'_>,
        rewind: impl FnOnce(usize) -> io::Result<()>,
    ) -> Poll<io::Result<()>> {
        match self.reconcile_read(cx, rewind) {
            Poll::Ready(Ok(())) => self.write.poll_drain(cx).map(|()| Ok(())),
            Poll::Ready(Err(error)) => Poll::Ready(Err(error)),
            Poll::Pending => Poll::Pending,
        }
    }

    fn reconcile_read(
        &mut self,
        cx: &mut Context<'_>,
        rewind: impl FnOnce(usize) -> io::Result<()>,
    ) -> Poll<io::Result<()>> {
        match self.read.poll_reconcile(cx) {
            Poll::Ready(Ok(bytes)) => Poll::Ready(rewind(bytes)),
            Poll::Ready(Err(error)) => Poll::Ready(Err(error)),
            Poll::Pending => Poll::Pending,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{CursorState, IoFuture, ReadState, WriteOperation, WriteState};
    use core::cell::{Cell, RefCell};
    use core::future::Future;
    use core::pin::Pin;
    use core::task::{Context, Poll, Waker};
    use std::io;
    use std::rc::Rc;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::task::Wake;

    struct Gate<T> {
        result: Rc<RefCell<Option<io::Result<T>>>>,
        polls: Rc<Cell<usize>>,
        waker: Rc<RefCell<Option<Waker>>>,
        drop_log: Option<Rc<RefCell<Vec<&'static str>>>>,
    }

    #[derive(Clone)]
    struct GateHandle<T> {
        result: Rc<RefCell<Option<io::Result<T>>>>,
        polls: Rc<Cell<usize>>,
        waker: Rc<RefCell<Option<Waker>>>,
    }

    impl<T> Gate<T> {
        fn new() -> (Self, GateHandle<T>) {
            let result = Rc::new(RefCell::new(None));
            let polls = Rc::new(Cell::new(0));
            let waker = Rc::new(RefCell::new(None));
            (
                Self {
                    result: Rc::clone(&result),
                    polls: Rc::clone(&polls),
                    waker: Rc::clone(&waker),
                    drop_log: None,
                },
                GateHandle {
                    result,
                    polls,
                    waker,
                },
            )
        }
    }

    impl<T> Future for Gate<T> {
        type Output = io::Result<T>;

        fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
            self.polls.set(self.polls.get() + 1);
            match self.result.borrow_mut().take() {
                Some(result) => Poll::Ready(result),
                None => {
                    *self.waker.borrow_mut() = Some(cx.waker().clone());
                    Poll::Pending
                }
            }
        }
    }

    impl<T> Drop for Gate<T> {
        fn drop(&mut self) {
            if let Some(log) = &self.drop_log {
                log.borrow_mut().push("operation");
            }
        }
    }

    impl<T> GateHandle<T> {
        fn complete(&self, result: io::Result<T>) {
            *self.result.borrow_mut() = Some(result);
            if let Some(waker) = self.waker.borrow_mut().take() {
                waker.wake();
            }
        }

        fn polls(&self) -> usize {
            self.polls.get()
        }
    }

    fn boxed<T: 'static>(future: impl Future<Output = io::Result<T>> + 'static) -> IoFuture<T> {
        Box::pin(future)
    }

    fn context() -> Context<'static> {
        Context::from_waker(Waker::noop())
    }

    fn ready<T>(poll: Poll<T>) -> T {
        match poll {
            Poll::Ready(value) => value,
            Poll::Pending => panic!("operation should be ready"),
        }
    }

    struct WakeFlag(AtomicBool);

    impl Wake for WakeFlag {
        fn wake(self: Arc<Self>) {
            self.0.store(true, Ordering::Release);
        }

        fn wake_by_ref(self: &Arc<Self>) {
            self.0.store(true, Ordering::Release);
        }
    }

    #[test]
    fn read_completion_overflow_survives_a_cancelled_larger_read() {
        let mut state = ReadState::default();
        let (first, first_handle) = Gate::new();
        let mut first = Some(boxed(first));
        let mut cx = context();
        let mut large = [0; 8];

        assert!(
            state
                .poll_slice(&mut cx, &mut large, |_| first.take().unwrap())
                .is_pending()
        );

        first_handle.complete(Ok(b"abcdef".to_vec()));
        let mut small = [0; 2];
        assert_eq!(
            ready(state.poll_slice(&mut cx, &mut small, |_| { panic!("read already started") }))
                .unwrap(),
            2
        );
        assert_eq!(&small, b"ab");

        let mut rest = [0; 4];
        assert_eq!(
            ready(state.poll_slice(&mut cx, &mut rest, |_| {
                panic!("overflow must drain first")
            }))
            .unwrap(),
            4
        );
        assert_eq!(&rest, b"cdef");
    }

    #[test]
    fn changed_write_waits_for_and_discards_the_orphan_completion() {
        let mut state = WriteState::default();
        let (first, first_handle) = Gate::new();
        let (second, second_handle) = Gate::new();
        let mut first = Some(boxed(first));
        let mut second = Some(boxed(second));
        let started = Rc::new(RefCell::new(Vec::new()));
        let mut cx = context();

        assert!(
            state
                .poll_write(&mut cx, 1, b"old", {
                    let started = Rc::clone(&started);
                    move |data| {
                        started.borrow_mut().push(data);
                        first.take().unwrap()
                    }
                })
                .is_pending()
        );

        assert!(
            state
                .poll_write(&mut cx, 2, b"new bytes", |_| {
                    panic!("orphan still pending")
                })
                .is_pending()
        );
        assert_eq!(first_handle.polls(), 2);

        first_handle.complete(Ok(3));
        assert!(
            state
                .poll_write(&mut cx, 2, b"new bytes", {
                    let started = Rc::clone(&started);
                    move |data| {
                        started.borrow_mut().push(data);
                        second.take().unwrap()
                    }
                })
                .is_pending()
        );

        second_handle.complete(Ok(9));
        assert_eq!(
            ready(state.poll_write(&mut cx, 2, b"new bytes", |_| {
                panic!("write already started")
            }))
            .unwrap(),
            9
        );
        assert_eq!(
            &*started.borrow(),
            &[b"old".to_vec(), b"new bytes".to_vec()]
        );
    }

    #[test]
    fn live_write_completion_is_retained_for_its_generation() {
        let mut state = WriteState::default();
        let first_owner = WriteOperation::new();
        let second_owner = WriteOperation::new();
        let (first, first_handle) = Gate::new();
        let (second, second_handle) = Gate::new();
        let mut first = Some(boxed(first));
        let mut second = Some(boxed(second));
        let started = Rc::new(RefCell::new(Vec::new()));
        let mut cx = context();

        assert!(
            state
                .poll_write(&mut cx, first_owner.generation(), b"A", {
                    let started = Rc::clone(&started);
                    move |data| {
                        started.borrow_mut().push(data);
                        first.take().unwrap()
                    }
                })
                .is_pending()
        );
        assert!(
            state
                .poll_write(&mut cx, second_owner.generation(), b"B", |_| {
                    panic!("second generation must remain queued")
                })
                .is_pending()
        );

        first_handle.complete(Ok(1));
        assert!(
            state
                .poll_write(&mut cx, second_owner.generation(), b"B", {
                    let started = Rc::clone(&started);
                    move |data| {
                        started.borrow_mut().push(data);
                        second.take().unwrap()
                    }
                })
                .is_pending()
        );
        assert_eq!(
            ready(
                state.poll_write(&mut cx, first_owner.generation(), b"A", |_| {
                    panic!("first generation must not be resubmitted")
                })
            )
            .unwrap(),
            1
        );

        second_handle.complete(Ok(1));
        assert_eq!(
            ready(
                state.poll_write(&mut cx, second_owner.generation(), b"B", |_| {
                    panic!("second generation already started")
                })
            )
            .unwrap(),
            1
        );
        assert_eq!(&*started.borrow(), &[b"A".to_vec(), b"B".to_vec()]);
    }

    #[test]
    fn cancelled_live_generation_is_removed_from_the_queue() {
        let mut state = WriteState::default();
        let first_owner = WriteOperation::new();
        let cancelled_owner = WriteOperation::new();
        let (first, first_handle) = Gate::new();
        let mut first = Some(boxed(first));
        let mut cx = context();

        assert!(
            state
                .poll_write(&mut cx, first_owner.generation(), b"A", |_| {
                    first.take().unwrap()
                })
                .is_pending()
        );
        assert!(
            state
                .poll_write(&mut cx, cancelled_owner.generation(), b"discard", |_| {
                    panic!("cancelled generation must remain queued")
                },)
                .is_pending()
        );
        drop(cancelled_owner);

        first_handle.complete(Ok(1));
        assert_eq!(
            ready(
                state.poll_write(&mut cx, first_owner.generation(), b"A", |_| {
                    panic!("first generation already started")
                })
            )
            .unwrap(),
            1
        );
        assert!(state.poll_drain(&mut cx).is_ready());
    }

    #[test]
    fn completed_orphan_and_timeout_never_satisfy_the_next_write() {
        let mut state = WriteState::default();
        let (old, old_handle) = Gate::new();
        let (new, new_handle) = Gate::new();
        let mut old = Some(boxed(old));
        let mut new = Some(boxed(new));
        let mut cx = context();

        assert!(
            state
                .poll_write(&mut cx, 1, b"old", |_| old.take().unwrap())
                .is_pending()
        );
        old_handle.complete(Err(io::Error::new(io::ErrorKind::TimedOut, "old timeout")));

        assert!(
            state
                .poll_write(&mut cx, 2, b"new", |_| new.take().unwrap())
                .is_pending()
        );
        new_handle.complete(Ok(3));
        assert_eq!(
            ready(state.poll_write(&mut cx, 2, b"new", |_| { panic!("write already started") }))
                .unwrap(),
            3
        );
    }

    /// Regression: a flush that drains an in-flight untracked write must not
    /// consume it. `AsyncWrite::poll_write` requires the caller to re-poll the
    /// same buffer after `Pending`, and if the drain discarded the operation
    /// that re-poll would submit the same bytes a second time.
    #[test]
    fn flush_does_not_make_an_untracked_repoll_write_twice() {
        const UNTRACKED: u64 = 0;
        let mut state = WriteState::default();
        let (gate, handle) = Gate::new();
        let mut pending = Some(boxed(gate));
        let buf = *b"exactly once";
        let mut cx = context();
        let mut submissions = 0usize;

        assert!(
            state
                .poll_write(&mut cx, UNTRACKED, &buf, |_| {
                    submissions += 1;
                    pending.take().unwrap()
                })
                .is_pending()
        );
        handle.complete(Ok(buf.len()));

        // A flush drains the operation the caller has not yet observed.
        assert!(ready(state.poll_flush(&mut cx)).is_ok());

        // The contract-mandated re-poll must resolve to that operation, not
        // start another one.
        assert_eq!(
            ready(state.poll_write(&mut cx, UNTRACKED, &buf, |_| {
                panic!("the drained completion must satisfy this re-poll")
            }))
            .unwrap(),
            buf.len()
        );
        assert_eq!(submissions, 1, "the bytes must be submitted exactly once");
    }

    /// A flush must surface the failure of a write the resource still owns;
    /// dropping it reports success for bytes that never reached the peer.
    #[test]
    fn flush_reports_an_untracked_write_failure() {
        const UNTRACKED: u64 = 0;
        let mut state = WriteState::default();
        let (gate, handle) = Gate::new();
        let mut pending = Some(boxed(gate));
        let buf = *b"doomed";
        let mut cx = context();

        assert!(
            state
                .poll_write(&mut cx, UNTRACKED, &buf, |_| pending.take().unwrap())
                .is_pending()
        );
        handle.complete(Err(io::Error::from(io::ErrorKind::ConnectionReset)));

        let error = ready(state.poll_flush(&mut cx))
            .expect_err("flush must report the abandoned write's failure");
        assert_eq!(error.kind(), io::ErrorKind::ConnectionReset);
    }

    /// Regression: raw `AsyncWrite::poll_write` callers all share one untracked
    /// generation. An abandoned write's byte count must never be reported as
    /// though it wrote the bytes a later, different buffer holds.
    #[test]
    fn untracked_write_does_not_credit_an_abandoned_count_to_a_new_buffer() {
        const UNTRACKED: u64 = 0;
        let mut state = WriteState::default();
        let (old, old_handle) = Gate::new();
        let (new, new_handle) = Gate::new();
        let mut old = Some(boxed(old));
        let mut new = Some(boxed(new));
        let mut cx = context();

        // A large write is submitted and then abandoned by its caller.
        let old_buf = vec![b'A'; 4096];
        assert!(
            state
                .poll_write(&mut cx, UNTRACKED, &old_buf, |_| old.take().unwrap())
                .is_pending()
        );
        old_handle.complete(Ok(old_buf.len()));

        // A new, much smaller buffer must not be credited with the old count.
        let new_buf = *b"sixteen bytes!!!";
        assert!(
            state
                .poll_write(&mut cx, UNTRACKED, &new_buf, |_| new.take().unwrap())
                .is_pending(),
            "the abandoned count must be consumed, not reported for new bytes"
        );

        new_handle.complete(Ok(new_buf.len()));
        let written = ready(state.poll_write(&mut cx, UNTRACKED, &new_buf, |_| {
            panic!("write already started")
        }))
        .unwrap();
        assert_eq!(
            written,
            new_buf.len(),
            "a write must never report more bytes than its buffer holds"
        );
    }

    /// Re-polling an untracked write with the same buffer is the documented
    /// contract and must still resolve to that operation's own result.
    #[test]
    fn untracked_write_repoll_with_the_same_buffer_keeps_its_completion() {
        const UNTRACKED: u64 = 0;
        let mut state = WriteState::default();
        let (pending, handle) = Gate::new();
        let mut pending = Some(boxed(pending));
        let buf = *b"stable";
        let mut cx = context();

        assert!(
            state
                .poll_write(&mut cx, UNTRACKED, &buf, |_| pending.take().unwrap())
                .is_pending()
        );
        handle.complete(Ok(buf.len()));
        assert_eq!(
            ready(state.poll_write(&mut cx, UNTRACKED, &buf, |_| {
                panic!("write already started")
            }))
            .unwrap(),
            buf.len()
        );
    }

    #[test]
    fn equal_bytes_in_a_different_buffer_start_a_new_operation() {
        let mut state = WriteState::default();
        let (old, old_handle) = Gate::new();
        let (new, new_handle) = Gate::new();
        let mut old = Some(boxed(old));
        let mut new = Some(boxed(new));
        let old_buf = *b"same";
        let new_buf = *b"same";
        assert_ne!(old_buf.as_ptr(), new_buf.as_ptr());
        let mut cx = context();

        assert!(
            state
                .poll_write(&mut cx, 1, &old_buf, |_| old.take().unwrap())
                .is_pending()
        );
        old_handle.complete(Ok(old_buf.len()));
        assert!(
            state
                .poll_write(&mut cx, 2, &new_buf, |_| new.take().unwrap())
                .is_pending(),
            "the old count must be consumed rather than returned for a new buffer"
        );
        new_handle.complete(Ok(new_buf.len()));
        assert_eq!(
            ready(state.poll_write(&mut cx, 2, &new_buf, |_| {
                panic!("write already started")
            }))
            .unwrap(),
            new_buf.len()
        );
    }

    #[test]
    fn mutated_bytes_in_the_same_allocation_start_a_new_operation() {
        let mut state = WriteState::default();
        let (old, old_handle) = Gate::new();
        let (new, new_handle) = Gate::new();
        let mut old = Some(boxed(old));
        let mut new = Some(boxed(new));
        let mut buf = *b"old";
        let address = buf.as_ptr();
        let mut cx = context();

        assert!(
            state
                .poll_write(&mut cx, 1, &buf, |_| old.take().unwrap())
                .is_pending()
        );
        old_handle.complete(Ok(buf.len()));
        buf.copy_from_slice(b"new");
        assert_eq!(address, buf.as_ptr());
        assert!(
            state
                .poll_write(&mut cx, 2, &buf, |_| new.take().unwrap())
                .is_pending(),
            "a new generation must not alias the old address"
        );
        new_handle.complete(Ok(buf.len()));
        assert_eq!(
            ready(state.poll_write(&mut cx, 2, &buf, |_| { panic!("write already started") }))
                .unwrap(),
            buf.len()
        );
    }

    #[test]
    fn repeated_prefix_buffer_with_a_new_generation_is_not_the_old_write() {
        let mut state = WriteState::default();
        let (old, old_handle) = Gate::new();
        let (new, new_handle) = Gate::new();
        let mut old = Some(boxed(old));
        let mut new = Some(boxed(new));
        let mut cx = context();

        assert!(
            state
                .poll_write(&mut cx, 41, b"frame", |_| old.take().unwrap())
                .is_pending()
        );
        old_handle.complete(Ok(5));
        assert!(
            state
                .poll_write(&mut cx, 42, b"frame plus more", |_| { new.take().unwrap() })
                .is_pending()
        );
        new_handle.complete(Ok(b"frame plus more".len()));
        assert_eq!(
            ready(state.poll_write(&mut cx, 42, b"frame plus more", |_| {
                panic!("new operation already started")
            }))
            .unwrap(),
            b"frame plus more".len()
        );
    }

    #[cfg(feature = "hyper")]
    #[test]
    fn buffered_framework_write_accepts_each_logical_buffer_once() {
        let mut state = WriteState::default();
        let (first, first_handle) = Gate::new();
        let (second, _second_handle) = Gate::new();
        let mut first = Some(boxed(first));
        let mut second = Some(boxed(second));
        let mut cx = context();

        assert_eq!(
            ready(state.poll_buffered_write(&mut cx, b"frame", |_| { first.take().unwrap() }))
                .unwrap(),
            5
        );
        assert!(
            state
                .poll_buffered_write(&mut cx, b"frame plus more", |_| {
                    panic!("first accepted buffer is still pending")
                })
                .is_pending()
        );

        first_handle.complete(Ok(5));
        assert_eq!(
            ready(
                state.poll_buffered_write(&mut cx, b"frame plus more", |_| {
                    second.take().unwrap()
                })
            )
            .unwrap(),
            b"frame plus more".len()
        );
    }

    #[test]
    fn shutdown_is_ordered_after_an_abandoned_write() {
        let mut state = WriteState::default();
        let (write, write_handle) = Gate::new();
        let (shutdown, shutdown_handle) = Gate::new();
        let mut write = Some(boxed(write));
        let mut shutdown = Some(boxed(shutdown));
        let mut cx = context();

        assert!(
            state
                .poll_write(&mut cx, 1, b"payload", |_| write.take().unwrap())
                .is_pending()
        );
        assert!(
            state
                .poll_shutdown(&mut cx, || { panic!("write must finish before shutdown") })
                .is_pending()
        );

        write_handle.complete(Ok(7));
        assert!(
            state
                .poll_shutdown(&mut cx, || shutdown.take().unwrap())
                .is_pending()
        );
        shutdown_handle.complete(Ok(()));
        assert!(
            ready(state.poll_shutdown(&mut cx, || { panic!("shutdown already started") })).is_ok()
        );
    }

    #[test]
    fn concurrent_shutdown_callers_are_all_woken_and_observe_the_result() {
        let mut state = WriteState::default();
        let (shutdown, shutdown_handle) = Gate::new();
        let mut shutdown = Some(boxed(shutdown));
        let first_flag = Arc::new(WakeFlag(AtomicBool::new(false)));
        let second_flag = Arc::new(WakeFlag(AtomicBool::new(false)));
        let first_waker = Waker::from(Arc::clone(&first_flag));
        let mut first_cx = Context::from_waker(&first_waker);

        assert!(
            state
                .poll_shutdown(&mut first_cx, || shutdown.take().unwrap())
                .is_pending()
        );
        {
            let second_waker = Waker::from(Arc::clone(&second_flag));
            let mut second_cx = Context::from_waker(&second_waker);
            assert!(
                state
                    .poll_shutdown(&mut second_cx, || panic!("shutdown already started"))
                    .is_pending()
            );
        }

        // The latest caller is now abandoned. The broadcast waker must wake
        // every registered caller without waiting for that caller to poll.
        shutdown_handle.complete(Ok(()));
        assert!(first_flag.0.load(Ordering::Acquire));
        assert!(second_flag.0.load(Ordering::Acquire));
        assert!(
            ready(state.poll_shutdown(&mut first_cx, || { panic!("shutdown already started") }))
                .is_ok()
        );
    }

    #[test]
    fn shutdown_error_preserves_raw_os_error_for_every_caller() {
        let mut state = WriteState::default();
        let mut cx = context();
        let raw = 12_345;
        let first = ready(state.poll_shutdown(&mut cx, || {
            Box::pin(std::future::ready(Err(io::Error::from_raw_os_error(raw))))
        }))
        .expect_err("shutdown should fail");
        assert_eq!(first.raw_os_error(), Some(raw));

        let second = ready(state.poll_shutdown(&mut cx, || {
            panic!("failed shutdown result must be retained")
        }))
        .expect_err("retained shutdown should fail");
        assert_eq!(second.raw_os_error(), Some(raw));
    }

    #[test]
    fn read_shutdown_cancels_pending_reads_and_discards_overflow() {
        let mut state = ReadState::default();
        let dropped = Rc::new(RefCell::new(Vec::new()));
        let (mut read, _read_handle) = Gate::<Vec<u8>>::new();
        read.drop_log = Some(Rc::clone(&dropped));
        let mut read = Some(boxed(read));
        let mut cx = context();
        let mut buf = [0; 8];
        assert!(
            state
                .poll_slice(&mut cx, &mut buf, |_| read.take().unwrap())
                .is_pending()
        );

        let (shutdown, shutdown_handle) = Gate::new();
        let mut shutdown = Some(boxed(shutdown));
        assert!(
            state
                .poll_shutdown(&mut cx, || shutdown.take().unwrap())
                .is_pending()
        );
        assert_eq!(&*dropped.borrow(), &["operation"]);

        let mut after = [0; 1];
        assert_eq!(
            ready(state.poll_slice(&mut cx, &mut after, |_| {
                panic!("read direction is shut down")
            }))
            .unwrap(),
            0
        );
        shutdown_handle.complete(Ok(()));
        assert!(
            ready(state.poll_shutdown(&mut cx, || { panic!("shutdown already started") })).is_ok()
        );

        let mut overflow = ReadState::default();
        let mut small = [0; 1];
        assert_eq!(
            ready(overflow.poll_slice(&mut cx, &mut small, |_| {
                Box::pin(std::future::ready(Ok(b"abc".to_vec())))
            }))
            .unwrap(),
            1
        );
        assert!(
            ready(overflow.poll_shutdown(&mut cx, || { Box::pin(std::future::ready(Ok(()))) }))
                .is_ok()
        );
        let mut discarded = [0; 2];
        assert_eq!(
            ready(overflow.poll_slice(&mut cx, &mut discarded, |_| {
                panic!("overflow must be discarded on read shutdown")
            }))
            .unwrap(),
            0
        );
    }

    /// A cursor's write state is shared by every clone of the handle, so a
    /// flush must wait for a write any clone still owns rather than reporting
    /// success while bytes are in flight.
    #[test]
    fn cursor_flush_waits_for_a_write_the_handle_still_owns() {
        const UNTRACKED: u64 = 0;
        let mut state = CursorState::default();
        let (gate, handle) = Gate::new();
        let mut pending = Some(boxed(gate));
        let buf = *b"in flight";
        let mut cx = context();

        assert!(
            state
                .poll_write(
                    &mut cx,
                    UNTRACKED,
                    &buf,
                    |_| Ok(()),
                    |_| pending.take().unwrap()
                )
                .is_pending()
        );
        assert!(
            state.poll_flush(&mut cx).is_pending(),
            "flush must not report success while the write is outstanding"
        );

        handle.complete(Ok(buf.len()));
        assert!(ready(state.poll_flush(&mut cx)).is_ok());
    }

    #[test]
    fn cursor_reconcile_orders_injected_read_before_write_and_rewind() {
        let mut state = CursorState::default();
        let cursor = Rc::new(Cell::new(0i64));
        let order = Rc::new(RefCell::new(Vec::new()));
        let (read, read_handle) = Gate::new();
        let (write, write_handle) = Gate::new();

        let read_cursor = Rc::clone(&cursor);
        let read_order = Rc::clone(&order);
        let mut read = Some(boxed(async move {
            let data: Vec<u8> = read.await?;
            read_cursor.set(data.len() as i64);
            read_order.borrow_mut().push("read");
            Ok(data)
        }));
        let write_cursor = Rc::clone(&cursor);
        let write_order = Rc::clone(&order);
        let mut write = Some(boxed(async move {
            let written: usize = write.await?;
            write_cursor.set(written as i64);
            write_order.borrow_mut().push("write");
            Ok(written)
        }));
        let mut cx = context();
        let mut read_buf = [0; 4];

        assert!(
            state
                .read
                .poll_slice(&mut cx, &mut read_buf, |_| read.take().unwrap())
                .is_pending()
        );
        assert!(
            state
                .write
                .poll_write(&mut cx, 1, b"abc", |_| write.take().unwrap())
                .is_pending()
        );
        read_handle.complete(Ok(b"read".to_vec()));
        write_handle.complete(Ok(3));

        let rewind_cursor = Rc::clone(&cursor);
        let rewind_order = Rc::clone(&order);
        assert!(
            ready(state.poll_reconcile(&mut cx, move |bytes| {
                rewind_cursor.set(rewind_cursor.get() - bytes as i64);
                rewind_order.borrow_mut().push("rewind");
                Ok(())
            }))
            .is_ok()
        );
        assert_eq!(&*order.borrow(), &["read", "rewind", "write"]);
        assert_eq!(cursor.get(), 3, "write advancement must win after rewind");
    }

    #[test]
    fn draining_writes_does_not_orphan_an_inflight_shutdown() {
        let mut state = WriteState::default();
        let (shutdown, shutdown_handle) = Gate::new();
        let mut shutdown = Some(boxed(shutdown));
        let mut cx = context();

        assert!(
            state
                .poll_shutdown(&mut cx, || shutdown.take().unwrap())
                .is_pending()
        );
        assert!(state.poll_drain(&mut cx).is_ready());
        assert_eq!(
            shutdown_handle.polls(),
            1,
            "flush must leave shutdown for its original caller"
        );

        shutdown_handle.complete(Ok(()));
        assert!(
            ready(state.poll_shutdown(&mut cx, || { panic!("shutdown already started") })).is_ok()
        );
    }

    #[test]
    fn pending_operation_drops_before_its_resource_owner() {
        struct Owner(Rc<RefCell<Vec<&'static str>>>);
        impl Drop for Owner {
            fn drop(&mut self) {
                self.0.borrow_mut().push("owner");
            }
        }
        struct Resource {
            _state: WriteState,
            _owner: Owner,
        }

        let log = Rc::new(RefCell::new(Vec::new()));
        let (mut operation, _handle) = Gate::<usize>::new();
        operation.drop_log = Some(Rc::clone(&log));
        let mut state = WriteState::default();
        let mut operation = Some(boxed(operation));
        let mut cx = context();
        assert!(
            state
                .poll_write(&mut cx, 1, b"x", |_| operation.take().unwrap())
                .is_pending()
        );

        drop(Resource {
            _state: state,
            _owner: Owner(Rc::clone(&log)),
        });
        assert_eq!(&*log.borrow(), &["operation", "owner"]);
    }
}
