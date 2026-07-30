//! Cooperative cancellation signalling.
//!
//! A [`CancellationToken`] is a shared flag that tasks can poll or await. It
//! complements [`AbortHandle`](crate::AbortHandle), which terminates a task at
//! its next suspension point whether or not the task is ready: an abort is
//! something done *to* a task, a token is something a task chooses to observe.
//! Use a token when work needs to finish what it is doing — flush a buffer,
//! release a lock, report a partial result — rather than stopping wherever it
//! happens to be.
//!
//! Tokens are `!Send`, like the rest of this module. They coordinate tasks on
//! one runtime thread; to cancel work on another thread, send a message through
//! a channel or [`ThreadHandle`](crate::ThreadHandle) and cancel a token there.

use core::cell::{Cell, RefCell};
use core::future::poll_fn;
use core::task::{Poll, Waker};
use std::rc::{Rc, Weak};

/// A cloneable, hierarchical cancellation signal.
///
/// Every clone observes the same signal, and [`child_token`](Self::child_token)
/// creates a token that is cancelled when its parent is — so a subsystem can
/// hand children to its own tasks and cancel them all, without being able to
/// cancel its parent's other branches.
///
/// # Examples
///
/// ```
/// use std::cell::Cell;
/// use std::rc::Rc;
///
/// let token = runite::sync::CancellationToken::new();
/// let stopped = Rc::new(Cell::new(false));
///
/// runite::spawn({
///     let token = token.clone();
///     let stopped = Rc::clone(&stopped);
///     async move {
///         token.cancelled().await;
///         stopped.set(true);
///     }
/// });
///
/// runite::spawn(async move {
///     token.cancel();
/// });
///
/// runite::run();
/// assert!(stopped.get());
/// ```
#[derive(Clone)]
pub struct CancellationToken {
    state: Rc<TokenState>,
}

struct TokenState {
    cancelled: Cell<bool>,
    wakers: RefCell<WakerSlots>,
    /// Children are held weakly: a token that nobody kept must not be pinned
    /// alive by its parent, and cancelling a parent should not resurrect one.
    children: RefCell<Vec<Weak<TokenState>>>,
}

/// Registered [`CancellationToken::cancelled`] wakers, addressable by slot.
///
/// A plain `Vec<Waker>` cannot support deregistration, and without
/// deregistration a token that is never cancelled grows by one waker for every
/// `cancelled()` future ever polled — the `select!`-in-a-loop shape the module
/// documentation recommends. Each retained waker also pins the shared state of
/// the task that registered it, so finished tasks stay resident. Slots give a
/// waiter an identity it can surrender on drop; the free list keeps
/// registration O(1) without leaving holes behind.
#[derive(Default)]
struct WakerSlots {
    slots: Vec<Option<Waker>>,
    free: Vec<usize>,
}

impl WakerSlots {
    fn register(&mut self, waker: Waker) -> usize {
        match self.free.pop() {
            Some(slot) => {
                self.slots[slot] = Some(waker);
                slot
            }
            None => {
                self.slots.push(Some(waker));
                self.slots.len() - 1
            }
        }
    }

    fn deregister(&mut self, slot: usize) {
        // Cancellation takes every slot at once, so a waiter dropped after its
        // token was cancelled has nothing left to release. It cannot collide
        // with a later waiter either: a cancelled token never registers again.
        let Some(entry) = self.slots.get_mut(slot) else {
            return;
        };
        *entry = None;
        self.free.push(slot);
        if self.free.len() == self.slots.len() {
            // No waiters left: release the backing allocations rather than
            // holding a high-water mark for the life of the token.
            self.slots = Vec::new();
            self.free = Vec::new();
        }
    }

    fn take_all(&mut self, out: &mut Vec<Waker>) {
        out.extend(std::mem::take(&mut self.slots).into_iter().flatten());
        self.free = Vec::new();
    }
}

/// Holds a waiter's slot for as long as its `cancelled()` future lives.
///
/// The registration has to be undone when the future is dropped rather than
/// when the token is cancelled, because the common case is a future that is
/// dropped without the token ever being cancelled.
struct Registration<'token> {
    state: &'token Rc<TokenState>,
    slot: Option<usize>,
}

impl Drop for Registration<'_> {
    fn drop(&mut self) {
        if let Some(slot) = self.slot {
            self.state.wakers.borrow_mut().deregister(slot);
        }
    }
}

impl TokenState {
    fn new() -> Rc<Self> {
        Rc::new(Self {
            cancelled: Cell::new(false),
            wakers: RefCell::new(WakerSlots::default()),
            children: RefCell::new(Vec::new()),
        })
    }

    /// Marks this subtree cancelled and returns every waker to notify.
    ///
    /// Wakers are collected rather than woken in place: waking runs user code,
    /// which may clone the token, create a child, or cancel something else, and
    /// doing that while `wakers` or `children` is borrowed would panic. The
    /// borrows are all released before any waker runs.
    fn collect_cancellation(self: &Rc<Self>, wakers: &mut Vec<Waker>) {
        if self.cancelled.replace(true) {
            return;
        }
        self.wakers.borrow_mut().take_all(wakers);
        let children = std::mem::take(&mut *self.children.borrow_mut());
        for child in children {
            if let Some(child) = child.upgrade() {
                child.collect_cancellation(wakers);
            }
        }
    }
}

impl CancellationToken {
    /// Creates an uncancelled token.
    ///
    /// # Examples
    ///
    /// ```
    /// let token = runite::sync::CancellationToken::new();
    /// assert!(!token.is_cancelled());
    /// ```
    pub fn new() -> Self {
        Self {
            state: TokenState::new(),
        }
    }

    /// Cancels this token and every token derived from it.
    ///
    /// Idempotent: cancelling an already-cancelled token does nothing. Clones
    /// of this token observe the cancellation; the parent it was derived from,
    /// if any, does not.
    ///
    /// # Examples
    ///
    /// ```
    /// let parent = runite::sync::CancellationToken::new();
    /// let child = parent.child_token();
    ///
    /// child.cancel();
    /// assert!(child.is_cancelled());
    /// assert!(!parent.is_cancelled(), "cancellation flows down, not up");
    /// ```
    pub fn cancel(&self) {
        let mut wakers = Vec::new();
        self.state.collect_cancellation(&mut wakers);
        for waker in wakers {
            waker.wake();
        }
    }

    /// Returns whether this token has been cancelled.
    ///
    /// # Examples
    ///
    /// ```
    /// let token = runite::sync::CancellationToken::new();
    /// token.cancel();
    /// assert!(token.is_cancelled());
    /// ```
    pub fn is_cancelled(&self) -> bool {
        self.state.cancelled.get()
    }

    /// Creates a token cancelled when this one is.
    ///
    /// Cancelling the child does not cancel the parent, so a subsystem can be
    /// given a child, cancel its own work, and leave its siblings running.
    ///
    /// A child of an already-cancelled parent starts cancelled.
    ///
    /// # Examples
    ///
    /// ```
    /// let parent = runite::sync::CancellationToken::new();
    /// let child = parent.child_token();
    ///
    /// parent.cancel();
    /// assert!(child.is_cancelled(), "cancellation reaches children");
    /// ```
    pub fn child_token(&self) -> Self {
        let child = TokenState::new();
        if self.state.cancelled.get() {
            child.cancelled.set(true);
        } else {
            let mut children = self.state.children.borrow_mut();
            // A dropped child leaves its `Weak` behind, and a `Weak` keeps the
            // child's allocation reserved — so a long-lived parent handing out
            // one child per request would grow without bound. Compacting only
            // when the list is full makes this amortized O(1) and bounds the
            // list at twice the live child count.
            if children.len() == children.capacity() {
                children.retain(|child| child.strong_count() > 0);
            }
            children.push(Rc::downgrade(&child));
        }
        Self { state: child }
    }

    /// Waits until this token is cancelled.
    ///
    /// Resolves immediately if it already is. Cancel-safe: dropping the
    /// returned future releases only its own registration, leaving every other
    /// waiter untouched, and a later call observes the same state.
    ///
    /// # Examples
    ///
    /// ```
    /// # async fn example(token: runite::sync::CancellationToken) {
    /// token.cancelled().await;
    /// # }
    /// ```
    pub async fn cancelled(&self) {
        let mut registration = Registration {
            state: &self.state,
            slot: None,
        };
        poll_fn(|context| {
            if self.state.cancelled.get() {
                // Cancellation already took every slot, so there is nothing
                // for the guard to release.
                registration.slot = None;
                return Poll::Ready(());
            }
            if registration.slot.is_none() {
                let slot = self
                    .state
                    .wakers
                    .borrow_mut()
                    .register(context.waker().clone());
                registration.slot = Some(slot);
            }
            Poll::Pending
        })
        .await;
    }
}

impl Default for CancellationToken {
    fn default() -> Self {
        Self::new()
    }
}

impl std::fmt::Debug for CancellationToken {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("CancellationToken")
            .field("cancelled", &self.is_cancelled())
            .finish_non_exhaustive()
    }
}

#[cfg(test)]
mod tests {
    use super::CancellationToken;
    use crate::{queue_macrotask, run, spawn};
    use core::future::Future;
    use core::pin::pin;
    use core::task::{Context, Poll, Waker};
    use std::cell::Cell;
    use std::rc::Rc;

    /// Polls `token.cancelled()` once and drops the future, as `select!` does
    /// on every loop iteration whose other branch wins.
    fn poll_once_and_drop(token: &CancellationToken) -> Poll<()> {
        let mut future = pin!(token.cancelled());
        future
            .as_mut()
            .poll(&mut Context::from_waker(Waker::noop()))
    }

    #[test]
    fn cancellation_reaches_clones_and_children_but_not_parents() {
        let parent = CancellationToken::new();
        let clone = parent.clone();
        let child = parent.child_token();
        let grandchild = child.child_token();
        let sibling = parent.child_token();

        child.cancel();
        assert!(child.is_cancelled());
        assert!(
            grandchild.is_cancelled(),
            "cancellation reaches descendants"
        );
        assert!(!parent.is_cancelled(), "cancellation must not flow upward");
        assert!(!clone.is_cancelled());
        assert!(!sibling.is_cancelled(), "siblings are independent");

        parent.cancel();
        assert!(clone.is_cancelled(), "clones share one signal");
        assert!(sibling.is_cancelled());
    }

    #[test]
    fn a_child_of_a_cancelled_parent_starts_cancelled() {
        let parent = CancellationToken::new();
        parent.cancel();
        assert!(parent.child_token().is_cancelled());
    }

    #[test]
    fn waiters_wake_on_cancellation_and_late_waiters_resolve_immediately() {
        let token = CancellationToken::new();
        let observed = Rc::new(Cell::new(0u32));

        for _ in 0..3 {
            let token = token.clone();
            let observed = Rc::clone(&observed);
            spawn(async move {
                token.cancelled().await;
                observed.set(observed.get() + 1);
            });
        }

        {
            let token = token.clone();
            queue_macrotask(move || token.cancel());
        }
        run();
        assert_eq!(observed.get(), 3, "every waiter should wake");

        // A waiter that arrives after cancellation must not hang.
        let late = Rc::new(Cell::new(false));
        {
            let flag = Rc::clone(&late);
            spawn(async move {
                token.cancelled().await;
                flag.set(true);
            });
        }
        run();
        assert!(late.get());
    }

    /// Cancelling runs user wakers, and those may touch the same token. The
    /// implementation collects wakers before waking precisely so this does not
    /// panic on a re-entrant borrow.
    #[test]
    fn cancelling_from_within_a_waiter_does_not_panic() {
        let token = CancellationToken::new();
        let reached = Rc::new(Cell::new(false));

        {
            let token = token.clone();
            let reached = Rc::clone(&reached);
            spawn(async move {
                token.cancelled().await;
                // Re-entrant: creates a child and cancels it while the outer
                // cancellation is still unwinding its waker list.
                let nested = token.child_token();
                nested.cancel();
                token.cancel();
                reached.set(true);
            });
        }

        {
            let token = token.clone();
            queue_macrotask(move || token.cancel());
        }
        run();
        assert!(reached.get());
    }

    /// The advertised pattern is one long-lived token raced against work in a
    /// loop, so a registration that outlives its future is an unbounded leak:
    /// the slot itself, and the task shared state each retained waker pins.
    #[test]
    fn dropping_a_waiter_releases_its_registration() {
        let token = CancellationToken::new();

        for _ in 0..1_000 {
            assert!(poll_once_and_drop(&token).is_pending());
        }

        let wakers = token.state.wakers.borrow();
        assert_eq!(
            wakers.slots.len(),
            0,
            "every dropped waiter should have surrendered its slot"
        );
        assert_eq!(wakers.free.len(), 0, "and the free list with it");
    }

    /// Two live waiters must keep two distinct slots; only the one that is
    /// dropped may be reclaimed.
    #[test]
    fn concurrent_waiters_keep_independent_slots() {
        let token = CancellationToken::new();
        let mut context = Context::from_waker(Waker::noop());

        let mut first = Box::pin(token.cancelled());
        assert!(first.as_mut().poll(&mut context).is_pending());
        {
            let mut second = pin!(token.cancelled());
            assert!(second.as_mut().poll(&mut context).is_pending());
            assert_eq!(token.state.wakers.borrow().slots.len(), 2);
        }
        assert_eq!(
            token.state.wakers.borrow().slots.len(),
            2,
            "the surviving waiter still owns its slot"
        );
        assert_eq!(token.state.wakers.borrow().free.len(), 1);

        drop(first);
        assert_eq!(token.state.wakers.borrow().slots.len(), 0);
    }

    /// Cancelling must release the waker storage, not just its contents: a
    /// drained `Vec` keeps the high-water-mark allocation for the life of the
    /// token, which is exactly as long as the leak would have lasted.
    #[test]
    fn cancelling_releases_the_waker_storage() {
        let token = CancellationToken::new();
        let mut futures = Vec::new();
        let mut context = Context::from_waker(Waker::noop());
        for _ in 0..64 {
            let mut future = Box::pin(token.cancelled());
            assert!(future.as_mut().poll(&mut context).is_pending());
            futures.push(future);
        }
        assert_eq!(token.state.wakers.borrow().slots.len(), 64);

        token.cancel();
        assert_eq!(token.state.wakers.borrow().slots.capacity(), 0);

        // Waiters resolve and drop after the drain; that must not panic or
        // resurrect storage.
        for mut future in futures {
            assert!(future.as_mut().poll(&mut context).is_ready());
        }
        assert_eq!(token.state.wakers.borrow().slots.capacity(), 0);
    }

    /// A `Weak` left in the parent keeps the dropped child's allocation
    /// reserved, so a per-request child token would leak against a
    /// process-lifetime root.
    #[test]
    fn dropped_children_do_not_accumulate_in_the_parent() {
        let parent = CancellationToken::new();
        for _ in 0..1_000 {
            drop(parent.child_token());
        }
        let retained = parent.state.children.borrow().len();
        assert!(
            retained <= 8,
            "expected dead children to be compacted away, found {retained}"
        );
    }

    /// Compaction must not drop children that are still held.
    #[test]
    fn compaction_keeps_live_children() {
        let parent = CancellationToken::new();
        let live: Vec<_> = (0..16).map(|_| parent.child_token()).collect();
        for _ in 0..1_000 {
            drop(parent.child_token());
        }

        parent.cancel();
        assert!(live.iter().all(CancellationToken::is_cancelled));
    }
}
