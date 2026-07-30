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
    /// Waiters registered by [`CancellationToken::cancelled`], each tagged so
    /// that a dropped future can find and remove its own. Without the tag a
    /// `select!` arm — which polls and drops one of these every iteration —
    /// would leave a `Waker` behind on each pass, growing this vector for the
    /// life of the token and lengthening every later `cancel`.
    wakers: RefCell<Vec<(u64, Waker)>>,
    /// Source of the tags above. Never reused, so a slot freed by one waiter
    /// cannot be mistaken for another's.
    next_waiter: Cell<u64>,
    /// Children are held weakly: a token that nobody kept must not be pinned
    /// alive by its parent, and cancelling a parent should not resurrect one.
    children: RefCell<Vec<Weak<TokenState>>>,
}

impl TokenState {
    fn new() -> Rc<Self> {
        Rc::new(Self {
            cancelled: Cell::new(false),
            wakers: RefCell::new(Vec::new()),
            next_waiter: Cell::new(0),
            children: RefCell::new(Vec::new()),
        })
    }

    /// Marks this subtree cancelled and returns every waker to notify.
    ///
    /// Wakers are collected rather than woken in place: waking runs user code,
    /// which may clone the token, create a child, or cancel something else, and
    /// doing that while `wakers` or `children` is borrowed would panic. The
    /// borrows are all released before any waker runs.
    ///
    /// The subtree is walked with an explicit stack rather than by recursing.
    /// Depth here is the application's, not ours: nothing stops a program from
    /// deriving a child per layer of a deeply nested structure. Recursing once
    /// per generation overflowed the stack at around ten thousand, which aborts
    /// the process — not a panic a caller can catch, and reachable from safe
    /// code through `child_token` and `cancel` alone. An explicit stack makes
    /// the depth bounded by the heap instead.
    fn collect_cancellation(self: &Rc<Self>, wakers: &mut Vec<Waker>) {
        let mut pending = vec![Rc::clone(self)];
        while let Some(state) = pending.pop() {
            if state.cancelled.replace(true) {
                continue;
            }
            wakers.extend(
                std::mem::take(&mut *state.wakers.borrow_mut())
                    .into_iter()
                    .map(|(_, waker)| waker),
            );
            let children = std::mem::take(&mut *state.children.borrow_mut());
            pending.extend(children.into_iter().filter_map(|child| child.upgrade()));
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

    /// How many child slots the parent is holding, live or dead.
    #[cfg(test)]
    fn live_child_slots(&self) -> usize {
        self.state.children.borrow().len()
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
            // Drop the entries whose tokens are gone before adding another.
            // A `Weak` costs nothing to keep alive, but the slot holding it
            // does, and the shape this type is built for — a long-lived parent
            // handing a child to each of many short-lived tasks — would
            // otherwise grow this vector for the life of the parent and make
            // every later `cancel` walk the accumulated corpses.
            //
            // Pruning on push rather than on drop keeps the cost amortised and
            // keeps `Drop` free of borrowing, which matters because cancelling
            // runs user code that may create children.
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
    /// returned future takes its own registration with it and leaves every
    /// other waiter untouched, and a later call observes the same state. That
    /// is what makes the `select!` shape below sustainable — it drops one of
    /// these on every iteration, and a registration left behind by each would
    /// grow the token's waiter list for the life of the token.
    ///
    /// # Examples
    ///
    /// ```
    /// # async fn example(
    /// #     token: runite::sync::CancellationToken,
    /// #     mut rx: runite::channel::mpsc::Receiver<u32>,
    /// # ) {
    /// loop {
    ///     runite::select! {
    ///         _ = token.cancelled() => break,
    ///         message = rx.recv() => { let _ = message; }
    ///     }
    /// }
    /// # }
    /// ```
    pub async fn cancelled(&self) {
        /// Removes this waiter's registration when the future is dropped,
        /// whether it was dropped mid-wait or after resolving.
        struct Registration<'state> {
            state: &'state TokenState,
            waiter: Option<u64>,
        }

        impl Drop for Registration<'_> {
            fn drop(&mut self) {
                let Some(waiter) = self.waiter else {
                    return;
                };
                let mut wakers = self.state.wakers.borrow_mut();
                if let Some(index) = wakers.iter().position(|(slot, _)| *slot == waiter) {
                    wakers.swap_remove(index);
                }
            }
        }

        let mut registration = Registration {
            state: &self.state,
            waiter: None,
        };
        poll_fn(|context| {
            if self.state.cancelled.get() {
                return Poll::Ready(());
            }
            let mut wakers = self.state.wakers.borrow_mut();
            match registration.waiter {
                None => {
                    let waiter = self.state.next_waiter.get();
                    self.state.next_waiter.set(waiter + 1);
                    registration.waiter = Some(waiter);
                    wakers.push((waiter, context.waker().clone()));
                }
                // Re-polled, possibly by a different task than the one that
                // registered — a `select!` arm re-created each iteration is
                // exactly that. The stored waker has to be the current one or
                // the cancellation reaches nobody.
                Some(waiter) => {
                    if let Some((_, stored)) = wakers.iter_mut().find(|(slot, _)| *slot == waiter)
                        && !stored.will_wake(context.waker())
                    {
                        *stored = context.waker().clone();
                    }
                }
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
    use std::cell::Cell;
    use std::future::Future;
    use std::pin::pin;
    use std::rc::Rc;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::task::{Context, Poll, Wake, Waker};

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

    /// Polling a `cancelled()` future and dropping it is what a `select!` arm
    /// does on every iteration, so the registration it made has to go with it.
    /// Retaining them would grow the token's waiter list for the life of the
    /// token and lengthen every later `cancel`.
    #[test]
    fn a_dropped_waiter_leaves_no_registration_behind() {
        let token = CancellationToken::new();
        let waker = Waker::noop();
        let mut context = Context::from_waker(waker);

        for _ in 0..1_000 {
            let mut waiting = pin!(token.cancelled());
            assert_eq!(
                waiting.as_mut().poll(&mut context),
                Poll::Pending,
                "an uncancelled token does not resolve"
            );
            assert_eq!(
                token.state.wakers.borrow().len(),
                1,
                "a polled waiter registers exactly once"
            );
        }

        assert_eq!(
            token.state.wakers.borrow().len(),
            0,
            "every dropped waiter should have taken its registration with it"
        );
    }

    /// Deregistration must take the waiter's own slot and nobody else's: a
    /// dropped `select!` arm alongside a live waiter must leave that waiter
    /// registered *and* wakeable, which is the half a length check alone would
    /// not catch.
    #[test]
    fn dropping_one_waiter_leaves_the_others_registered() {
        struct Counting(AtomicUsize);

        impl Wake for Counting {
            fn wake(self: Arc<Self>) {
                self.0.fetch_add(1, Ordering::SeqCst);
            }
        }

        let token = CancellationToken::new();
        let kept = Arc::new(Counting(AtomicUsize::new(0)));
        let abandoned = Arc::new(Counting(AtomicUsize::new(0)));
        let kept_waker = Waker::from(Arc::clone(&kept));
        let abandoned_waker = Waker::from(Arc::clone(&abandoned));

        let mut survivor = pin!(token.cancelled());
        assert_eq!(
            survivor
                .as_mut()
                .poll(&mut Context::from_waker(&kept_waker)),
            Poll::Pending
        );
        {
            let mut discarded = pin!(token.cancelled());
            assert_eq!(
                discarded
                    .as_mut()
                    .poll(&mut Context::from_waker(&abandoned_waker)),
                Poll::Pending
            );
            assert_eq!(token.state.wakers.borrow().len(), 2);
        }
        assert_eq!(
            token.state.wakers.borrow().len(),
            1,
            "only the dropped waiter's registration should go"
        );

        token.cancel();
        assert_eq!(
            kept.0.load(Ordering::SeqCst),
            1,
            "the surviving waiter must still be woken"
        );
        assert_eq!(
            abandoned.0.load(Ordering::SeqCst),
            0,
            "a dropped waiter must not be woken"
        );
        assert_eq!(
            survivor
                .as_mut()
                .poll(&mut Context::from_waker(&kept_waker)),
            Poll::Ready(())
        );
    }

    /// A future polled again by a different task must leave the *current*
    /// waker registered. `select!` re-polls its arms, and a task that moved
    /// its work between polls would otherwise never be woken.
    #[test]
    fn a_repolled_waiter_registers_the_waker_it_was_last_polled_with() {
        struct Counting(AtomicUsize);

        impl Wake for Counting {
            fn wake(self: Arc<Self>) {
                self.0.fetch_add(1, Ordering::SeqCst);
            }
        }

        let token = CancellationToken::new();
        let first = Arc::new(Counting(AtomicUsize::new(0)));
        let second = Arc::new(Counting(AtomicUsize::new(0)));
        let first_waker = Waker::from(Arc::clone(&first));
        let second_waker = Waker::from(Arc::clone(&second));

        let mut waiting = pin!(token.cancelled());
        assert_eq!(
            waiting
                .as_mut()
                .poll(&mut Context::from_waker(&first_waker)),
            Poll::Pending
        );
        assert_eq!(
            waiting
                .as_mut()
                .poll(&mut Context::from_waker(&second_waker)),
            Poll::Pending
        );
        assert_eq!(
            token.state.wakers.borrow().len(),
            1,
            "a re-poll replaces the registration rather than adding one"
        );

        token.cancel();
        assert_eq!(
            second.0.load(Ordering::SeqCst),
            1,
            "the current waker wakes"
        );
        assert_eq!(
            first.0.load(Ordering::SeqCst),
            0,
            "the waker from the earlier poll is stale and must not be used"
        );
    }

    /// Cancelling a deep chain must not overflow the stack.
    ///
    /// Depth is the application's to choose — a child per layer of a nested
    /// structure is an ordinary use — and a stack overflow aborts the process
    /// rather than panicking, so no caller can recover from it. 20,000 is well
    /// past where the recursive version died.
    #[test]
    fn cancelling_a_deep_chain_does_not_overflow_the_stack() {
        let root = CancellationToken::new();
        let mut chain = vec![root.clone()];
        for _ in 0..20_000 {
            let next = chain.last().expect("chain is never empty").child_token();
            chain.push(next);
        }

        root.cancel();

        assert!(
            chain.last().expect("chain is never empty").is_cancelled(),
            "cancellation must reach the deepest descendant"
        );
    }

    /// A long-lived parent handing out short-lived children must not grow.
    ///
    /// This is the shape the type exists for — a subsystem giving each task its
    /// own child — so a slot retained per child would grow the parent's
    /// registry for the life of the program and lengthen every later `cancel`.
    #[test]
    fn a_parent_does_not_accumulate_dead_children() {
        let parent = CancellationToken::new();
        for _ in 0..10_000 {
            let child = parent.child_token();
            assert!(!child.is_cancelled());
        }
        let slots = parent.live_child_slots();
        assert!(
            slots < 128,
            "a parent handed out 10,000 short-lived children and kept {slots} slots"
        );
    }

    /// A `select!` arm is a fresh `cancelled()` future on every iteration, and
    /// the task polling it is the same one each time. The registration count
    /// must not track the number of iterations.
    #[test]
    fn a_select_loop_does_not_accumulate_registrations() {
        let token = CancellationToken::new();
        let deepest = Rc::new(Cell::new(0usize));

        let (sender, mut receiver) = crate::channel::mpsc::channel::<u32>(4);
        {
            let token = token.clone();
            let deepest = Rc::clone(&deepest);
            spawn(async move {
                for _ in 0..64u32 {
                    crate::select! {
                        _ = token.cancelled() => break,
                        message = receiver.recv() => {
                            if message.is_none() {
                                break;
                            }
                        }
                    }
                    deepest.set(deepest.get().max(token.state.wakers.borrow().len()));
                }
            });
        }

        spawn(async move {
            for value in 0..64u32 {
                sender.send(value).await.expect("receiver is alive");
            }
        });
        run();

        assert_eq!(
            deepest.get(),
            0,
            "each iteration's arm should deregister before the next one registers"
        );
        assert_eq!(token.state.wakers.borrow().len(), 0);
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
}
