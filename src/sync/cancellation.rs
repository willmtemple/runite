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
    wakers: RefCell<Vec<Waker>>,
    /// Children are held weakly: a token that nobody kept must not be pinned
    /// alive by its parent, and cancelling a parent should not resurrect one.
    children: RefCell<Vec<Weak<TokenState>>>,
}

impl TokenState {
    fn new() -> Rc<Self> {
        Rc::new(Self {
            cancelled: Cell::new(false),
            wakers: RefCell::new(Vec::new()),
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
        wakers.append(&mut self.wakers.borrow_mut());
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
            self.state.children.borrow_mut().push(Rc::downgrade(&child));
        }
        Self { state: child }
    }

    /// Waits until this token is cancelled.
    ///
    /// Resolves immediately if it already is. Cancel-safe: dropping the
    /// returned future deregisters nothing that another waiter depends on, and
    /// a later call observes the same state.
    ///
    /// # Examples
    ///
    /// ```
    /// # async fn example(token: runite::sync::CancellationToken) {
    /// token.cancelled().await;
    /// # }
    /// ```
    pub async fn cancelled(&self) {
        let mut registered = false;
        poll_fn(|context| {
            if self.state.cancelled.get() {
                return Poll::Ready(());
            }
            if !registered {
                registered = true;
                self.state.wakers.borrow_mut().push(context.waker().clone());
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
    use std::rc::Rc;

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
}
