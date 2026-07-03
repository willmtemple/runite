use core::fmt;
use core::future::Future;
use core::pin::Pin;
use core::task::Poll;

use crate::{JoinHandle, spawn};

/// Error returned by awaiting a join handle or [`JoinSet`] task.
///
/// Produced both by [`crate::task::BlockingJoinHandle`] (when a blocking-pool
/// worker exits without delivering a value) and by [`crate::JoinHandle`] or
/// [`JoinSet`] (when a queued future is aborted before it completes). A queued
/// task's join output is `Result<T, JoinError>`, so callers should handle these
/// errors when awaiting any join handle.
///
/// A queued future that **panics** while being polled resolves its join handle
/// to [`JoinError::Panicked`] rather than unwinding the event loop: runite
/// isolates task panics so one misbehaving task cannot take down the runtime
/// thread (the panic is still reported through the process panic hook).
///
/// Use [`JoinError::is_cancelled`], [`JoinError::is_aborted`], and
/// [`JoinError::is_panicked`] when the caller only needs to distinguish the
/// category.
///
/// # Examples
///
/// ```
/// use std::rc::Rc;
/// use std::cell::Cell;
///
/// let saw_abort = Rc::new(Cell::new(false));
/// let saw_abort_task = Rc::clone(&saw_abort);
///
/// runite::spawn(async move {
///     let mut set = runite::task::JoinSet::<()>::new();
///     set.spawn(async { std::future::pending::<()>().await });
///     set.abort_all();
///     let result = set.join_next().await.expect("aborted task should remain joinable");
///     saw_abort_task.set(result.expect_err("task should be aborted").is_aborted());
/// });
///
/// runite::run();
/// assert!(saw_abort.get());
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum JoinError {
    /// The worker exited without producing a value.
    ///
    /// This is used for blocking tasks whose result channel closes before the
    /// worker delivers a value, such as during runtime shutdown or panic
    /// unwinding.
    Cancelled,
    /// The task was aborted before it completed.
    ///
    /// This is returned by [`crate::JoinHandle`] when
    /// [`JoinHandle::abort`](crate::JoinHandle::abort), an
    /// [`AbortHandle`](crate::AbortHandle), or [`JoinSet::abort_all`] cancels
    /// the queued future.
    Aborted,
    /// The task panicked while being polled.
    ///
    /// runite catches panics that unwind out of a spawned future so the panic
    /// does not tear down the whole runtime thread; the joiner observes this
    /// variant instead. The panic itself is still reported through the process
    /// panic hook (message and, if enabled, backtrace on stderr). The panic
    /// payload is not carried on the error, so that [`JoinError`] can stay
    /// `Copy`.
    Panicked,
}

impl JoinError {
    /// Returns `true` if the task was aborted before completion.
    ///
    /// This is true only for [`JoinError::Aborted`].
    ///
    /// # Examples
    ///
    /// ```
    /// use std::rc::Rc;
    /// use std::cell::Cell;
    ///
    /// let observed = Rc::new(Cell::new(false));
    /// let observed_task = Rc::clone(&observed);
    ///
    /// runite::spawn(async move {
    ///     let mut set = runite::task::JoinSet::<()>::new();
    ///     set.spawn(async { std::future::pending::<()>().await });
    ///     set.abort_all();
    ///     let err = set.join_next().await.unwrap().unwrap_err();
    ///     observed_task.set(err.is_aborted());
    /// });
    ///
    /// runite::run();
    /// assert!(observed.get());
    /// ```
    pub fn is_aborted(&self) -> bool {
        matches!(self, JoinError::Aborted)
    }

    /// Returns `true` if a blocking-pool worker was cancelled without producing
    /// a value.
    ///
    /// This is true only for [`JoinError::Cancelled`].
    ///
    /// # Examples
    ///
    /// ```
    /// assert!(runite::task::JoinError::Cancelled.is_cancelled());
    /// assert!(!runite::task::JoinError::Aborted.is_cancelled());
    /// ```
    pub fn is_cancelled(&self) -> bool {
        matches!(self, JoinError::Cancelled)
    }

    /// Returns `true` if the task panicked while being polled.
    ///
    /// This is true only for [`JoinError::Panicked`].
    ///
    /// # Examples
    ///
    /// ```
    /// assert!(runite::task::JoinError::Panicked.is_panicked());
    /// assert!(!runite::task::JoinError::Aborted.is_panicked());
    /// ```
    pub fn is_panicked(&self) -> bool {
        matches!(self, JoinError::Panicked)
    }
}

impl fmt::Display for JoinError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            JoinError::Cancelled => f.write_str("blocking task was cancelled"),
            JoinError::Aborted => f.write_str("task was aborted"),
            JoinError::Panicked => f.write_str("task panicked"),
        }
    }
}

impl std::error::Error for JoinError {}

/// A collection of spawned local tasks that yields results as tasks complete.
///
/// `JoinSet` owns each task's [`JoinHandle`]. Dropping the set aborts any tasks
/// that are still running, making it a structured-concurrency primitive: child
/// tasks cannot silently outlive the owner unless [`detach_all`](Self::detach_all)
/// is called first.
///
/// Like all futures in this runtime, tasks in a `JoinSet` are `!Send` and stay on
/// the runtime thread where they were spawned. This differs from Tokio's
/// multithreaded `JoinSet`: runite owns local tasks on one event-loop thread,
/// dropping the set aborts remaining tasks, and dropping an individual
/// [`JoinHandle`] detaches that task instead of cancelling it.
///
/// # Examples
///
/// ```
/// use std::cell::RefCell;
/// use std::rc::Rc;
///
/// let results = Rc::new(RefCell::new(Vec::new()));
/// let results_task = Rc::clone(&results);
///
/// runite::spawn(async move {
///     let mut set = runite::task::JoinSet::new();
///     set.spawn(async { 1usize });
///     set.spawn(async { 2usize });
///
///     while let Some(result) = set.join_next().await {
///         results_task.borrow_mut().push(result.expect("task should finish"));
///     }
/// });
///
/// runite::run();
/// results.borrow_mut().sort_unstable();
/// assert_eq!(&*results.borrow(), &[1, 2]);
/// ```
pub struct JoinSet<T> {
    handles: Vec<JoinHandle<T>>,
}

impl<T> JoinSet<T> {
    /// Creates an empty `JoinSet`.
    ///
    /// # Examples
    ///
    /// ```
    /// use std::rc::Rc;
    /// use std::cell::Cell;
    ///
    /// let was_empty = Rc::new(Cell::new(false));
    /// let was_empty_task = Rc::clone(&was_empty);
    ///
    /// runite::spawn(async move {
    ///     let set = runite::task::JoinSet::<usize>::new();
    ///     was_empty_task.set(set.is_empty());
    /// });
    ///
    /// runite::run();
    /// assert!(was_empty.get());
    /// ```
    pub fn new() -> Self {
        Self {
            handles: Vec::new(),
        }
    }

    /// Spawns `future` on the current runtime thread and adds it to the set.
    ///
    /// The task is scheduled through [`crate::spawn`] as a microtask. Its output
    /// can later be retrieved by awaiting [`join_next`](Self::join_next).
    ///
    /// # Examples
    ///
    /// ```
    /// use std::rc::Rc;
    /// use std::cell::Cell;
    ///
    /// let result = Rc::new(Cell::new(0));
    /// let result_task = Rc::clone(&result);
    ///
    /// runite::spawn(async move {
    ///     let mut set = runite::task::JoinSet::new();
    ///     set.spawn(async { 42 });
    ///     result_task.set(set.join_next().await.unwrap().unwrap());
    /// });
    ///
    /// runite::run();
    /// assert_eq!(result.get(), 42);
    /// ```
    pub fn spawn<F>(&mut self, future: F)
    where
        F: Future<Output = T> + 'static,
        T: 'static,
    {
        self.handles.push(spawn(future));
    }

    /// Waits for the next task in the set to complete.
    ///
    /// Resolves to `Some(Ok(value))` for a completed task,
    /// `Some(Err(JoinError::Aborted))` for an aborted task, or `None` when the
    /// set is empty. `abort_all` keeps handles in the set, so aborted tasks are
    /// reported by subsequent calls to `join_next`. `detach_all` removes handles,
    /// so detached tasks will not be reported.
    ///
    /// Each call returns one ready task result. If several tasks are ready at
    /// once, selection follows the set's internal scan order and is not a stable
    /// completion-order guarantee.
    ///
    /// # Performance
    ///
    /// This implementation linearly scans stored handles and registers the same
    /// waker with every pending task; smarter ready-queue wake bookkeeping is a
    /// follow-up optimization.
    ///
    /// # Examples
    ///
    /// ```
    /// use std::rc::Rc;
    /// use std::cell::Cell;
    ///
    /// let result = Rc::new(Cell::new(None));
    /// let result_task = Rc::clone(&result);
    ///
    /// runite::spawn(async move {
    ///     let mut set = runite::task::JoinSet::new();
    ///     set.spawn(async { "done" });
    ///     result_task.set(Some(set.join_next().await.unwrap().unwrap()));
    /// });
    ///
    /// runite::run();
    /// assert_eq!(result.get(), Some("done"));
    /// ```
    pub fn join_next(&mut self) -> impl Future<Output = Option<Result<T, JoinError>>> + '_ {
        std::future::poll_fn(|cx| {
            if self.handles.is_empty() {
                return Poll::Ready(None);
            }

            // 0.1 keeps this deliberately simple: scan every handle and let
            // each pending task store this future's waker. A ready queue keyed
            // by task wakeups would avoid the linear scan in a follow-up.
            for index in 0..self.handles.len() {
                match Pin::new(&mut self.handles[index]).poll(cx) {
                    Poll::Ready(result) => {
                        self.handles.swap_remove(index);
                        return Poll::Ready(Some(result));
                    }
                    Poll::Pending => {}
                }
            }

            Poll::Pending
        })
    }

    /// Aborts all tasks currently owned by the set.
    ///
    /// Aborted handles remain in the set. Drain them with
    /// [`join_next`](Self::join_next) to observe their
    /// [`JoinError::Aborted`] results.
    ///
    /// # Examples
    ///
    /// ```
    /// use std::rc::Rc;
    /// use std::cell::Cell;
    ///
    /// let aborted = Rc::new(Cell::new(false));
    /// let aborted_task = Rc::clone(&aborted);
    ///
    /// runite::spawn(async move {
    ///     let mut set = runite::task::JoinSet::<()>::new();
    ///     set.spawn(async { std::future::pending::<()>().await });
    ///     set.abort_all();
    ///     let err = set.join_next().await.unwrap().unwrap_err();
    ///     aborted_task.set(err.is_aborted());
    /// });
    ///
    /// runite::run();
    /// assert!(aborted.get());
    /// ```
    pub fn abort_all(&mut self) {
        for handle in &self.handles {
            handle.abort();
        }
    }

    /// Detaches all tasks from the set without aborting them.
    ///
    /// After this call the set is empty, and the detached tasks continue running
    /// to completion in the background. Dropping the set after `detach_all` will
    /// not abort those tasks.
    ///
    /// # Examples
    ///
    /// ```
    /// use std::rc::Rc;
    /// use std::cell::Cell;
    ///
    /// let completed = Rc::new(Cell::new(false));
    /// let completed_task = Rc::clone(&completed);
    ///
    /// runite::spawn(async move {
    ///     let mut set = runite::task::JoinSet::new();
    ///     set.spawn(async move { completed_task.set(true); });
    ///     set.detach_all();
    /// });
    ///
    /// runite::run();
    /// assert!(completed.get());
    /// ```
    pub fn detach_all(&mut self) {
        self.handles.clear();
    }

    /// Returns the number of task handles currently owned by the set.
    ///
    /// # Examples
    ///
    /// ```
    /// use std::rc::Rc;
    /// use std::cell::Cell;
    ///
    /// let len = Rc::new(Cell::new(0));
    /// let len_task = Rc::clone(&len);
    ///
    /// runite::spawn(async move {
    ///     let mut set = runite::task::JoinSet::new();
    ///     set.spawn(async { 1 });
    ///     len_task.set(set.len());
    /// });
    ///
    /// runite::run();
    /// assert_eq!(len.get(), 1);
    /// ```
    pub fn len(&self) -> usize {
        self.handles.len()
    }

    /// Returns `true` when the set owns no task handles.
    ///
    /// # Examples
    ///
    /// ```
    /// use std::rc::Rc;
    /// use std::cell::Cell;
    ///
    /// let empty = Rc::new(Cell::new(false));
    /// let empty_task = Rc::clone(&empty);
    ///
    /// runite::spawn(async move {
    ///     let set = runite::task::JoinSet::<usize>::new();
    ///     empty_task.set(set.is_empty());
    /// });
    ///
    /// runite::run();
    /// assert!(empty.get());
    /// ```
    pub fn is_empty(&self) -> bool {
        self.handles.is_empty()
    }
}

impl<T> Default for JoinSet<T> {
    fn default() -> Self {
        Self::new()
    }
}

impl<T> Drop for JoinSet<T> {
    fn drop(&mut self) {
        self.abort_all();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{run, spawn};
    use std::cell::{Cell, RefCell};
    use std::rc::Rc;

    #[test]
    fn join_next_returns_results_as_tasks_complete() {
        let results = Rc::new(RefCell::new(Vec::new()));
        let results_task = Rc::clone(&results);

        spawn(async move {
            let mut set = JoinSet::new();
            set.spawn(async { 3 });
            set.spawn(async { 1 });
            set.spawn(async { 2 });

            while let Some(result) = set.join_next().await {
                results_task
                    .borrow_mut()
                    .push(result.expect("task should complete"));
            }
        });

        run();

        results.borrow_mut().sort_unstable();
        assert_eq!(&*results.borrow(), &[1, 2, 3]);
    }

    #[test]
    fn join_next_returns_none_when_empty() {
        let observed = Rc::new(Cell::new(false));
        let observed_task = Rc::clone(&observed);

        spawn(async move {
            let mut set = JoinSet::<usize>::new();
            observed_task.set(set.join_next().await.is_none());
        });

        run();

        assert!(observed.get());
    }

    #[test]
    fn abort_all_surfaces_aborted_join_errors() {
        let aborted = Rc::new(Cell::new(0));
        let aborted_task = Rc::clone(&aborted);

        spawn(async move {
            let mut set = JoinSet::<usize>::new();
            set.spawn(async { std::future::pending::<usize>().await });
            set.spawn(async { std::future::pending::<usize>().await });

            set.abort_all();

            let mut count = 0;
            while let Some(result) = set.join_next().await {
                assert!(result.expect_err("task should be aborted").is_aborted());
                count += 1;
            }
            aborted_task.set(count);
        });

        run();

        assert_eq!(aborted.get(), 2);
    }

    #[test]
    fn drop_aborts_running_tasks() {
        let completed = Rc::new(Cell::new(false));
        let completed_task = Rc::clone(&completed);

        spawn(async move {
            let mut set = JoinSet::new();
            set.spawn(async move {
                completed_task.set(true);
            });
            drop(set);
        });

        run();

        assert!(!completed.get());
    }
}
