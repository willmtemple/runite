use std::cell::{Cell, UnsafeCell};
use std::future::Future;
use std::marker::PhantomData;
use std::rc::Rc;

use super::Notify;

#[derive(Clone, Copy, Eq, PartialEq)]
enum State {
    Empty,
    Initializing,
    Ready,
}

/// A single-threaded async cell initialized at most once.
///
/// The first caller to [`get_or_init`](Self::get_or_init) runs the initializer.
/// Concurrent callers wait for that initializer and then receive the same
/// reference.
///
/// # Differences from other once cells
///
/// Initializer futures are local to one runtime thread and may be `!Send`,
/// unlike `tokio::sync::OnceCell` on Tokio's multithreaded runtime.
/// `std::sync::OnceLock` is synchronous and blocks threads; this type awaits
/// local async initialization without atomics.
///
/// # Examples
///
/// ```
/// use std::cell::{Cell, RefCell};
/// use std::rc::Rc;
///
/// use runite::sync::OnceCell;
///
/// let cell = Rc::new(OnceCell::new());
/// let init_count = Rc::new(Cell::new(0));
/// let observed = Rc::new(RefCell::new(Vec::new()));
///
/// for _ in 0..2 {
///     runite::spawn({
///         let cell = Rc::clone(&cell);
///         let init_count = Rc::clone(&init_count);
///         let observed = Rc::clone(&observed);
///         async move {
///             let value = cell
///                 .get_or_init(|| {
///                     let init_count = Rc::clone(&init_count);
///                     async move {
///                         init_count.set(init_count.get() + 1);
///                         runite::yield_now().await;
///                         42
///                     }
///                 })
///                 .await;
///             observed.borrow_mut().push(*value);
///         }
///     });
/// }
///
/// runite::run();
///
/// assert_eq!(init_count.get(), 1);
/// assert_eq!(&*observed.borrow(), &[42, 42]);
/// assert_eq!(cell.get(), Some(&42));
/// ```
pub struct OnceCell<T> {
    state: Cell<State>,
    notify: Notify,
    _not_send_sync: PhantomData<Rc<()>>,
    value: UnsafeCell<Option<T>>,
}

impl<T> OnceCell<T> {
    /// Creates an empty cell.
    pub fn new() -> Self {
        Self {
            state: Cell::new(State::Empty),
            notify: Notify::new(),
            _not_send_sync: PhantomData,
            value: UnsafeCell::new(None),
        }
    }

    /// Returns the initialized value, if any.
    ///
    /// Returns [`None`] while the cell is empty or while an asynchronous
    /// initializer is still running. If an initializer is cancelled or panics,
    /// the cell returns to the empty state and `get()` continues to return
    /// `None` until another initializer completes.
    pub fn get(&self) -> Option<&T> {
        if self.state.get() != State::Ready {
            return None;
        }

        // SAFETY: once the state is `Ready`, the option contains a value and is
        // never mutated again. `OnceCell` is `!Send`/`!Sync`, so this shared
        // reference cannot race with initialization on another thread.
        unsafe { (&*self.value.get()).as_ref() }
    }

    /// Returns the cell value, initializing it with `f` if needed.
    ///
    /// Only one caller runs an initializer at a time. Other callers that arrive
    /// while initialization is in progress wait until the value is ready.
    ///
    /// # Cancellation and panics
    ///
    /// If the initializer future is dropped before completing (for example, the
    /// awaiting task is aborted) or panics, the cell is reset to its empty state
    /// and a waiting caller is woken to retry initialization with its own `f`.
    /// The cell never becomes permanently stuck because an initializer did not
    /// finish.
    pub async fn get_or_init<F, Fut>(&self, f: F) -> &T
    where
        F: FnOnce() -> Fut,
        Fut: Future<Output = T>,
    {
        let mut initializer = Some(f);

        loop {
            match self.state.get() {
                State::Ready => return self.ready_value(),
                State::Empty => {
                    self.state.set(State::Initializing);

                    // Reset the cell and wake the next waiter if the initializer
                    // does not run to completion (cancellation or panic).
                    let mut guard = InitGuard {
                        cell: self,
                        armed: true,
                    };
                    let f = initializer
                        .take()
                        .expect("initializer is available on the empty path");
                    let value = f().await;

                    // SAFETY: this branch is entered by the single active
                    // initializer after it changed state from `Empty` to
                    // `Initializing`. No references to the inner value exist
                    // before `Ready`, and after `Ready` the option is not
                    // mutated again.
                    unsafe {
                        *self.value.get() = Some(value);
                    }
                    self.state.set(State::Ready);
                    guard.armed = false;
                    self.notify.notify_waiters();
                    return self.ready_value();
                }
                State::Initializing => {
                    self.notify.notified().await;
                    // Re-check the state on the next loop iteration. If the
                    // active initializer was cancelled, the state is now `Empty`
                    // and this caller may take over (it still owns `f`).
                }
            }
        }
    }

    fn ready_value(&self) -> &T {
        self.get()
            .expect("OnceCell ready state must contain an initialized value")
    }
}

/// Restores an [`OnceCell`] to its empty state if an in-progress initializer is
/// dropped or unwinds before completing, waking a waiter to retry.
struct InitGuard<'a, T> {
    cell: &'a OnceCell<T>,
    armed: bool,
}

impl<T> Drop for InitGuard<'_, T> {
    fn drop(&mut self) {
        if self.armed {
            self.cell.state.set(State::Empty);
            self.cell.notify.notify_waiters();
        }
    }
}

impl<T> Default for OnceCell<T> {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::cell::{Cell, RefCell};

    use crate::{run, spawn, yield_now};

    #[test]
    fn fast_path_initializes_and_returns_value() {
        let cell = Rc::new(OnceCell::new());
        let observed = Rc::new(Cell::new(0));

        spawn({
            let cell = Rc::clone(&cell);
            let observed = Rc::clone(&observed);
            async move {
                let value = cell.get_or_init(|| async { 42 }).await;
                observed.set(*value);
            }
        });

        run();

        assert_eq!(observed.get(), 42);
        assert_eq!(cell.get(), Some(&42));
    }

    #[test]
    fn racing_callers_run_one_initializer() {
        let cell = Rc::new(OnceCell::new());
        let init_count = Rc::new(Cell::new(0));
        let observed = Rc::new(RefCell::new(Vec::new()));

        for _ in 0..2 {
            spawn({
                let cell = Rc::clone(&cell);
                let init_count = Rc::clone(&init_count);
                let observed = Rc::clone(&observed);
                async move {
                    let value = cell
                        .get_or_init(|| {
                            let init_count = Rc::clone(&init_count);
                            async move {
                                init_count.set(init_count.get() + 1);
                                yield_now().await;
                                99
                            }
                        })
                        .await;
                    observed.borrow_mut().push(*value);
                }
            });
        }

        run();

        assert_eq!(init_count.get(), 1);
        assert_eq!(&*observed.borrow(), &[99, 99]);
    }

    #[test]
    fn cancelled_initializer_lets_next_caller_initialize() {
        let cell = Rc::new(OnceCell::new());
        let observed = Rc::new(Cell::new(0));

        // Task A begins initializing but never completes; it is aborted below.
        let initializing = spawn({
            let cell = Rc::clone(&cell);
            async move {
                cell.get_or_init(|| async {
                    std::future::pending::<()>().await;
                    1
                })
                .await;
            }
        });

        // Task B aborts the stuck initializer, then initializes the cell itself.
        spawn({
            let cell = Rc::clone(&cell);
            let observed = Rc::clone(&observed);
            async move {
                yield_now().await;
                initializing.abort();
                let value = cell.get_or_init(|| async { 42 }).await;
                observed.set(*value);
            }
        });

        run();

        assert_eq!(observed.get(), 42);
        assert_eq!(cell.get(), Some(&42));
    }
}
