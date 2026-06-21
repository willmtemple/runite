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
pub struct OnceCell<T> {
    state: Cell<State>,
    notify: Notify,
    _not_send_sync: PhantomData<Rc<()>>,
    value: UnsafeCell<Option<T>>,
}

impl<T> OnceCell<T> {
    pub fn new() -> Self {
        Self {
            state: Cell::new(State::Empty),
            notify: Notify::new(),
            _not_send_sync: PhantomData,
            value: UnsafeCell::new(None),
        }
    }

    pub fn get(&self) -> Option<&T> {
        if self.state.get() != State::Ready {
            return None;
        }

        // SAFETY: once the state is `Ready`, the option contains a value and is
        // never mutated again. `OnceCell` is `!Send`/`!Sync`, so this shared
        // reference cannot race with initialization on another thread.
        unsafe { (&*self.value.get()).as_ref() }
    }

    pub async fn get_or_init<F, Fut>(&self, f: F) -> &T
    where
        F: FnOnce() -> Fut,
        Fut: Future<Output = T>,
    {
        if self.state.get() == State::Ready {
            return self.ready_value();
        }

        if self.state.get() == State::Empty {
            self.state.set(State::Initializing);
            let value = f().await;

            // SAFETY: this branch is entered by the single initializer after it
            // changed state from `Empty` to `Initializing`. No references to the
            // inner value exist before `Ready`, and after `Ready` the option is
            // not mutated again.
            unsafe {
                *self.value.get() = Some(value);
            }
            self.state.set(State::Ready);
            self.notify.notify_waiters();
            return self.ready_value();
        }

        loop {
            self.notify.notified().await;
            if self.state.get() == State::Ready {
                return self.ready_value();
            }
        }
    }

    fn ready_value(&self) -> &T {
        self.get()
            .expect("OnceCell ready state must contain an initialized value")
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

    use crate::platform::current::runtime::{queue_future, run, yield_now};

    #[test]
    fn fast_path_initializes_and_returns_value() {
        let cell = Rc::new(OnceCell::new());
        let observed = Rc::new(Cell::new(0));

        queue_future({
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
            queue_future({
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
}
