//! Async control-flow macros.
//!
//! These macros are intentionally declarative so they can live in
//! `runite` without introducing a dependency cycle with the proc-macro
//! crate. They poll all child futures on the current task and do not require
//! `Send`, matching the runtime's single-threaded future model.

/// Awaits multiple futures concurrently on the current task.
///
/// The futures are polled in lexical order whenever the parent future is
/// polled. The macro resolves once every future has completed, yielding a tuple
/// containing each output in the same order as the inputs.
#[macro_export]
macro_rules! join {
    () => {
        ()
    };
    ($($future:expr),+ $(,)?) => {
        $crate::join!(
            @collect
            []
            [
                (__runite_join_f0 __runite_join_done0 __runite_join_out0)
                (__runite_join_f1 __runite_join_done1 __runite_join_out1)
                (__runite_join_f2 __runite_join_done2 __runite_join_out2)
                (__runite_join_f3 __runite_join_done3 __runite_join_out3)
                (__runite_join_f4 __runite_join_done4 __runite_join_out4)
                (__runite_join_f5 __runite_join_done5 __runite_join_out5)
                (__runite_join_f6 __runite_join_done6 __runite_join_out6)
                (__runite_join_f7 __runite_join_done7 __runite_join_out7)
                (__runite_join_f8 __runite_join_done8 __runite_join_out8)
                (__runite_join_f9 __runite_join_done9 __runite_join_out9)
                (__runite_join_f10 __runite_join_done10 __runite_join_out10)
                (__runite_join_f11 __runite_join_done11 __runite_join_out11)
                (__runite_join_f12 __runite_join_done12 __runite_join_out12)
                (__runite_join_f13 __runite_join_done13 __runite_join_out13)
                (__runite_join_f14 __runite_join_done14 __runite_join_out14)
                (__runite_join_f15 __runite_join_done15 __runite_join_out15)
            ]
            [$($future),+]
        )
    };
    (@collect
        [$(($future_var:ident $done_var:ident $out_var:ident $future:expr))*]
        [($next_future_var:ident $next_done_var:ident $next_out_var:ident) $($names:tt)*]
        [$next_future:expr $(, $remaining_future:expr)*]
    ) => {
        $crate::join!(
            @collect
            [$(($future_var $done_var $out_var $future))* ($next_future_var $next_done_var $next_out_var $next_future)]
            [$($names)*]
            [$($remaining_future),*]
        )
    };
    (@collect
        [$(($future_var:ident $done_var:ident $out_var:ident $future:expr))+]
        [$($names:tt)*]
        []
    ) => {
        async move {
            $(
                let mut $future_var = ::core::pin::pin!($future);
                let mut $done_var = false;
                let mut $out_var = ::core::option::Option::None;
            )+

            ::core::future::poll_fn(move |__runite_cx| {
                $(
                    if !$done_var {
                        match ::core::future::Future::poll($future_var.as_mut(), __runite_cx) {
                            ::core::task::Poll::Ready(__runite_value) => {
                                $out_var = ::core::option::Option::Some(__runite_value);
                                $done_var = true;
                            }
                            ::core::task::Poll::Pending => {}
                        }
                    }
                )+

                if true $(&& $done_var)+ {
                    ::core::task::Poll::Ready(($(
                        $out_var
                            .take()
                            .expect("join! output should be present when marked done"),
                    )+))
                } else {
                    ::core::task::Poll::Pending
                }
            })
            .await
        }
        .await
    };
}

/// Awaits multiple `Result`-returning futures concurrently on the current task.
///
/// The macro resolves to the first error observed in lexical polling order, or
/// to `Ok((...))` once every future has completed successfully.
#[macro_export]
macro_rules! try_join {
    () => {
        ::core::result::Result::Ok(())
    };
    ($($future:expr),+ $(,)?) => {
        $crate::try_join!(
            @collect
            []
            [
                (__runite_try_join_f0 __runite_try_join_done0 __runite_try_join_out0)
                (__runite_try_join_f1 __runite_try_join_done1 __runite_try_join_out1)
                (__runite_try_join_f2 __runite_try_join_done2 __runite_try_join_out2)
                (__runite_try_join_f3 __runite_try_join_done3 __runite_try_join_out3)
                (__runite_try_join_f4 __runite_try_join_done4 __runite_try_join_out4)
                (__runite_try_join_f5 __runite_try_join_done5 __runite_try_join_out5)
                (__runite_try_join_f6 __runite_try_join_done6 __runite_try_join_out6)
                (__runite_try_join_f7 __runite_try_join_done7 __runite_try_join_out7)
                (__runite_try_join_f8 __runite_try_join_done8 __runite_try_join_out8)
                (__runite_try_join_f9 __runite_try_join_done9 __runite_try_join_out9)
                (__runite_try_join_f10 __runite_try_join_done10 __runite_try_join_out10)
                (__runite_try_join_f11 __runite_try_join_done11 __runite_try_join_out11)
                (__runite_try_join_f12 __runite_try_join_done12 __runite_try_join_out12)
                (__runite_try_join_f13 __runite_try_join_done13 __runite_try_join_out13)
                (__runite_try_join_f14 __runite_try_join_done14 __runite_try_join_out14)
                (__runite_try_join_f15 __runite_try_join_done15 __runite_try_join_out15)
            ]
            [$($future),+]
        )
    };
    (@collect
        [$(($future_var:ident $done_var:ident $out_var:ident $future:expr))*]
        [($next_future_var:ident $next_done_var:ident $next_out_var:ident) $($names:tt)*]
        [$next_future:expr $(, $remaining_future:expr)*]
    ) => {
        $crate::try_join!(
            @collect
            [$(($future_var $done_var $out_var $future))* ($next_future_var $next_done_var $next_out_var $next_future)]
            [$($names)*]
            [$($remaining_future),*]
        )
    };
    (@collect
        [$(($future_var:ident $done_var:ident $out_var:ident $future:expr))+]
        [$($names:tt)*]
        []
    ) => {
        async move {
            $(
                let mut $future_var = ::core::pin::pin!($future);
                let mut $done_var = false;
                let mut $out_var = ::core::option::Option::None;
            )+

            ::core::future::poll_fn(move |__runite_cx| {
                $(
                    if !$done_var {
                        match ::core::future::Future::poll($future_var.as_mut(), __runite_cx) {
                            ::core::task::Poll::Ready(::core::result::Result::Ok(__runite_value)) => {
                                $out_var = ::core::option::Option::Some(__runite_value);
                                $done_var = true;
                            }
                            ::core::task::Poll::Ready(::core::result::Result::Err(__runite_error)) => {
                                return ::core::task::Poll::Ready(
                                    ::core::result::Result::Err(__runite_error),
                                );
                            }
                            ::core::task::Poll::Pending => {}
                        }
                    }
                )+

                if true $(&& $done_var)+ {
                    ::core::task::Poll::Ready(::core::result::Result::Ok(($(
                        $out_var
                            .take()
                            .expect("try_join! output should be present when marked done"),
                    )+)))
                } else {
                    ::core::task::Poll::Pending
                }
            })
            .await
        }
        .await
    };
}

#[doc(hidden)]
pub use runite_proc_macros::select as __select;

/// Resolves with the handler for the first enabled future that becomes ready.
///
/// By default, the first arm polled rotates between invocations and after each
/// pending poll, preventing an always-ready arm from permanently starving its
/// peers. Add `biased;` at the start of the invocation to poll in lexical order
/// every time:
///
/// <div class="warning">
///
/// **Changed in 0.2.** The 0.1 macro always polled in lexical order. Existing
/// code compiles unchanged and silently changes behavior, with no compiler
/// diagnostic — audit every `select!` and add `biased;` wherever lexical
/// priority was intentional (a shutdown arm written first, for example, is no
/// longer checked first).
///
/// </div>
///
/// ```
/// # async fn example() {
/// let value = runite::select! {
///     biased;
///     value = async { 1 } => value,
///     value = async { 2 } => value,
/// };
/// assert_eq!(value, 1);
/// # }
/// ```
///
/// A branch may have an `if` precondition. Every precondition is evaluated
/// exactly once in lexical order before any branch future is created. A
/// disabled branch's future is still created, but is not polled. If every
/// branch is disabled, including branches disabled because their output did
/// not match the branch pattern, the `else` handler runs. Without `else`, the
/// macro panics.
///
/// ```
/// # async fn example() {
/// let value = runite::select! {
///     value = async { 1 }, if false => value,
///     else => 2,
/// };
/// assert_eq!(value, 2);
/// # }
/// ```
///
/// Once an arm wins, every branch future is dropped before its handler runs.
/// Handlers therefore execute in the caller's async and control-flow context:
/// they may contain `.await`, `return`, or `break`.
///
/// The arm count has no fixed macro limit, and each future expression is
/// created once per invocation. When selecting in a loop over methods that
/// borrow loop-owned resources, creating those futures before the invocation
/// makes the ownership boundary explicit:
///
/// ```no_run
/// # async fn example(listener: &mut runite::net::TcpListener) -> std::io::Result<()> {
/// loop {
///     let accept = listener.accept();
///     let accepted = runite::select! {
///         accepted = accept => accepted,
///     };
///     let (_stream, _address) = accepted?;
/// }
/// # }
/// ```
#[macro_export]
macro_rules! select {
    ($($input:tt)*) => {
        $crate::macros::__select! { $($input)* }
    };
}

#[cfg(test)]
mod tests {
    use std::cell::RefCell;
    use std::future::Future;
    use std::pin::Pin;
    use std::rc::Rc;
    use std::task::{Context, Poll};

    use crate::{run, spawn};

    struct PendingOnce {
        label: &'static str,
        log: Rc<RefCell<Vec<&'static str>>>,
        polled_once: bool,
    }

    impl PendingOnce {
        fn new(label: &'static str, log: Rc<RefCell<Vec<&'static str>>>) -> Self {
            Self {
                label,
                log,
                polled_once: false,
            }
        }
    }

    impl Future for PendingOnce {
        type Output = &'static str;

        fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
            self.log.borrow_mut().push(self.label);
            if self.polled_once {
                Poll::Ready(self.label)
            } else {
                self.polled_once = true;
                cx.waker().wake_by_ref();
                Poll::Pending
            }
        }
    }

    #[test]
    fn join_two_ready_futures() {
        let observed = Rc::new(RefCell::new(None));
        let observed_for_future = Rc::clone(&observed);

        spawn(async move {
            let value = crate::join!(async { 1 }, async { 2 });
            *observed_for_future.borrow_mut() = Some(value);
        });
        run();

        assert_eq!(*observed.borrow(), Some((1, 2)));
    }

    #[test]
    fn join_with_pending_then_ready() {
        let observed = Rc::new(RefCell::new(None));
        let log = Rc::new(RefCell::new(Vec::new()));
        let observed_for_future = Rc::clone(&observed);
        let log_for_future = Rc::clone(&log);

        spawn(async move {
            let value = crate::join!(
                PendingOnce::new("left", Rc::clone(&log_for_future)),
                PendingOnce::new("right", Rc::clone(&log_for_future)),
            );
            *observed_for_future.borrow_mut() = Some(value);
        });
        run();

        assert_eq!(*observed.borrow(), Some(("left", "right")));
        assert_eq!(
            log.borrow().as_slice(),
            ["left", "right", "left", "right"],
            "join! should poll both pending futures before waiting for either to complete",
        );
    }

    #[test]
    fn try_join_propagates_err() {
        let observed = Rc::new(RefCell::new(None));
        let observed_for_future = Rc::clone(&observed);

        spawn(async move {
            let value = crate::try_join!(async { Ok::<i32, &'static str>(1) }, async {
                Err::<i32, &'static str>("boom")
            },);
            *observed_for_future.borrow_mut() = Some(value);
        });
        run();

        assert_eq!(*observed.borrow(), Some(Err("boom")));
    }

    #[test]
    fn select_resolves_first_ready() {
        let observed = Rc::new(RefCell::new(None));
        let observed_for_future = Rc::clone(&observed);

        spawn(async move {
            let value = crate::select! {
                _v = ::core::future::pending::<i32>() => "pending",
                v = async { 7 } => {
                    assert_eq!(v, 7);
                    "ready"
                },
            };
            *observed_for_future.borrow_mut() = Some(value);
        });
        run();

        assert_eq!(*observed.borrow(), Some("ready"));
    }
}
