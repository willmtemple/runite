//! Linux channel wake helpers.

use crate::op::completion::{CompletionFuture, CompletionHandle, WakeClass, completion};
use crate::platform::current::runtime::try_current_thread_handle;

pub(crate) fn runtime_waiter<T: Send + 'static>() -> (CompletionFuture<T>, CompletionHandle<T>) {
    let owner = try_current_thread_handle()
        .expect("async channel operations must be polled on a runtime thread");
    // Channel resolutions are in-process events: same-thread wakes join the
    // current microtask checkpoint (the `Promise.resolve` analog), unlike I/O
    // completions which always take a macro turn.
    completion(owner, WakeClass::Microtask)
}
