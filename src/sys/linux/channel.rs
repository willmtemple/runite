//! Linux channel wake helpers.

use crate::op::completion::{CompletionFuture, CompletionHandle, completion};
use crate::platform::current::runtime::try_current_thread_handle;

pub(crate) fn runtime_waiter<T: Send + 'static>() -> (CompletionFuture<T>, CompletionHandle<T>) {
    let owner = try_current_thread_handle()
        .expect("async channel operations must be polled on a runtime thread");
    completion(owner)
}
