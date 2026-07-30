fn assert_send<T: Send>() {}

fn main() {
    // The guard's whole contract is "dropping cancels", and cancelling from a
    // foreign thread is a documented no-op — so it must not be sendable.
    assert_send::<runite::CancelOnDrop<runite::IntervalHandle>>();
}
