fn assert_send<T: Send>() {}

fn main() {
    assert_send::<runite::ThreadHandle>();

    // The timer tokens stay `Send` on purpose: cancelling one from a foreign
    // thread fails its generation check and is silently ignored, which has been
    // the documented behaviour since 0.2. Only `CancelOnDrop` is `!Send`,
    // because it cancels implicitly and so gives nobody a call site at which to
    // read that — see `tests/ui/fail/cancel_on_drop_send.rs`.
    assert_send::<runite::TimeoutHandle>();
    assert_send::<runite::IntervalHandle>();
}
