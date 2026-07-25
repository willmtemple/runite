use super::{
    READ_DIR_BUFFER_CAPACITY, ReadDirConsumer, ReadDirJob, read_dir_channel_with_scheduler,
};
use crate::platform::current::runtime::current_thread_handle;
use std::collections::VecDeque;
use std::io;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll, Waker};

fn poll_protocol<T: Send + 'static>(
    consumer: &mut ReadDirConsumer<T>,
) -> Poll<io::Result<Option<T>>> {
    let mut context = Context::from_waker(Waker::noop());
    consumer.poll_next(&mut context)
}

#[derive(Default)]
struct ManualReadDirScheduler {
    jobs: Mutex<VecDeque<ReadDirJob>>,
    submissions: AtomicUsize,
    fail_submission: AtomicUsize,
    fail_permanently: AtomicBool,
}

impl ManualReadDirScheduler {
    fn schedule(&self, job: ReadDirJob) -> io::Result<()> {
        let submission = self.submissions.fetch_add(1, Ordering::AcqRel) + 1;
        if self.fail_submission.load(Ordering::Acquire) == submission {
            return Err(if self.fail_permanently.load(Ordering::Acquire) {
                io::Error::other("synthetic scheduler is gone")
            } else {
                io::Error::new(
                    io::ErrorKind::WouldBlock,
                    "synthetic blocking queue is full",
                )
            });
        }
        self.jobs.lock().unwrap().push_back(job);
        Ok(())
    }

    fn fail_on(&self, submission: usize) {
        self.fail_submission.store(submission, Ordering::Release);
    }

    /// Fails the selected submission with a non-transient error, which must end
    /// the scan rather than being retried.
    fn fail_permanently_on(&self, submission: usize) {
        self.fail_permanently.store(true, Ordering::Release);
        self.fail_submission.store(submission, Ordering::Release);
    }

    fn run_one(&self) -> bool {
        let job = self.jobs.lock().unwrap().pop_front();
        if let Some(job) = job {
            job();
            true
        } else {
            false
        }
    }

    fn queued(&self) -> usize {
        self.jobs.lock().unwrap().len()
    }
}

fn synthetic_read_dir<T, I>(
    owner: crate::platform::current::runtime::ThreadHandle,
    capacity: usize,
    open: impl FnOnce() -> io::Result<I> + Send + 'static,
    scheduler: &Arc<ManualReadDirScheduler>,
) -> io::Result<ReadDirConsumer<T>>
where
    T: Send + 'static,
    I: Iterator<Item = io::Result<T>> + Send + 'static,
{
    let scheduler_for_submit = Arc::clone(scheduler);
    read_dir_channel_with_scheduler(owner, capacity, open, move |job| {
        scheduler_for_submit.schedule(job)
    })
}

fn next_protocol<T: Send + 'static>(
    consumer: &mut ReadDirConsumer<T>,
    scheduler: &ManualReadDirScheduler,
) -> io::Result<Option<T>> {
    loop {
        if let Poll::Ready(entry) = poll_protocol(consumer) {
            return entry;
        }
        assert!(
            scheduler.run_one(),
            "pending synthetic directory read had no scheduled batch"
        );
    }
}

struct DropTrackedEntries<I> {
    entries: I,
    dropped: Arc<AtomicBool>,
}

impl<I: Iterator> Iterator for DropTrackedEntries<I> {
    type Item = I::Item;

    fn next(&mut self) -> Option<Self::Item> {
        self.entries.next()
    }
}

impl<I> Drop for DropTrackedEntries<I> {
    fn drop(&mut self) {
        self.dropped.store(true, Ordering::Release);
    }
}

#[test]
fn batch_stops_at_the_exact_buffer_capacity() {
    let owner = current_thread_handle();
    let baseline = owner.shared.pending_ops.load(Ordering::Acquire);
    let scheduler = Arc::new(ManualReadDirScheduler::default());
    let attempts = Arc::new(Mutex::new(Vec::new()));
    let attempts_by_iterator = Arc::clone(&attempts);
    let mut consumer = synthetic_read_dir(
        owner.clone(),
        READ_DIR_BUFFER_CAPACITY,
        move || {
            Ok((0..=READ_DIR_BUFFER_CAPACITY).map(move |entry| {
                attempts_by_iterator.lock().unwrap().push(entry);
                Ok(entry)
            }))
        },
        &scheduler,
    )
    .expect("initial batch should schedule");
    let observer = consumer.observer();

    assert!(scheduler.run_one(), "initial batch should be queued");
    assert_eq!(
        attempts.lock().unwrap().as_slice(),
        &(0..READ_DIR_BUFFER_CAPACITY).collect::<Vec<_>>(),
        "a full batch must not pull one extra iterator item"
    );
    assert_eq!(observer.buffered(), READ_DIR_BUFFER_CAPACITY);
    assert_eq!(observer.peak_buffered(), READ_DIR_BUFFER_CAPACITY);
    assert_eq!(
        scheduler.queued(),
        0,
        "a full producer must return its worker instead of self-scheduling"
    );

    for expected in 0..READ_DIR_BUFFER_CAPACITY {
        assert_eq!(
            next_protocol(&mut consumer, &scheduler).unwrap(),
            Some(expected)
        );
    }
    assert_eq!(
        next_protocol(&mut consumer, &scheduler).unwrap(),
        Some(READ_DIR_BUFFER_CAPACITY)
    );
    assert_eq!(next_protocol(&mut consumer, &scheduler).unwrap(), None);
    assert_eq!(owner.shared.pending_ops.load(Ordering::Acquire), baseline);
}

#[test]
fn large_synthetic_directory_preserves_order_and_bound() {
    let owner = current_thread_handle();
    let baseline = owner.shared.pending_ops.load(Ordering::Acquire);
    let scheduler = Arc::new(ManualReadDirScheduler::default());
    let total = READ_DIR_BUFFER_CAPACITY * 8 + 7;
    let mut consumer = synthetic_read_dir(
        owner.clone(),
        READ_DIR_BUFFER_CAPACITY,
        move || Ok((0..total).map(Ok)),
        &scheduler,
    )
    .expect("initial batch should schedule");
    let observer = consumer.observer();

    for expected in 0..total {
        assert_eq!(
            next_protocol(&mut consumer, &scheduler).unwrap(),
            Some(expected)
        );
    }
    assert_eq!(next_protocol(&mut consumer, &scheduler).unwrap(), None);
    assert!(observer.peak_buffered() <= READ_DIR_BUFFER_CAPACITY);
    assert_eq!(owner.shared.pending_ops.load(Ordering::Acquire), baseline);
}

#[test]
fn preserves_entry_errors_and_terminal_error_then_eof() {
    let owner = current_thread_handle();
    let baseline = owner.shared.pending_ops.load(Ordering::Acquire);
    let scheduler = Arc::new(ManualReadDirScheduler::default());
    let mut consumer = synthetic_read_dir(
        owner.clone(),
        3,
        || {
            Ok(vec![
                Ok(10),
                Err(io::Error::new(io::ErrorKind::PermissionDenied, "entry")),
                Ok(20),
            ]
            .into_iter())
        },
        &scheduler,
    )
    .expect("initial batch should schedule");

    assert_eq!(next_protocol(&mut consumer, &scheduler).unwrap(), Some(10));
    assert_eq!(
        next_protocol(&mut consumer, &scheduler)
            .expect_err("per-entry failure should be preserved")
            .kind(),
        io::ErrorKind::PermissionDenied
    );
    assert_eq!(next_protocol(&mut consumer, &scheduler).unwrap(), Some(20));
    assert_eq!(next_protocol(&mut consumer, &scheduler).unwrap(), None);

    let scheduler = Arc::new(ManualReadDirScheduler::default());
    let mut consumer = synthetic_read_dir(
        owner.clone(),
        1,
        || {
            Err::<std::vec::IntoIter<io::Result<usize>>, _>(io::Error::new(
                io::ErrorKind::NotFound,
                "directory",
            ))
        },
        &scheduler,
    )
    .expect("open-error batch should schedule");
    assert_eq!(
        next_protocol(&mut consumer, &scheduler)
            .expect_err("open failure should be returned once")
            .kind(),
        io::ErrorKind::NotFound
    );
    assert_eq!(next_protocol(&mut consumer, &scheduler).unwrap(), None);
    assert_eq!(owner.shared.pending_ops.load(Ordering::Acquire), baseline);
}

#[test]
fn dropping_before_start_skips_the_directory_factory() {
    let owner = current_thread_handle();
    let baseline = owner.shared.pending_ops.load(Ordering::Acquire);
    let scheduler = Arc::new(ManualReadDirScheduler::default());
    let opened = Arc::new(AtomicBool::new(false));
    let opened_by_worker = Arc::clone(&opened);
    let consumer = synthetic_read_dir(
        owner.clone(),
        1,
        move || {
            opened_by_worker.store(true, Ordering::Release);
            Ok(std::iter::empty::<io::Result<usize>>())
        },
        &scheduler,
    )
    .expect("initial batch should schedule");
    let observer = consumer.observer();
    drop(consumer);

    assert!(observer.is_cancelled());
    assert!(!opened.load(Ordering::Acquire));
    assert_eq!(owner.shared.pending_ops.load(Ordering::Acquire), baseline);
    assert!(
        scheduler.run_one(),
        "the cancelled batch remains queued but must be a no-op"
    );
    assert!(!opened.load(Ordering::Acquire));
}

#[test]
fn dropping_paused_scan_drops_iterator_and_liveness() {
    let owner = current_thread_handle();
    let baseline = owner.shared.pending_ops.load(Ordering::Acquire);
    let scheduler = Arc::new(ManualReadDirScheduler::default());
    let iterator_dropped = Arc::new(AtomicBool::new(false));
    let dropped_by_iterator = Arc::clone(&iterator_dropped);
    let consumer = synthetic_read_dir(
        owner.clone(),
        READ_DIR_BUFFER_CAPACITY,
        move || {
            Ok(DropTrackedEntries {
                entries: (0..).map(Ok),
                dropped: dropped_by_iterator,
            })
        },
        &scheduler,
    )
    .expect("initial batch should schedule");
    let observer = consumer.observer();

    assert!(scheduler.run_one(), "initial batch should run");
    assert_eq!(observer.buffered(), READ_DIR_BUFFER_CAPACITY);
    assert_eq!(observer.peak_buffered(), READ_DIR_BUFFER_CAPACITY);
    assert!(!iterator_dropped.load(Ordering::Acquire));
    assert_eq!(scheduler.queued(), 0);

    drop(consumer);
    assert!(observer.is_cancelled());
    assert!(iterator_dropped.load(Ordering::Acquire));
    assert_eq!(owner.shared.pending_ops.load(Ordering::Acquire), baseline);
}

/// A full blocking-pool queue is transient. As long as the consumer still has
/// buffered entries to hand out, a failed refill must not end a half-read
/// directory: a later poll drives another refill and the scan completes.
#[test]
fn transient_refill_failure_resumes_while_entries_remain_buffered() {
    let owner = current_thread_handle();
    let baseline = owner.shared.pending_ops.load(Ordering::Acquire);
    let scheduler = Arc::new(ManualReadDirScheduler::default());
    scheduler.fail_on(2);
    let iterator_dropped = Arc::new(AtomicBool::new(false));
    let dropped_by_iterator = Arc::clone(&iterator_dropped);
    let mut consumer = synthetic_read_dir(
        owner.clone(),
        4,
        move || {
            Ok(DropTrackedEntries {
                entries: (0..10).map(Ok),
                dropped: dropped_by_iterator,
            })
        },
        &scheduler,
    )
    .expect("initial batch should schedule");

    assert!(scheduler.run_one(), "initial batch should run");

    let mut seen = Vec::new();
    while let Some(entry) = next_protocol(&mut consumer, &scheduler)
        .expect("a transient refill failure must not end the scan")
    {
        seen.push(entry);
    }

    assert_eq!(
        seen,
        (0..10).collect::<Vec<_>>(),
        "every entry should still be delivered, in order"
    );
    assert!(
        iterator_dropped.load(Ordering::Acquire),
        "the iterator is dropped once the scan finishes"
    );
    assert_eq!(owner.shared.pending_ops.load(Ordering::Acquire), baseline);
}

#[test]
fn permanent_reschedule_failure_is_terminal_and_drops_iterator() {
    let owner = current_thread_handle();
    let baseline = owner.shared.pending_ops.load(Ordering::Acquire);
    let scheduler = Arc::new(ManualReadDirScheduler::default());
    scheduler.fail_permanently_on(2);
    let iterator_dropped = Arc::new(AtomicBool::new(false));
    let dropped_by_iterator = Arc::clone(&iterator_dropped);
    let mut consumer = synthetic_read_dir(
        owner.clone(),
        4,
        move || {
            Ok(DropTrackedEntries {
                entries: (0..10).map(Ok),
                dropped: dropped_by_iterator,
            })
        },
        &scheduler,
    )
    .expect("initial batch should schedule");

    assert!(scheduler.run_one(), "initial batch should run");
    assert_eq!(next_protocol(&mut consumer, &scheduler).unwrap(), Some(0));
    assert_eq!(
        next_protocol(&mut consumer, &scheduler).unwrap(),
        Some(1),
        "the entry that triggers refill remains ordered before its failure"
    );
    assert!(iterator_dropped.load(Ordering::Acquire));
    assert_eq!(next_protocol(&mut consumer, &scheduler).unwrap(), Some(2));
    assert_eq!(next_protocol(&mut consumer, &scheduler).unwrap(), Some(3));
    assert_eq!(
        next_protocol(&mut consumer, &scheduler)
            .expect_err("failed reschedule should be reported after buffered entries")
            .kind(),
        io::ErrorKind::Other
    );
    assert_eq!(next_protocol(&mut consumer, &scheduler).unwrap(), None);
    assert_eq!(owner.shared.pending_ops.load(Ordering::Acquire), baseline);
}

#[test]
fn initial_schedule_failure_releases_state_without_opening() {
    let owner = current_thread_handle();
    let baseline = owner.shared.pending_ops.load(Ordering::Acquire);
    let scheduler = Arc::new(ManualReadDirScheduler::default());
    scheduler.fail_on(1);
    let opened = Arc::new(AtomicBool::new(false));
    let opened_by_worker = Arc::clone(&opened);

    let error = synthetic_read_dir::<usize, _>(
        owner.clone(),
        READ_DIR_BUFFER_CAPACITY,
        move || {
            opened_by_worker.store(true, Ordering::Release);
            Ok(std::iter::empty())
        },
        &scheduler,
    )
    .err()
    .expect("initial scheduling failure should be returned");

    assert_eq!(error.kind(), io::ErrorKind::WouldBlock);
    assert!(!opened.load(Ordering::Acquire));
    assert_eq!(owner.shared.pending_ops.load(Ordering::Acquire), baseline);
}

#[test]
fn panicking_batch_becomes_terminal_error() {
    let owner = current_thread_handle();
    let baseline = owner.shared.pending_ops.load(Ordering::Acquire);
    let scheduler = Arc::new(ManualReadDirScheduler::default());
    let mut consumer = synthetic_read_dir(
        owner.clone(),
        READ_DIR_BUFFER_CAPACITY,
        || {
            Ok(std::iter::once_with(|| -> io::Result<usize> {
                panic!("synthetic iterator panic")
            }))
        },
        &scheduler,
    )
    .expect("initial batch should schedule");

    let panic = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| scheduler.run_one()));
    assert!(panic.is_err(), "the synthetic batch should panic");
    assert_eq!(
        next_protocol(&mut consumer, &scheduler)
            .expect_err("a panicking producer must terminalize the stream")
            .kind(),
        io::ErrorKind::Other
    );
    assert_eq!(next_protocol(&mut consumer, &scheduler).unwrap(), None);
    assert_eq!(owner.shared.pending_ops.load(Ordering::Acquire), baseline);
}

/// A panic raised while the shared state lock is held poisons that mutex. The
/// batch guard's drop then runs during the unwind and takes the same lock, so
/// an `unwrap` there would panic while already panicking and abort the process
/// -- in a runtime whose whole premise is that a panic stays contained. The
/// scan must instead terminalize normally and stay usable.
#[test]
fn poisoned_state_does_not_abort_the_scan() {
    let owner = current_thread_handle();
    let baseline = owner.shared.pending_ops.load(Ordering::Acquire);
    let scheduler = Arc::new(ManualReadDirScheduler::default());
    let mut consumer = synthetic_read_dir(owner.clone(), 4, || Ok((0..6).map(Ok)), &scheduler)
        .expect("initial batch should schedule");
    let shared = Arc::clone(&consumer.observer().shared);

    assert!(scheduler.run_one(), "initial batch should run");
    assert_eq!(next_protocol(&mut consumer, &scheduler).unwrap(), Some(0));

    // Poison the state mutex exactly as a panic under the lock would.
    let poisoned = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        let _guard = shared.state.lock().expect("lock should still be healthy");
        panic!("poison the read_dir state");
    }));
    assert!(
        poisoned.is_err(),
        "the helper panic should have been caught"
    );
    assert!(
        shared.state.lock().is_err(),
        "the state mutex should now be poisoned"
    );

    // Every remaining entry must still be delivered, and the scan must reach a
    // clean end with its runtime liveness released.
    let mut seen = Vec::new();
    while let Some(entry) =
        next_protocol(&mut consumer, &scheduler).expect("a poisoned mutex must not break the scan")
    {
        seen.push(entry);
    }
    assert_eq!(seen, (1..6).collect::<Vec<_>>());
    assert_eq!(owner.shared.pending_ops.load(Ordering::Acquire), baseline);
}
