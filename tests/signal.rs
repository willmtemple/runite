//! Subprocess tests for process-global signal and console-handler behavior.

use std::future::{Future, poll_fn};
use std::process::{Command, Stdio};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering as AtomicOrdering};
use std::thread;
use std::time::{Duration, Instant};

const HELPER_ENV: &str = "RUNITE_SIGNAL_TEST_HELPER";
const HELPER_TIMEOUT: Duration = Duration::from_secs(10);

fn run_helper(mode: &str, remote_queue_capacity: usize) {
    let mut child = Command::new(std::env::current_exe().expect("test executable should exist"))
        .args([
            "--exact",
            "signal_subprocess_entry",
            "--nocapture",
            "--test-threads=1",
        ])
        .env(HELPER_ENV, mode)
        .env(
            "RUNITE_REMOTE_QUEUE_CAPACITY",
            remote_queue_capacity.to_string(),
        )
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("signal test subprocess should start");

    let deadline = Instant::now() + HELPER_TIMEOUT;
    loop {
        if child
            .try_wait()
            .expect("signal test subprocess status should be readable")
            .is_some()
        {
            break;
        }
        if Instant::now() >= deadline {
            child
                .kill()
                .expect("timed-out signal test subprocess should be killed");
            let output = child
                .wait_with_output()
                .expect("killed signal test subprocess output should be readable");
            panic!(
                "signal test subprocess {mode:?} timed out\nstdout:\n{}\nstderr:\n{}",
                String::from_utf8_lossy(&output.stdout),
                String::from_utf8_lossy(&output.stderr),
            );
        }
        thread::sleep(Duration::from_millis(10));
    }

    let output = child
        .wait_with_output()
        .expect("signal test subprocess output should be readable");
    assert!(
        output.status.success(),
        "signal test subprocess {mode:?} failed with {}\nstdout:\n{}\nstderr:\n{}",
        output.status,
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr),
    );
}

#[cfg(unix)]
#[test]
fn ctrl_c_bypasses_saturated_remote_queue() {
    run_helper("unix-saturated-ctrl-c", 1);
}

#[cfg(unix)]
#[test]
fn unix_signal_wakes_coalesce_until_the_runtime_drains_them() {
    run_helper("unix-coalescing", 64);
}

#[cfg(unix)]
#[test]
fn unix_signal_receiver_drop_cleans_up_pending_delivery() {
    run_helper("unix-receiver-drop", 1);
}

#[cfg(unix)]
#[test]
fn unix_signal_kinds_install_in_an_isolated_process() {
    run_helper("unix-kinds-install", 64);
}

#[cfg(unix)]
#[test]
fn unix_multi_kind_stream_reports_which_signal_arrived() {
    run_helper("unix-multi-kind", 64);
}

#[cfg(windows)]
#[test]
fn windows_ctrl_c_bypasses_saturated_remote_queue() {
    run_helper("windows-saturated-ctrl-c", 1);
}

#[cfg(windows)]
#[test]
fn windows_console_wakes_coalesce_until_the_runtime_drains_them() {
    run_helper("windows-coalescing", 64);
}

#[cfg(windows)]
#[test]
fn windows_console_receiver_drop_cleans_up_pending_delivery() {
    run_helper("windows-receiver-drop", 1);
}

#[test]
fn signal_subprocess_entry() {
    let Ok(mode) = std::env::var(HELPER_ENV) else {
        return;
    };

    #[cfg(unix)]
    match mode.as_str() {
        "unix-saturated-ctrl-c" => unix_saturated_ctrl_c(),
        "unix-coalescing" => unix_coalescing(),
        "unix-receiver-drop" => unix_receiver_drop(),
        "unix-kinds-install" => unix_kinds_install(),
        "unix-multi-kind" => unix_multi_kind(),
        _ => panic!("unknown Unix signal test helper mode {mode:?}"),
    }

    #[cfg(windows)]
    match mode.as_str() {
        "windows-saturated-ctrl-c" => windows_saturated_ctrl_c(),
        "windows-coalescing" => windows_coalescing(),
        "windows-receiver-drop" => windows_receiver_drop(),
        _ => panic!("unknown Windows signal test helper mode {mode:?}"),
    }
}

fn fill_one_remote_queue_slot() {
    let handle = runite::current_thread_handle();
    thread::spawn(move || handle.queue_macrotask(|| {}))
        .join()
        .expect("remote queue sender thread should not panic")
        .expect("one user macrotask should fill a capacity-one remote queue");
}

async fn count_polls<F: Future>(future: F, polls: Arc<AtomicUsize>) -> F::Output {
    let mut future = Box::pin(future);
    poll_fn(move |cx| {
        polls.fetch_add(1, AtomicOrdering::AcqRel);
        future.as_mut().poll(cx)
    })
    .await
}

#[cfg(unix)]
fn send_unix_signal(signal: libc::c_int) {
    // SAFETY: `getpid` returns this process's id and each caller supplies a
    // signal number supported by the installed runite handler.
    let result = unsafe { libc::kill(libc::getpid(), signal) };
    assert_eq!(result, 0, "sending signal {signal} should succeed");
}

#[cfg(unix)]
fn unix_saturated_ctrl_c() {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, Ordering};

    let delivered = Arc::new(AtomicBool::new(false));
    let delivered_task = Arc::clone(&delivered);
    runite::spawn(async move {
        let received =
            runite::time::timeout(Duration::from_secs(2), runite::signal::ctrl_c()).await;
        delivered_task.store(matches!(received, Ok(Ok(()))), Ordering::Release);
    });
    runite::run_until_stalled();

    fill_one_remote_queue_slot();
    send_unix_signal(libc::SIGINT);
    thread::sleep(Duration::from_millis(150));

    runite::run();
    assert!(
        delivered.load(Ordering::Acquire),
        "Ctrl-C should use a capacity-bypassing internal wake"
    );
}

#[cfg(unix)]
fn unix_coalescing() {
    use std::sync::{Arc, Mutex};

    let observations = Arc::new(Mutex::new(None));
    let observations_task = Arc::clone(&observations);
    let second_polls = Arc::new(AtomicUsize::new(0));
    let second_polls_task = Arc::clone(&second_polls);
    let mut signal = runite::signal::unix::signal(runite::signal::unix::SignalKind::User1)
        .expect("SIGUSR1 stream should install");
    runite::spawn(async move {
        let first = runite::time::timeout(Duration::from_secs(2), signal.recv())
            .await
            .is_ok();
        let second = runite::time::timeout(
            Duration::from_millis(100),
            count_polls(signal.recv(), Arc::clone(&second_polls_task)),
        )
        .await
        .is_ok();
        *observations_task
            .lock()
            .expect("signal observations mutex poisoned") = Some((
            first,
            second,
            second_polls_task.load(AtomicOrdering::Acquire),
        ));
    });
    runite::run_until_stalled();

    for _ in 0..3 {
        send_unix_signal(libc::SIGUSR1);
        thread::sleep(Duration::from_millis(150));
    }

    runite::run();
    assert_eq!(
        observations
            .lock()
            .expect("signal observations mutex poisoned")
            .take(),
        Some((true, false, 2)),
        "signals delivered while one wake is pending should queue only one wake"
    );
}

#[cfg(unix)]
fn unix_receiver_drop() {
    let signal = runite::signal::unix::signal(runite::signal::unix::SignalKind::User2)
        .expect("SIGUSR2 stream should install");
    send_unix_signal(libc::SIGUSR2);
    thread::sleep(Duration::from_millis(150));
    drop(signal);

    runite::run();
}

#[cfg(unix)]
fn unix_kinds_install() {
    use runite::signal::unix::{SignalKind, signal};

    for kind in [
        SignalKind::Interrupt,
        SignalKind::Terminate,
        SignalKind::Hangup,
        SignalKind::Quit,
        SignalKind::User1,
        SignalKind::User2,
        SignalKind::WindowChange,
    ] {
        let first = signal(kind).expect("signal stream should install");
        let second = signal(kind).expect("repeat registration should share the process handler");
        drop(second);
        drop(first);
    }

    runite::run();
}

/// One stream serving several kinds reports which one arrived, and registering
/// the same kind twice registers it once.
#[cfg(unix)]
fn unix_multi_kind() {
    use std::sync::{Arc, Mutex};

    use runite::signal::unix::{SignalKind, signals};

    assert!(
        signals(&[]).is_err(),
        "an empty kind set should be rejected rather than never ready"
    );

    let deduplicated =
        signals(&[SignalKind::Hangup, SignalKind::Hangup]).expect("duplicate kinds should install");
    assert_eq!(
        deduplicated.kinds().len(),
        1,
        "a repeated kind should register once"
    );
    drop(deduplicated);

    let observed = Arc::new(Mutex::new(Vec::<String>::new()));
    let observed_task = Arc::clone(&observed);
    let mut stream = signals(&[SignalKind::User1, SignalKind::User2])
        .expect("SIGUSR1/SIGUSR2 streams should install");

    runite::spawn(async move {
        for _ in 0..2 {
            let Ok(Some(kind)) = runite::time::timeout(Duration::from_secs(2), stream.recv()).await
            else {
                break;
            };
            observed_task
                .lock()
                .expect("observation mutex poisoned")
                .push(format!("{kind:?}"));
        }
    });
    runite::run_until_stalled();

    send_unix_signal(libc::SIGUSR1);
    thread::sleep(Duration::from_millis(150));
    send_unix_signal(libc::SIGUSR2);
    thread::sleep(Duration::from_millis(150));

    runite::run();

    let observed = observed.lock().expect("observation mutex poisoned").clone();
    assert_eq!(
        observed,
        vec!["User1".to_string(), "User2".to_string()],
        "each event should name the kind that produced it"
    );
}

#[cfg(windows)]
fn prepare_private_console() {
    use windows_sys::Win32::System::Console::{AllocConsole, FreeConsole, SetConsoleCtrlHandler};

    // SAFETY: this helper runs in an isolated subprocess. It detaches any
    // inherited console and allocates one owned only by that subprocess.
    unsafe {
        let _ = FreeConsole();
        assert_ne!(AllocConsole(), 0, "private test console should allocate");
        // Ctrl-C *ignoring* is a per-process disposition that a child inherits
        // and that allocating a fresh console does not clear. A parent that set
        // it — an OpenSSH session host does — would otherwise make
        // `CTRL_C_EVENT` land on a process that discards it, while
        // `CTRL_BREAK_EVENT` still arrives, which is a confusing way to fail.
        // Passing a null handler with `FALSE` removes the inherited ignore.
        assert_ne!(
            SetConsoleCtrlHandler(None, 0),
            0,
            "Ctrl-C handling should be re-enabled for the test subprocess"
        );
    }
}

#[cfg(windows)]
fn send_console_event(event: u32) {
    use windows_sys::Win32::System::Console::GenerateConsoleCtrlEvent;

    // SAFETY: the subprocess owns a private console, and process group zero
    // broadcasts only within that console.
    let result = unsafe { GenerateConsoleCtrlEvent(event, 0) };
    assert_ne!(result, 0, "console control event {event} should generate");
}

#[cfg(windows)]
fn windows_saturated_ctrl_c() {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, Ordering};
    use windows_sys::Win32::System::Console::CTRL_C_EVENT;

    prepare_private_console();

    let delivered = Arc::new(AtomicBool::new(false));
    let delivered_task = Arc::clone(&delivered);
    runite::spawn(async move {
        let received =
            runite::time::timeout(Duration::from_secs(2), runite::signal::ctrl_c()).await;
        delivered_task.store(matches!(received, Ok(Ok(()))), Ordering::Release);
    });
    runite::run_until_stalled();

    fill_one_remote_queue_slot();
    send_console_event(CTRL_C_EVENT);
    thread::sleep(Duration::from_millis(150));

    runite::run();
    assert!(
        delivered.load(Ordering::Acquire),
        "Ctrl-C should use a capacity-bypassing internal wake"
    );
}

#[cfg(windows)]
fn windows_coalescing() {
    use std::sync::{Arc, Mutex};
    use windows_sys::Win32::System::Console::CTRL_BREAK_EVENT;

    prepare_private_console();

    let observations = Arc::new(Mutex::new(None));
    let observations_task = Arc::clone(&observations);
    let second_polls = Arc::new(AtomicUsize::new(0));
    let second_polls_task = Arc::clone(&second_polls);
    let mut signal =
        runite::signal::windows::ctrl_break().expect("Ctrl-Break stream should install");
    runite::spawn(async move {
        let first = runite::time::timeout(Duration::from_secs(2), signal.recv())
            .await
            .is_ok();
        let second = runite::time::timeout(
            Duration::from_millis(100),
            count_polls(signal.recv(), Arc::clone(&second_polls_task)),
        )
        .await
        .is_ok();
        *observations_task
            .lock()
            .expect("console observations mutex poisoned") = Some((
            first,
            second,
            second_polls_task.load(AtomicOrdering::Acquire),
        ));
    });
    runite::run_until_stalled();

    for _ in 0..3 {
        send_console_event(CTRL_BREAK_EVENT);
        thread::sleep(Duration::from_millis(150));
    }

    runite::run();
    assert_eq!(
        observations
            .lock()
            .expect("console observations mutex poisoned")
            .take(),
        Some((true, false, 2)),
        "console events delivered while one wake is pending should queue only one wake"
    );
}

#[cfg(windows)]
fn windows_receiver_drop() {
    use windows_sys::Win32::System::Console::CTRL_BREAK_EVENT;

    prepare_private_console();

    let signal = runite::signal::windows::ctrl_break().expect("Ctrl-Break stream should install");
    send_console_event(CTRL_BREAK_EVENT);
    thread::sleep(Duration::from_millis(150));
    drop(signal);

    runite::run();
}
