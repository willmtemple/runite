//! End-to-end filesystem tests exercising the public `runite::fs` API.

mod common;

use common::block_on;
use runite::fs::{self, File, OpenOptions};
use std::process::{Command, Stdio};
use std::thread;
use std::time::{Duration, Instant};

const READ_DIR_HELPER_ENV: &str = "RUNITE_READ_DIR_BLOCKING_HELPER";
const READ_DIR_HELPER_TIMEOUT: Duration = Duration::from_secs(20);

fn temp_path(name: &str) -> std::path::PathBuf {
    let mut dir = std::env::current_dir()
        .expect("current directory")
        .join("target");
    let unique = format!(
        "runite-fs-it-{}-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("system clock after epoch")
            .as_nanos(),
        name,
    );
    dir.push(unique);
    dir
}

fn run_read_dir_helper(test_name: &str, mode: &str) {
    let mut child = Command::new(std::env::current_exe().expect("test executable should exist"))
        .args(["--exact", test_name, "--nocapture", "--test-threads=1"])
        .env(READ_DIR_HELPER_ENV, mode)
        .env("RUNITE_BLOCKING_THREADS", "1")
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("read_dir helper subprocess should start");

    let deadline = Instant::now() + READ_DIR_HELPER_TIMEOUT;
    loop {
        if child
            .try_wait()
            .expect("read_dir helper status should be readable")
            .is_some()
        {
            break;
        }
        if Instant::now() >= deadline {
            child
                .kill()
                .expect("timed-out read_dir helper should be killed");
            let output = child
                .wait_with_output()
                .expect("timed-out read_dir helper output should be readable");
            panic!(
                "read_dir helper {mode:?} timed out\nstdout:\n{}\nstderr:\n{}",
                String::from_utf8_lossy(&output.stdout),
                String::from_utf8_lossy(&output.stderr),
            );
        }
        thread::sleep(Duration::from_millis(10));
    }

    let output = child
        .wait_with_output()
        .expect("read_dir helper output should be readable");
    assert!(
        output.status.success(),
        "read_dir helper {mode:?} failed with {}\nstdout:\n{}\nstderr:\n{}",
        output.status,
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr),
    );
}

fn create_read_dir_fixture(name: &str, entries: usize) -> std::path::PathBuf {
    let path = temp_path(name);
    std::fs::create_dir_all(&path).expect("create read_dir fixture");
    for index in 0..entries {
        std::fs::write(path.join(format!("entry-{index:03}.txt")), b"x")
            .expect("write read_dir fixture entry");
    }
    path
}

async fn scan_with_dependent_blocking_work(path: std::path::PathBuf) -> usize {
    let mut directory = fs::read_dir(&path).await.expect("open large directory");
    let first = directory
        .next_entry()
        .await
        .expect("read first directory entry")
        .expect("large directory should not be empty");

    let value = runite::spawn_blocking(|| 42usize)
        .expect("dependent blocking work should queue")
        .await
        .expect("dependent blocking work should complete");
    assert_eq!(value, 42);
    first
        .metadata()
        .await
        .expect("entry metadata should complete with one blocking worker");

    let mut count = 1;
    while directory
        .next_entry()
        .await
        .expect("directory scan should continue")
        .is_some()
    {
        count += 1;
    }
    count
}

#[test]
fn read_dir_releases_one_worker_for_dependent_blocking_work() {
    const MODE: &str = "dependent";
    if std::env::var(READ_DIR_HELPER_ENV).as_deref() != Ok(MODE) {
        run_read_dir_helper(
            "read_dir_releases_one_worker_for_dependent_blocking_work",
            MODE,
        );
        return;
    }

    let path = create_read_dir_fixture("one-worker-dependent", 97);
    let scan_path = path.clone();
    let count = block_on(move || scan_with_dependent_blocking_work(scan_path));
    assert_eq!(count, 97);
    std::fs::remove_dir_all(path).expect("remove read_dir fixture");
}

#[test]
fn concurrent_read_dirs_do_not_starve_one_worker() {
    const MODE: &str = "concurrent";
    if std::env::var(READ_DIR_HELPER_ENV).as_deref() != Ok(MODE) {
        run_read_dir_helper("concurrent_read_dirs_do_not_starve_one_worker", MODE);
        return;
    }

    const ENTRIES: usize = 65;
    let root = temp_path("one-worker-concurrent");
    let paths = (0..4)
        .map(|index| {
            let path = root.join(format!("scan-{index}"));
            std::fs::create_dir_all(&path).expect("create concurrent read_dir fixture");
            for entry in 0..ENTRIES {
                std::fs::write(path.join(format!("entry-{entry:03}.txt")), b"x")
                    .expect("write concurrent read_dir fixture entry");
            }
            path
        })
        .collect::<Vec<_>>();
    let mut paths = paths.into_iter();
    let first = paths.next().unwrap();
    let second = paths.next().unwrap();
    let third = paths.next().unwrap();
    let fourth = paths.next().unwrap();

    let counts = block_on(move || async move {
        runite::join!(
            scan_with_dependent_blocking_work(first),
            scan_with_dependent_blocking_work(second),
            scan_with_dependent_blocking_work(third),
            scan_with_dependent_blocking_work(fourth),
        )
    });
    assert_eq!(counts, (ENTRIES, ENTRIES, ENTRIES, ENTRIES));
    std::fs::remove_dir_all(root).expect("remove concurrent read_dir fixtures");
}

#[test]
fn abandoned_write_completion_is_not_returned_for_a_new_file_buffer() {
    use runite::io::AsyncWrite;
    use std::future::poll_fn;
    use std::pin::Pin;
    use std::task::Poll;

    let path = temp_path("abandoned-write");
    let contents = block_on(move || async move {
        let mut file = File::create(&path).await.expect("create file");

        poll_fn(|cx| {
            assert!(
                Pin::new(&mut file).poll_write(cx, b"old").is_pending(),
                "the first backend poll must leave the write in flight"
            );
            Poll::Ready(())
        })
        .await;

        file.write_all(b"new bytes")
            .await
            .expect("new write must get its own completion");
        drop(file);

        let contents = fs::read(&path).await.expect("read completed writes");
        fs::remove_file(&path).await.expect("remove fixture");
        contents
    });

    assert_eq!(contents, b"oldnew bytes");
}

#[test]
fn repeated_identical_write_future_gets_a_new_operation_generation() {
    use std::future::{Future, poll_fn};
    use std::pin::pin;
    use std::task::Poll;

    let path = temp_path("repeated-identical-write");
    let contents = block_on(move || async move {
        let mut file = File::create(&path).await.expect("create file");
        let bytes = b"same";
        {
            let mut first = pin!(file.write(bytes));
            poll_fn(|cx| {
                assert!(first.as_mut().poll(cx).is_pending());
                Poll::Ready(())
            })
            .await;
        }
        assert_eq!(file.write(bytes).await.expect("second write"), bytes.len());
        drop(file);
        let contents = fs::read(&path).await.expect("read completed writes");
        fs::remove_file(&path).await.expect("remove fixture");
        contents
    });
    assert_eq!(contents, b"samesame");
}

#[test]
fn seek_rewinds_cancelled_read_overflow_to_the_logical_cursor() {
    use runite::io::AsyncRead;
    use std::future::poll_fn;
    use std::pin::Pin;
    use std::task::Poll;

    let path = temp_path("seek-read-overflow");
    block_on(move || async move {
        fs::write(&path, b"0123456789")
            .await
            .expect("write fixture");
        let mut file = File::open(&path).await.expect("open fixture");
        let mut large = [0; 10];
        poll_fn(|cx| {
            assert!(
                Pin::new(&mut file).poll_read(cx, &mut large).is_pending(),
                "first read poll must stay pending"
            );
            Poll::Ready(())
        })
        .await;

        let mut prefix = [0; 2];
        assert_eq!(file.read(&mut prefix).await.expect("finish old read"), 2);
        assert_eq!(&prefix, b"01");
        assert_eq!(
            file.seek(std::io::SeekFrom::Current(0))
                .await
                .expect("logical seek"),
            2
        );

        let mut next = [0; 2];
        assert_eq!(file.read(&mut next).await.expect("read after seek"), 2);
        assert_eq!(&next, b"23");
        fs::remove_file(&path).await.expect("remove fixture");
    });
}

#[test]
fn seek_waits_for_an_abandoned_sequential_write() {
    use std::future::{Future, poll_fn};
    use std::pin::pin;
    use std::task::Poll;

    let path = temp_path("seek-pending-write");
    let contents = block_on(move || async move {
        let mut file = File::create(&path).await.expect("create fixture");
        {
            let mut write = pin!(file.write(b"abc"));
            poll_fn(|cx| {
                assert!(write.as_mut().poll(cx).is_pending());
                Poll::Ready(())
            })
            .await;
        }

        assert_eq!(
            file.seek(std::io::SeekFrom::Start(0))
                .await
                .expect("seek after abandoned write"),
            0
        );
        file.write_all(b"Z").await.expect("overwrite first byte");
        drop(file);
        let contents = fs::read(&path).await.expect("read fixture");
        fs::remove_file(&path).await.expect("remove fixture");
        contents
    });
    assert_eq!(contents, b"Zbc");
}

#[test]
fn write_then_read_roundtrip() {
    let path = temp_path("roundtrip");
    let read_path = path.clone();
    let contents = block_on(move || async move {
        let mut file = File::create(&path).await.expect("create file");
        file.write_all(b"runite filesystem")
            .await
            .expect("write all");
        file.sync_all().await.expect("sync all");
        drop(file);

        let mut reopened = File::open(&read_path).await.expect("reopen file");
        let mut buf = Vec::new();
        reopened.read_to_end(&mut buf).await.expect("read to end");
        let _ = fs::remove_file(&read_path).await;
        buf
    });
    assert_eq!(contents, b"runite filesystem");
}

#[test]
fn read_only_open_with_truncate_is_rejected_and_preserves_contents() {
    let path = temp_path("read-truncate");
    let (errored, contents) = block_on(move || async move {
        let mut file = File::create(&path).await.expect("create file");
        file.write_all(b"keep me").await.expect("write initial");
        file.sync_all().await.expect("sync all");
        drop(file);

        // `read(true).truncate(true)` is an invalid combination: it must be
        // rejected rather than silently opening O_RDONLY | O_TRUNC (which would
        // truncate the file). Matches std and the macOS backend.
        let errored = OpenOptions::new()
            .read(true)
            .truncate(true)
            .open(&path)
            .await
            .is_err();

        let mut buf = Vec::new();
        File::open(&path)
            .await
            .expect("reopen file")
            .read_to_end(&mut buf)
            .await
            .expect("read to end");
        let _ = fs::remove_file(&path).await;
        (errored, buf)
    });

    assert!(errored, "read+truncate must be rejected");
    assert_eq!(contents, b"keep me", "file must not have been truncated");
}

#[test]
fn positional_read_and_metadata() {
    let path = temp_path("positional");
    let work_path = path.clone();
    let (len, slice) = block_on(move || async move {
        let mut file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(true)
            .open(&work_path)
            .await
            .expect("open file");
        file.write_all(b"0123456789").await.expect("write payload");
        file.sync_data().await.expect("sync data");

        let meta = file.metadata().await.expect("metadata");

        let mut buf = [0u8; 4];
        file.read_exact_at(2, &mut buf)
            .await
            .expect("positional read");

        let _ = fs::remove_file(&work_path).await;
        (meta.len(), buf)
    });

    assert_eq!(len, 10);
    assert_eq!(&slice, b"2345");
}

#[test]
fn read_and_write_free_functions() {
    let path = temp_path("free-fns");
    let work_path = path.clone();
    let text = block_on(move || async move {
        fs::write(&work_path, b"top-level helpers")
            .await
            .expect("fs::write");
        let text = fs::read_to_string(&work_path)
            .await
            .expect("fs::read_to_string");
        let _ = fs::remove_file(&work_path).await;
        text
    });
    assert_eq!(text, "top-level helpers");
}

#[test]
fn create_dir_all_accepts_existing_directory() {
    let path = temp_path("mkdirp-existing");
    let work_path = path.clone();
    let ok = block_on(move || async move {
        fs::create_dir_all(&work_path).await.expect("first create");
        // Re-creating an existing directory must succeed.
        let result = fs::create_dir_all(&work_path).await;
        let _ = fs::remove_dir(&work_path).await;
        result.is_ok()
    });
    assert!(ok);
}

#[test]
fn create_dir_all_rejects_existing_file() {
    let path = temp_path("mkdirp-file");
    let work_path = path.clone();
    let kind = block_on(move || async move {
        fs::write(&work_path, b"i am a file")
            .await
            .expect("write file");
        // The leaf path already exists as a regular file; this must error
        // rather than silently succeed.
        let result = fs::create_dir_all(&work_path).await;
        let _ = fs::remove_file(&work_path).await;
        result.err().map(|error| error.kind())
    });
    assert_eq!(kind, Some(std::io::ErrorKind::AlreadyExists));
}
