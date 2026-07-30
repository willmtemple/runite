//! Starting a child on a pseudoterminal: `Stdio::from(OwnedFd)` plus
//! `CommandExt::pre_exec`.
//!
//! This is the pair a terminal multiplexer needs. The I/O half — driving the
//! controller — is already covered by `runite::fd`; what these tests exercise
//! is the spawn half, which had no API before.

#![cfg(unix)]

mod common;

use std::io;
use std::os::fd::{AsRawFd, FromRawFd, OwnedFd, RawFd};
use std::time::{Duration, Instant};

use common::block_on;
use runite::os::unix::process::CommandExt;
use runite::process::{Command, Stdio};

const READ_TIMEOUT: Duration = Duration::from_secs(10);

/// Opens a pseudoterminal pair, returning `(controller, user)` — the halves
/// POSIX calls master and slave.
fn open_pty() -> (OwnedFd, OwnedFd) {
    let mut controller = -1;
    let mut user = -1;
    // SAFETY: both out-pointers are valid. Null optional pointers request the
    // default name, termios, and window size.
    let rc = unsafe {
        libc::openpty(
            &mut controller,
            &mut user,
            std::ptr::null_mut(),
            std::ptr::null_mut(),
            std::ptr::null_mut(),
        )
    };
    assert_eq!(
        rc,
        0,
        "openpty should succeed: {}",
        io::Error::last_os_error()
    );
    // SAFETY: `openpty` initialized both with owned, open descriptors.
    unsafe { (OwnedFd::from_raw_fd(controller), OwnedFd::from_raw_fd(user)) }
}

fn dup(fd: &OwnedFd) -> OwnedFd {
    fd.try_clone().expect("descriptor should duplicate")
}

/// Blocking read of the controller until `needle` appears or the child's exit
/// closes the line.
///
/// A controller whose last user-side descriptor has closed reports `EIO` on
/// Linux rather than end of file, which is the signal a multiplexer actually
/// keys on; both are treated as "no more output" here.
fn read_until(controller: RawFd, needle: &str) -> String {
    let deadline = Instant::now() + READ_TIMEOUT;
    let mut seen = String::new();
    let mut chunk = [0u8; 4096];
    while Instant::now() < deadline {
        // SAFETY: `controller` is open for the call, and `chunk` points to
        // `chunk.len()` writable bytes.
        let read = unsafe {
            libc::read(
                controller,
                chunk.as_mut_ptr().cast::<libc::c_void>(),
                chunk.len(),
            )
        };
        if read > 0 {
            seen.push_str(&String::from_utf8_lossy(&chunk[..read as usize]));
            if seen.contains(needle) {
                return seen;
            }
            continue;
        }
        if read == 0 {
            break;
        }
        let error = io::Error::last_os_error();
        match error.kind() {
            io::ErrorKind::Interrupted => continue,
            // The user side is gone: the child exited.
            _ if error.raw_os_error() == Some(libc::EIO) => break,
            _ => panic!("controller read failed: {error}"),
        }
    }
    seen
}

/// A child whose standard streams are the user side of a pseudoterminal sees a
/// terminal, and its output reaches the controller.
#[test]
fn stdio_from_fd_puts_the_child_on_a_terminal() {
    let (controller, user) = open_pty();
    let (child_in, child_out, child_err) = (dup(&user), dup(&user), dup(&user));

    let status = block_on(move || async move {
        let mut command = Command::new("sh");
        command
            .arg("-c")
            .arg("test -t 1 && echo IS_A_TTY")
            .stdin(Stdio::from(child_in))
            .stdout(Stdio::from(child_out))
            .stderr(Stdio::from(child_err));

        let mut child = command.spawn().expect("child should spawn on the pty");
        child.wait().await.expect("child should exit")
    });

    assert!(status.success(), "`test -t 1` should succeed on a pty");

    // Drain before dropping the last user-side descriptor: `read_until` stops
    // at the needle rather than at end of input, and BSD ptys discard the
    // pending output queue when the last user-side descriptor closes.
    let output = read_until(controller.as_raw_fd(), "IS_A_TTY");
    drop(user);
    assert!(
        output.contains("IS_A_TTY"),
        "child stdout should reach the controller, saw {output:?}"
    );
}

/// `pre_exec` runs between fork and exec, so `setsid` plus `TIOCSCTTY` can give
/// the child a controlling terminal.
///
/// Opening `/dev/tty` succeeds only for a process that has one, so writing
/// through it is a direct test of the acquisition rather than of inheritance.
///
/// The controller is drained on a separate thread, concurrently with the
/// child's exit, rather than after it. Because the child owns the pty as its
/// controlling terminal, exit runs the terminal's teardown: a session leader's
/// last close drains the output queue before the line is torn down, and that
/// drain only advances while something reads the controller. Reading only after
/// `wait` would wedge the child mid-exit and hang `wait` forever. The concurrent
/// read is the correct pattern on every platform — Linux merely tolerates the
/// after-the-fact read that BSD-derived kernels (including macOS) deadlock on.
#[test]
fn pre_exec_gives_the_child_a_controlling_terminal() {
    let (controller, user) = open_pty();
    let user_raw = user.as_raw_fd();
    let (child_in, child_out, child_err) = (dup(&user), dup(&user), dup(&user));

    let controller_raw = controller.as_raw_fd();
    let reader = std::thread::spawn(move || read_until(controller_raw, "HAS_CTTY"));

    let status = block_on(move || async move {
        let mut command = Command::new("sh");
        command
            .arg("-c")
            .arg("echo HAS_CTTY > /dev/tty")
            .stdin(Stdio::from(child_in))
            .stdout(Stdio::from(child_out))
            .stderr(Stdio::from(child_err));

        // SAFETY: `setsid` and `ioctl` are async-signal-safe, and neither
        // allocates nor takes a lock, so both are sound between fork and exec.
        unsafe {
            command.pre_exec(move || {
                if libc::setsid() == -1 {
                    return Err(io::Error::last_os_error());
                }
                // `TIOCSCTTY` is `c_uint` on Apple but the request type
                // (`c_ulong`) elsewhere, so `.into()` is a real widening on
                // macOS and a no-op on Linux; let inference pick the target
                // rather than hard-coding a type that breaks on musl.
                #[allow(clippy::useless_conversion)]
                if libc::ioctl(user_raw, libc::TIOCSCTTY.into(), 0) == -1 {
                    return Err(io::Error::last_os_error());
                }
                Ok(())
            });
        }

        let mut child = command.spawn().expect("child should spawn");
        child.wait().await.expect("child should exit")
    });

    assert!(
        status.success(),
        "writing to /dev/tty should succeed once the child owns the terminal"
    );

    // The controller was drained on `reader` while the child exited; joining it
    // collects what reached the controller. Keeping `user` open until here holds
    // the line up so that read cannot race the teardown to end-of-input.
    let output = reader.join().expect("controller reader should not panic");
    drop(user);
    assert!(
        output.contains("HAS_CTTY"),
        "the child's /dev/tty write should reach the controller, saw {output:?}"
    );
}

/// An error from the hook aborts the spawn and surfaces as the spawn error,
/// rather than leaving a half-started child behind.
#[test]
fn pre_exec_failure_fails_the_spawn() {
    let error = block_on(|| async {
        let mut command = Command::new("sh");
        command.arg("-c").arg("exit 0").stdout(Stdio::null());

        // SAFETY: the hook only constructs an error value; it does not
        // allocate, take a lock, or call anything unsafe.
        unsafe {
            command.pre_exec(|| Err(io::Error::from_raw_os_error(libc::EACCES)));
        }

        command
            .spawn()
            .expect_err("spawn should fail when the hook fails")
    });

    assert_eq!(error.raw_os_error(), Some(libc::EACCES));
}

/// Appends one byte to `path` using only async-signal-safe calls, so the
/// closure is legal between `fork` and `exec`. `path` is built in the parent;
/// the child only reads it.
fn append_marker(path: &std::ffi::CStr, byte: u8) -> io::Result<()> {
    // SAFETY: `path` is a live NUL-terminated string and the mode is only read
    // when the file is created.
    let fd = unsafe {
        libc::open(
            path.as_ptr(),
            libc::O_WRONLY | libc::O_CREAT | libc::O_APPEND,
            0o600,
        )
    };
    if fd < 0 {
        return Err(io::Error::last_os_error());
    }
    // SAFETY: `fd` is open for writing and `byte` is one readable byte.
    let written = unsafe { libc::write(fd, std::ptr::from_ref(&byte).cast::<libc::c_void>(), 1) };
    // SAFETY: `fd` came from the `open` above and is not used again.
    unsafe { libc::close(fd) };
    if written != 1 {
        return Err(io::Error::last_os_error());
    }
    Ok(())
}

/// Registering a second hook does not discard the first: both run, in
/// registration order, the way `std::os::unix::process::CommandExt` composes.
/// A layered builder that adds a hook on top of one already there must not
/// silently lose it.
#[test]
fn pre_exec_hooks_chain_in_registration_order() {
    // `temp_dir`, not `target/`: this path is also a `sun_path` habit, and a
    // packaged crate unpacks somewhere much deeper than the source tree.
    let marker = std::env::temp_dir().join(format!(
        "runite-pre-exec-chain-{}-{:?}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("clock should be after the epoch")
            .as_nanos()
    ));
    let _ = std::fs::remove_file(&marker);
    let path = std::ffi::CString::new(marker.as_os_str().as_encoded_bytes())
        .expect("a temp path should not contain NUL");

    let status = block_on({
        let path = path.clone();
        move || async move {
            let mut command = Command::new("sh");
            command.arg("-c").arg("exit 0").stdout(Stdio::null());

            let first = path.clone();
            let second = path;
            // SAFETY: `open`, `write` and `close` are async-signal-safe, and
            // neither hook allocates nor takes a lock — the path is a `CString`
            // built in the parent.
            unsafe {
                command.pre_exec(move || append_marker(&first, b'a'));
                command.pre_exec(move || append_marker(&second, b'b'));
            }

            command
                .spawn()
                .expect("spawn should succeed")
                .wait()
                .await
                .expect("child should exit")
        }
    });

    let recorded = std::fs::read(&marker).unwrap_or_default();
    let _ = std::fs::remove_file(&marker);
    assert!(status.success(), "the child should exit cleanly");
    assert_eq!(
        String::from_utf8_lossy(&recorded),
        "ab",
        "both hooks should run, first-registered first"
    );
}

/// The descriptor is duplicated per spawn, so one `Command` can start several
/// children and the caller keeps its original.
#[test]
fn descriptor_stdio_survives_repeated_spawns() {
    let (controller, user) = open_pty();
    let (child_in, child_out, child_err) = (dup(&user), dup(&user), dup(&user));

    block_on(move || async move {
        let mut command = Command::new("sh");
        command
            .arg("-c")
            .arg("echo ROUND")
            .stdin(Stdio::from(child_in))
            .stdout(Stdio::from(child_out))
            .stderr(Stdio::from(child_err));

        for round in 0..3 {
            let mut child = command
                .spawn()
                .unwrap_or_else(|error| panic!("spawn {round} should succeed: {error}"));
            let status = child.wait().await.expect("child should exit");
            assert!(status.success(), "spawn {round} should exit cleanly");
        }
    });

    // The caller's own descriptor is still open and usable.
    assert!(
        user.try_clone().is_ok(),
        "the caller's descriptor should be untouched by spawning"
    );

    // Drained before the last user-side descriptor closes; see the note in
    // `stdio_from_fd_puts_the_child_on_a_terminal`.
    let output = read_until(controller.as_raw_fd(), "ROUND");
    drop(user);
    assert!(
        output.contains("ROUND"),
        "each spawn should reach the controller, saw {output:?}"
    );
}
