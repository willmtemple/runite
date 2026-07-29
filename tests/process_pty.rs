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

    // Drop the last user-side descriptor so the controller reports end of
    // input once the buffered output is drained.
    drop(user);
    let output = read_until(controller.as_raw_fd(), "IS_A_TTY");
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
#[test]
fn pre_exec_gives_the_child_a_controlling_terminal() {
    let (controller, user) = open_pty();
    let user_raw = user.as_raw_fd();
    let (child_in, child_out, child_err) = (dup(&user), dup(&user), dup(&user));

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
                if libc::ioctl(user_raw, libc::TIOCSCTTY, 0) == -1 {
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

    drop(user);
    let output = read_until(controller.as_raw_fd(), "HAS_CTTY");
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

    drop(user);
    let output = read_until(controller.as_raw_fd(), "ROUND");
    assert!(
        output.contains("ROUND"),
        "each spawn should reach the controller, saw {output:?}"
    );
}
