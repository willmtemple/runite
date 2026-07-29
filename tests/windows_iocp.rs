#![cfg(windows)]

use std::future::poll_fn;
use std::io;
use std::os::windows::fs::OpenOptionsExt;
use std::os::windows::io::{AsRawHandle, AsRawSocket, FromRawHandle, OwnedHandle, OwnedSocket};
use std::pin::Pin;
use std::sync::atomic::{AtomicU64, Ordering};
use std::task::Poll;
use std::time::Duration;

use runite::io::{AsyncRead, AsyncReadExt as _, AsyncSeekExt as _, AsyncWriteExt as _};
use windows_sys::Win32::Foundation::{ERROR_INVALID_PARAMETER, INVALID_HANDLE_VALUE};
use windows_sys::Win32::Storage::FileSystem::{
    FILE_FLAG_OVERLAPPED, SetFileCompletionNotificationModes,
};
use windows_sys::Win32::System::IO::CreateIoCompletionPort;

static NEXT_PATH_ID: AtomicU64 = AtomicU64::new(1);

fn fixture_path(name: &str) -> std::path::PathBuf {
    let id = NEXT_PATH_ID.fetch_add(1, Ordering::Relaxed);
    std::env::temp_dir().join(format!("runite-windows-{name}-{}-{id}", std::process::id()))
}

#[test]
fn iocp_identity_survives_sequential_block_on_and_run() {
    let path = fixture_path("sequential-runtime");
    let mut options = runite::fs::OpenOptions::new();
    options.read(true).write(true).create(true).truncate(true);
    let mut file = runite::block_on(options.open(&path)).expect("create fixture");

    runite::block_on(file.write_all(b"stable")).expect("first block_on writes");
    runite::run();
    runite::block_on(file.seek(io::SeekFrom::Start(0))).expect("second block_on seeks");

    let mut bytes = Vec::new();
    runite::block_on(file.read_to_end(&mut bytes)).expect("third block_on reads");
    assert_eq!(bytes, b"stable");

    drop(file);
    std::fs::remove_file(path).expect("remove fixture");
}

#[test]
fn synchronous_std_file_is_rejected() {
    let path = fixture_path("synchronous-adoption");
    std::fs::write(&path, b"sync").expect("write fixture");

    let file = std::fs::File::open(&path).expect("open synchronous std file");
    let error = match runite::fs::File::from_std(file) {
        Ok(_) => panic!("a synchronous handle must not be adopted by IOCP"),
        Err(error) => error,
    };
    assert_eq!(error.kind(), io::ErrorKind::InvalidInput);

    std::fs::remove_file(path).expect("remove fixture");
}

#[test]
fn skip_completion_on_success_std_file_is_rejected() {
    const FILE_SKIP_COMPLETION_PORT_ON_SUCCESS: u8 = 1;

    let path = fixture_path("skip-success-adoption");
    let file = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(true)
        .custom_flags(FILE_FLAG_OVERLAPPED)
        .open(&path)
        .expect("open overlapped std file");

    // SAFETY: `file` is a live overlapped handle and the flag is a documented
    // completion-notification mode.
    let configured = unsafe {
        SetFileCompletionNotificationModes(
            file.as_raw_handle(),
            FILE_SKIP_COMPLETION_PORT_ON_SUCCESS,
        )
    };
    assert_ne!(
        configured,
        0,
        "configure skip-on-success: {}",
        io::Error::last_os_error()
    );

    let error = match runite::fs::File::from_std(file) {
        Ok(_) => panic!("skip-on-success would violate one-packet-per-operation"),
        Err(error) => error,
    };
    assert_eq!(error.kind(), io::ErrorKind::InvalidInput);

    std::fs::remove_file(path).expect("remove fixture");
}

#[test]
fn skip_completion_on_success_std_socket_is_rejected() {
    const FILE_SKIP_COMPLETION_PORT_ON_SUCCESS: u8 = 1;

    let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("bind std listener");
    // SAFETY: Winsock sockets are kernel handles; the listener is live and std
    // creates it for overlapped I/O.
    let configured = unsafe {
        SetFileCompletionNotificationModes(
            listener.as_raw_socket() as usize as *mut core::ffi::c_void,
            FILE_SKIP_COMPLETION_PORT_ON_SUCCESS,
        )
    };
    assert_ne!(
        configured,
        0,
        "configure socket skip-on-success: {}",
        io::Error::last_os_error()
    );

    let error = match runite::net::TcpListener::from_std(listener) {
        Ok(_) => panic!("skip-on-success socket would violate packet ownership"),
        Err(error) => error,
    };
    assert_eq!(error.kind(), io::ErrorKind::InvalidInput);
}

#[test]
fn overlapped_std_file_common_adoption_works() {
    let path = fixture_path("overlapped-adoption");
    let file = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(true)
        .custom_flags(FILE_FLAG_OVERLAPPED)
        .open(&path)
        .expect("open overlapped std file");
    let mut file = runite::fs::File::from_std(file).expect("adopt overlapped std file");

    runite::block_on(file.write_all(b"adopted")).expect("write adopted file");
    runite::block_on(file.seek(io::SeekFrom::Start(0))).expect("seek adopted file");
    let mut bytes = Vec::new();
    runite::block_on(file.read_to_end(&mut bytes)).expect("read adopted file");
    assert_eq!(bytes, b"adopted");

    drop(file);
    std::fs::remove_file(path).expect("remove fixture");
}

#[test]
fn common_owned_socket_adoption_is_fallible_and_works() {
    let std_listener = std::net::TcpListener::bind("127.0.0.1:0").expect("bind std listener");
    let address = std_listener.local_addr().expect("std listener address");
    let listener =
        runite::net::TcpListener::from_owned(OwnedSocket::from(std_listener)).expect("from_owned");

    runite::block_on(async move {
        let server = runite::spawn(async move {
            listener.accept().await.expect("accept adopted listener");
        });
        let _client = runite::net::TcpStream::connect(address)
            .await
            .expect("connect adopted listener");
        server.await.expect("server task");
    });
}

#[test]
fn handle_bound_to_foreign_iocp_is_rejected() {
    let path = fixture_path("foreign-iocp");
    let file = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(true)
        .custom_flags(FILE_FLAG_OVERLAPPED)
        .open(&path)
        .expect("open overlapped fixture");

    // SAFETY: the documented INVALID_HANDLE_VALUE form creates a fresh IOCP.
    let foreign_raw =
        unsafe { CreateIoCompletionPort(INVALID_HANDLE_VALUE, std::ptr::null_mut(), 0, 1) };
    assert!(
        !foreign_raw.is_null(),
        "create foreign IOCP: {}",
        io::Error::last_os_error()
    );
    // SAFETY: `foreign_raw` is freshly created and exclusively owned.
    let foreign = unsafe { OwnedHandle::from_raw_handle(foreign_raw) };

    // SAFETY: `file` remains live, and association transfers no ownership.
    let associated =
        unsafe { CreateIoCompletionPort(file.as_raw_handle(), foreign.as_raw_handle(), 7, 0) };
    assert_eq!(associated, foreign.as_raw_handle());

    let error = match runite::fs::File::from_std(file) {
        Ok(_) => panic!("a handle already bound to another IOCP must be rejected"),
        Err(error) => error,
    };
    assert_eq!(error.raw_os_error(), Some(ERROR_INVALID_PARAMETER as i32));

    drop(foreign);
    std::fs::remove_file(path).expect("remove fixture");
}

#[test]
fn trusted_clone_shares_one_serialized_cursor() {
    let path = fixture_path("clone-cursor");
    let expected = (0..=199u8).collect::<Vec<_>>();
    std::fs::write(&path, &expected).expect("write fixture");

    let (left, right) = runite::block_on(async {
        let mut left = runite::fs::File::open(&path).await.expect("open fixture");
        let mut right = left.try_clone().await.expect("clone associated handle");

        let left_task = runite::spawn(async move {
            let mut seen = Vec::new();
            let mut byte = [0u8; 1];
            loop {
                match left.read(&mut byte).await.expect("left read") {
                    0 => break,
                    1 => seen.push(byte[0]),
                    _ => unreachable!("one-byte read returned more than one byte"),
                }
            }
            seen
        });
        let right_task = runite::spawn(async move {
            let mut seen = Vec::new();
            let mut byte = [0u8; 1];
            loop {
                match right.read(&mut byte).await.expect("right read") {
                    0 => break,
                    1 => seen.push(byte[0]),
                    _ => unreachable!("one-byte read returned more than one byte"),
                }
            }
            seen
        });

        (
            left_task.await.expect("left task"),
            right_task.await.expect("right task"),
        )
    });

    let mut observed = left;
    observed.extend(right);
    observed.sort_unstable();
    assert_eq!(observed, expected);

    std::fs::remove_file(path).expect("remove fixture");
}

#[test]
fn cloned_cursor_writes_and_seeks_are_serialized() {
    let path = fixture_path("clone-write-cursor");

    runite::block_on(async {
        let mut left = runite::fs::File::create(&path)
            .await
            .expect("create fixture");
        let mut right = left.try_clone().await.expect("clone associated handle");

        let left_task = runite::spawn(async move {
            for _ in 0..100 {
                left.write_all(b"a").await.expect("left write");
                left.seek(io::SeekFrom::Current(0))
                    .await
                    .expect("left seek");
            }
        });
        let right_task = runite::spawn(async move {
            for _ in 0..100 {
                right.write_all(b"b").await.expect("right write");
                right
                    .seek(io::SeekFrom::Current(0))
                    .await
                    .expect("right seek");
            }
        });

        left_task.await.expect("left task");
        right_task.await.expect("right task");
    });

    let bytes = std::fs::read(&path).expect("read fixture");
    assert_eq!(bytes.len(), 200);
    assert_eq!(bytes.iter().filter(|&&byte| byte == b'a').count(), 100);
    assert_eq!(bytes.iter().filter(|&&byte| byte == b'b').count(), 100);
    std::fs::remove_file(path).expect("remove fixture");
}

#[test]
fn dropped_overlapped_read_reaches_terminal_packet_before_reuse() {
    let (listener, address) = runite::block_on(async {
        let listener = runite::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind listener");
        let address = listener.local_addr().expect("listener address");
        (listener, address)
    });

    runite::block_on(async move {
        let client = runite::net::TcpStream::connect(address)
            .await
            .expect("connect client");
        let (server, _) = listener.accept().await.expect("accept client");
        let mut client = Box::pin(client);
        let mut byte = [0u8; 1];

        poll_fn(
            |cx| match AsyncRead::poll_read(Pin::as_mut(&mut client), cx, &mut byte) {
                Poll::Pending => Poll::Ready(()),
                Poll::Ready(result) => panic!("read unexpectedly completed: {result:?}"),
            },
        )
        .await;

        drop(client);
        drop(server);
    });

    for _ in 0..256 {
        let socket = runite::net::UdpSocket::bind("127.0.0.1:0");
        drop(runite::block_on(socket).expect("churn socket"));
    }

    // The cancelled read remains runtime liveness until IOCP dequeues its
    // terminal packet. This must return without dispatching into a reused
    // handle or leaked packet context.
    runite::run();
}

#[test]
fn deadline_cancellation_is_terminal_and_completion_can_win() {
    runite::block_on(async {
        let listener = runite::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind listener");
        let address = listener.local_addr().expect("listener address");

        let server = runite::spawn(async move {
            let (mut stream, _) = listener.accept().await.expect("accept client");
            stream.write_all(b"x").await.expect("write boundary byte");
            runite::time::sleep(Duration::from_millis(100)).await;
        });

        let mut client = runite::net::TcpStream::connect(address)
            .await
            .expect("connect client");
        client
            .set_read_timeout(Some(Duration::from_secs(1)))
            .expect("set read deadline");
        let mut byte = [0u8; 1];
        assert_eq!(client.read(&mut byte).await.expect("completion wins"), 1);
        assert_eq!(byte, *b"x");
        client
            .set_read_timeout(Some(Duration::from_millis(1)))
            .expect("set short deadline");
        let error = client
            .read(&mut byte)
            .await
            .expect_err("idle read should time out");
        assert_eq!(error.kind(), io::ErrorKind::TimedOut);
        server.await.expect("server task");
    });

    // A timeout result is not published until the cancelled operation's
    // terminal packet has been observed, so no pending IOCP work remains.
    runite::run();
}
