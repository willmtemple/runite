//! Asynchronous I/O traits, adapters, and stream utilities.
//!
//! The `io` module defines runite's small current-thread I/O abstraction layer:
//! [`AsyncRead`] and [`AsyncWrite`] are poll-based byte traits, extension traits
//! turn those poll methods into futures, [`Stream`] represents asynchronous
//! sequences, and [`BufReader`]/[`BufWriter`] amortize small reads and writes.
//!
//! Runite futures are thread-local and are driven by the current thread's event
//! loop. Doctest examples in this module use [`crate::queue_future`] followed by
//! [`crate::run`] to execute async work until the loop is idle.
//!
//! # Examples
//!
//! ```
//! use core::pin::Pin;
//! use core::task::{Context, Poll};
//! use std::io;
//!
//! use runite::io::{AsyncRead, AsyncReadExt, BufReader};
//!
//! struct Bytes(&'static [u8]);
//!
//! impl AsyncRead for Bytes {
//!     fn poll_read(
//!         mut self: Pin<&mut Self>,
//!         _cx: &mut Context<'_>,
//!         buf: &mut [u8],
//!     ) -> Poll<io::Result<usize>> {
//!         let read = buf.len().min(self.0.len());
//!         buf[..read].copy_from_slice(&self.0[..read]);
//!         self.0 = &self.0[read..];
//!         Poll::Ready(Ok(read))
//!     }
//! }
//!
//! runite::queue_future(async {
//!     let mut reader = BufReader::with_capacity(4, Bytes(b"hello"));
//!     let mut out = Vec::new();
//!     reader.read_to_end(&mut out).await.unwrap();
//!     assert_eq!(&out, b"hello");
//! });
//! runite::run();
//! ```

mod buf;
#[cfg(feature = "futures-compat")]
pub mod compat;
mod ext;
mod stream;
mod traits;

pub use buf::{BufReader, BufWriter};
pub use ext::{
    AsyncReadExt, AsyncWriteExt, Copy, CopyBidirectional, Lines, copy, copy_bidirectional,
};
pub use stream::{Collect, Filter, ForEach, Map, Next, Skip, Stream, StreamExt, Take};
pub use traits::{AsyncRead, AsyncWrite};

#[cfg(test)]
mod tests {
    use std::path::PathBuf;
    use std::process;
    use std::sync::{Arc, Mutex, OnceLock};
    use std::time::{SystemTime, UNIX_EPOCH};

    use crate::fs::{self, OpenOptions};
    use crate::net::{TcpListener, TcpStream};
    use crate::{queue_future, queue_task, run};

    use super::{AsyncReadExt, AsyncWriteExt, StreamExt};

    fn test_lock() -> &'static Mutex<()> {
        static LOCK: OnceLock<Mutex<()>> = OnceLock::new();
        LOCK.get_or_init(|| Mutex::new(()))
    }

    fn unique_path(label: &str) -> PathBuf {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system time should be after epoch")
            .as_nanos();
        let root = std::env::current_dir()
            .expect("current dir should be available")
            .join("target")
            .join("runtime-io-tests");
        std::fs::create_dir_all(&root).expect("test artifact directory should be created");
        root.join(format!("runite-{label}-{}-{nanos}", process::id()))
    }

    #[test]
    fn async_read_ext_read_works_on_file() {
        let _guard = test_lock().lock().unwrap();
        let path = unique_path("async-read-ext-file");
        let observed = Arc::new(Mutex::new(None::<Vec<u8>>));

        {
            let observed = Arc::clone(&observed);
            queue_task(move || {
                queue_future(async move {
                    fs::write(&path, b"trait bytes")
                        .await
                        .expect("fixture write should succeed");
                    let mut file = OpenOptions::new()
                        .read(true)
                        .open(&path)
                        .await
                        .expect("file should open");
                    let mut buf = [0u8; 5];
                    let read = AsyncReadExt::read(&mut file, &mut buf)
                        .await
                        .expect("extension read should succeed");
                    *observed.lock().unwrap() = Some(buf[..read].to_vec());
                    fs::remove_file(&path)
                        .await
                        .expect("cleanup should succeed");
                });
            });
        }

        run();
        assert_eq!(
            observed.lock().unwrap().as_deref(),
            Some(b"trait".as_slice())
        );
    }

    #[test]
    fn async_read_ext_read_to_end_works_on_tcp_stream() {
        let received = Arc::new(Mutex::new(None::<Vec<u8>>));
        let received_for_task = Arc::clone(&received);

        queue_task(move || {
            queue_future(async move {
                let listener = Arc::new(
                    TcpListener::bind(std::net::SocketAddr::from(([127, 0, 0, 1], 0)))
                        .await
                        .expect("listener should bind"),
                );
                let local_addr = listener
                    .local_addr()
                    .expect("listener address should exist");
                let listener_for_accept = Arc::clone(&listener);

                let server = queue_future(async move {
                    let (mut stream, _) = listener_for_accept
                        .accept()
                        .await
                        .expect("listener should accept");
                    AsyncWriteExt::write_all(&mut stream, b"hello over traits")
                        .await
                        .expect("server write should succeed");
                });

                let mut client = TcpStream::connect(local_addr)
                    .await
                    .expect("client should connect");
                let mut out = Vec::new();
                let read = AsyncReadExt::read_to_end(&mut client, &mut out)
                    .await
                    .expect("client read_to_end should succeed");
                assert_eq!(read, b"hello over traits".len());
                server.await.expect("server task should not be aborted");
                *received_for_task.lock().unwrap() = Some(out);
            });
        });

        run();
        assert_eq!(
            received.lock().unwrap().as_deref(),
            Some(b"hello over traits".as_slice())
        );
    }

    #[test]
    fn async_write_ext_write_all_writes_full_buffer() {
        let received = Arc::new(Mutex::new(None::<Vec<u8>>));
        let received_for_task = Arc::clone(&received);

        queue_task(move || {
            queue_future(async move {
                let listener = Arc::new(
                    TcpListener::bind(std::net::SocketAddr::from(([127, 0, 0, 1], 0)))
                        .await
                        .expect("listener should bind"),
                );
                let local_addr = listener
                    .local_addr()
                    .expect("listener address should exist");
                let listener_for_accept = Arc::clone(&listener);
                let expected_len = 64 * 1024;

                let server = queue_future(async move {
                    let (mut stream, _) = listener_for_accept
                        .accept()
                        .await
                        .expect("listener should accept");
                    let mut received = Vec::new();
                    while received.len() < expected_len {
                        let mut chunk = [0u8; 4096];
                        let read = AsyncReadExt::read(&mut stream, &mut chunk)
                            .await
                            .expect("server read should succeed");
                        if read == 0 {
                            break;
                        }
                        received.extend_from_slice(&chunk[..read]);
                    }
                    received
                });

                let mut client = TcpStream::connect(local_addr)
                    .await
                    .expect("client should connect");
                let data = (0..expected_len)
                    .map(|index| (index % 251) as u8)
                    .collect::<Vec<_>>();
                AsyncWriteExt::write_all(&mut client, &data)
                    .await
                    .expect("client write_all should succeed");
                let server_received = server.await.expect("server task should not be aborted");
                assert_eq!(server_received, data);
                *received_for_task.lock().unwrap() = Some(server_received);
            });
        });

        run();
        assert_eq!(
            received.lock().unwrap().as_ref().map(Vec::len),
            Some(64 * 1024)
        );
    }

    #[test]
    fn async_read_lines_yields_lines() {
        let _guard = test_lock().lock().unwrap();
        let path = unique_path("async-read-lines-file");
        let observed = Arc::new(Mutex::new(None::<Vec<String>>));

        {
            let observed = Arc::clone(&observed);
            queue_task(move || {
                queue_future(async move {
                    fs::write(&path, b"alpha\nbeta\ngamma")
                        .await
                        .expect("fixture write should succeed");
                    let file = OpenOptions::new()
                        .read(true)
                        .open(&path)
                        .await
                        .expect("file should open");
                    let lines = file
                        .lines()
                        .collect::<Vec<_>>()
                        .await
                        .into_iter()
                        .collect::<Result<Vec<_>, _>>()
                        .expect("lines should read successfully");
                    *observed.lock().unwrap() = Some(lines);
                    fs::remove_file(&path)
                        .await
                        .expect("cleanup should succeed");
                });
            });
        }

        run();
        assert_eq!(
            *observed.lock().unwrap(),
            Some(vec![
                "alpha".to_string(),
                "beta".to_string(),
                "gamma".to_string()
            ])
        );
    }
}
