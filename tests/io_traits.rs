//! Conformance tests for runite's portable asynchronous I/O traits.

mod common;

use core::future::Future;
use core::pin::{Pin, pin};
use core::task::{Context, Poll, Waker};
use runite::io::{
    AsyncBufRead, AsyncRead, AsyncReadExt, AsyncSeek, AsyncSeekExt, AsyncWrite, AsyncWriteExt,
    BufReader, SeekFrom,
};
use std::io::{self, IoSlice, IoSliceMut, Read as _, Seek as _};

fn poll_once<F: Future>(future: Pin<&mut F>) -> Poll<F::Output> {
    let mut cx = Context::from_waker(Waker::noop());
    future.poll(&mut cx)
}

fn temp_path(label: &str) -> std::path::PathBuf {
    std::env::current_dir()
        .expect("current directory")
        .join("target")
        .join(format!(
            "runite-io-traits-{}-{}-{label}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .expect("clock after epoch")
                .as_nanos()
        ))
}

struct ScalarReader {
    bytes: Vec<u8>,
    position: usize,
    max_read: usize,
    calls: usize,
}

impl ScalarReader {
    fn new(bytes: &[u8], max_read: usize) -> Self {
        Self {
            bytes: bytes.to_vec(),
            position: 0,
            max_read,
            calls: 0,
        }
    }
}

impl AsyncRead for ScalarReader {
    fn poll_read(
        mut self: Pin<&mut Self>,
        _cx: &mut Context<'_>,
        buf: &mut [u8],
    ) -> Poll<io::Result<usize>> {
        self.calls += 1;
        let read = buf
            .len()
            .min(self.max_read)
            .min(self.bytes.len() - self.position);
        let start = self.position;
        buf[..read].copy_from_slice(&self.bytes[start..start + read]);
        self.position += read;
        Poll::Ready(Ok(read))
    }
}

#[derive(Default)]
struct ScalarWriter {
    bytes: Vec<u8>,
    max_write: usize,
    calls: usize,
}

impl AsyncWrite for ScalarWriter {
    fn poll_write(
        mut self: Pin<&mut Self>,
        _cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        self.calls += 1;
        let written = buf.len().min(self.max_write);
        self.bytes.extend_from_slice(&buf[..written]);
        Poll::Ready(Ok(written))
    }

    fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Poll::Ready(Ok(()))
    }

    fn poll_close(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Poll::Ready(Ok(()))
    }
}

#[derive(Default)]
struct VectoredWriter {
    bytes: Vec<u8>,
}

impl AsyncWrite for VectoredWriter {
    fn poll_write(
        self: Pin<&mut Self>,
        _cx: &mut Context<'_>,
        _buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        panic!("optimized vectored write must not use the scalar fallback")
    }

    fn poll_write_vectored(
        mut self: Pin<&mut Self>,
        _cx: &mut Context<'_>,
        bufs: &[IoSlice<'_>],
    ) -> Poll<io::Result<usize>> {
        let mut written = 0;
        for buf in bufs {
            self.bytes.extend_from_slice(buf);
            written += buf.len();
        }
        Poll::Ready(Ok(written))
    }

    fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Poll::Ready(Ok(()))
    }

    fn poll_close(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Poll::Ready(Ok(()))
    }
}

#[test]
fn scalar_vectored_defaults_preserve_partial_progress_and_empty_semantics() {
    let mut reader = ScalarReader::new(b"abcdef", 2);
    let mut empty = [];
    let mut first = [0; 3];
    let mut second = [0; 3];
    {
        let mut bufs = [
            IoSliceMut::new(&mut empty),
            IoSliceMut::new(&mut first),
            IoSliceMut::new(&mut second),
        ];
        let mut read = pin!(reader.read_vectored(&mut bufs));
        assert!(matches!(poll_once(read.as_mut()), Poll::Ready(Ok(2))));
    }
    assert_eq!(&first, b"ab\0");
    assert_eq!(&second, b"\0\0\0");
    assert_eq!(reader.calls, 1);

    let mut no_read_buffers: [IoSliceMut<'_>; 0] = [];
    {
        let mut read = pin!(reader.read_vectored(&mut no_read_buffers));
        assert!(matches!(poll_once(read.as_mut()), Poll::Ready(Ok(0))));
    }
    let (mut empty_a, mut empty_b) = ([], []);
    {
        let mut empty_bufs = [IoSliceMut::new(&mut empty_a), IoSliceMut::new(&mut empty_b)];
        let mut read = pin!(reader.read_vectored(&mut empty_bufs));
        assert!(matches!(poll_once(read.as_mut()), Poll::Ready(Ok(0))));
    }
    assert_eq!(reader.calls, 1, "empty reads must not poll the scalar path");

    let mut writer = ScalarWriter {
        max_write: 2,
        ..ScalarWriter::default()
    };
    {
        let bufs = [
            IoSlice::new(b""),
            IoSlice::new(b"abc"),
            IoSlice::new(b"def"),
        ];
        let mut write = pin!(writer.write_vectored(&bufs));
        assert!(matches!(poll_once(write.as_mut()), Poll::Ready(Ok(2))));
    }
    assert_eq!(writer.bytes, b"ab");
    assert_eq!(writer.calls, 1);

    let no_write_buffers: [IoSlice<'_>; 0] = [];
    {
        let mut write = pin!(writer.write_vectored(&no_write_buffers));
        assert!(matches!(poll_once(write.as_mut()), Poll::Ready(Ok(0))));
    }
    let empty_bufs = [IoSlice::new(b""), IoSlice::new(b"")];
    {
        let mut write = pin!(writer.write_vectored(&empty_bufs));
        assert!(matches!(poll_once(write.as_mut()), Poll::Ready(Ok(0))));
    }
    assert_eq!(
        writer.calls, 1,
        "empty writes must not poll the scalar path"
    );

    let mut optimized = VectoredWriter::default();
    let bufs = [IoSlice::new(b"ab"), IoSlice::new(b"cd")];
    {
        let mut write = pin!(optimized.write_vectored(&bufs));
        assert!(matches!(poll_once(write.as_mut()), Poll::Ready(Ok(4))));
    }
    assert_eq!(optimized.bytes, b"abcd");
}

#[test]
fn buf_reader_implements_async_buf_read_through_eof() {
    let inner = ScalarReader::new(b"abcdef", usize::MAX);
    let mut reader = BufReader::with_capacity(4, inner);
    let mut cx = Context::from_waker(Waker::noop());

    let first = match AsyncBufRead::poll_fill_buf(Pin::new(&mut reader), &mut cx) {
        Poll::Ready(Ok(bytes)) => bytes.to_vec(),
        other => panic!("expected buffered bytes, got {other:?}"),
    };
    assert_eq!(first, b"abcd");

    AsyncBufRead::consume(Pin::new(&mut reader), 3);
    let remaining = match AsyncBufRead::poll_fill_buf(Pin::new(&mut reader), &mut cx) {
        Poll::Ready(Ok(bytes)) => bytes.to_vec(),
        other => panic!("expected remaining byte, got {other:?}"),
    };
    assert_eq!(remaining, b"d");

    AsyncBufRead::consume(Pin::new(&mut reader), usize::MAX);
    let second = match AsyncBufRead::poll_fill_buf(Pin::new(&mut reader), &mut cx) {
        Poll::Ready(Ok(bytes)) => bytes.to_vec(),
        other => panic!("expected second fill, got {other:?}"),
    };
    assert_eq!(second, b"ef");

    AsyncBufRead::consume(Pin::new(&mut reader), second.len());
    assert!(matches!(
        AsyncBufRead::poll_fill_buf(Pin::new(&mut reader), &mut cx),
        Poll::Ready(Ok(bytes)) if bytes.is_empty()
    ));
}

struct ParkingLineReader {
    state: u8,
}

impl AsyncRead for ParkingLineReader {
    fn poll_read(
        mut self: Pin<&mut Self>,
        _cx: &mut Context<'_>,
        buf: &mut [u8],
    ) -> Poll<io::Result<usize>> {
        match self.state {
            0 => {
                self.state = 1;
                buf[..3].copy_from_slice(b"abc");
                Poll::Ready(Ok(3))
            }
            1 => {
                self.state = 2;
                Poll::Pending
            }
            2 => {
                self.state = 3;
                buf[0] = b'\n';
                Poll::Ready(Ok(1))
            }
            _ => Poll::Ready(Ok(0)),
        }
    }
}

#[test]
fn cancelled_buf_reader_read_line_keeps_its_consumed_prefix_recoverable() {
    let mut reader = BufReader::with_capacity(3, ParkingLineReader { state: 0 });
    let mut abandoned = String::new();
    {
        let mut read_line = pin!(reader.read_line(&mut abandoned));
        assert!(poll_once(read_line.as_mut()).is_pending());
    }

    assert_eq!(abandoned, "");
    assert_eq!(
        reader.buffer(),
        b"abc",
        "bytes consumed before parking must remain visible after cancellation"
    );

    let mut recovered = String::new();
    {
        let mut read_line = pin!(reader.read_line(&mut recovered));
        assert!(matches!(poll_once(read_line.as_mut()), Poll::Ready(Ok(4))));
    }
    assert_eq!(recovered, "abc\n");
}

#[test]
fn zero_capacity_buf_reader_still_supports_lines_eof_and_invalid_utf8() {
    let mut reader = BufReader::with_capacity(0, ScalarReader::new(b"x\nlast", usize::MAX));
    let mut first = String::new();
    {
        let mut first_read = pin!(reader.read_line(&mut first));
        assert!(matches!(poll_once(first_read.as_mut()), Poll::Ready(Ok(2))));
    }
    assert_eq!(first, "x\n");

    let mut last = String::new();
    {
        let mut last_read = pin!(reader.read_line(&mut last));
        assert!(matches!(poll_once(last_read.as_mut()), Poll::Ready(Ok(4))));
    }
    assert_eq!(last, "last");

    let mut eof = String::new();
    let mut eof_read = pin!(reader.read_line(&mut eof));
    assert!(matches!(poll_once(eof_read.as_mut()), Poll::Ready(Ok(0))));

    let mut invalid = BufReader::with_capacity(0, ScalarReader::new(b"\xff\n", usize::MAX));
    let mut output = String::from("unchanged");
    {
        let mut invalid_read = pin!(invalid.read_line(&mut output));
        assert!(matches!(
            poll_once(invalid_read.as_mut()),
            Poll::Ready(Err(error)) if error.kind() == io::ErrorKind::InvalidData
        ));
    }
    assert_eq!(output, "unchanged");
}

struct SeekableReader {
    cursor: std::io::Cursor<Vec<u8>>,
    park_seek: bool,
    seek_calls: Vec<SeekFrom>,
}

impl AsyncRead for SeekableReader {
    fn poll_read(
        mut self: Pin<&mut Self>,
        _cx: &mut Context<'_>,
        buf: &mut [u8],
    ) -> Poll<io::Result<usize>> {
        Poll::Ready(self.cursor.read(buf))
    }
}

impl AsyncSeek for SeekableReader {
    fn poll_seek(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        position: SeekFrom,
    ) -> Poll<io::Result<u64>> {
        self.seek_calls.push(position);
        if self.park_seek {
            self.park_seek = false;
            cx.waker().wake_by_ref();
            return Poll::Pending;
        }
        Poll::Ready(self.cursor.seek(position))
    }
}

#[test]
fn buf_reader_seek_accounts_for_buffered_data_and_pending_seek() {
    let inner = SeekableReader {
        cursor: std::io::Cursor::new(b"012345".to_vec()),
        park_seek: true,
        seek_calls: Vec::new(),
    };
    let mut reader = BufReader::with_capacity(4, inner);

    let mut prefix = [0; 2];
    {
        let mut read = pin!(reader.read_exact(&mut prefix));
        assert!(matches!(poll_once(read.as_mut()), Poll::Ready(Ok(()))));
    }
    assert_eq!(&prefix, b"01");
    assert_eq!(reader.buffer(), b"23");

    {
        let mut seek = pin!(reader.seek(SeekFrom::Current(0)));
        assert!(poll_once(seek.as_mut()).is_pending());
    }
    assert_eq!(reader.buffer(), b"23");
    {
        let mut seek = pin!(reader.seek(SeekFrom::Current(0)));
        assert!(matches!(poll_once(seek.as_mut()), Poll::Ready(Ok(2))));
    }
    assert!(reader.buffer().is_empty());
    assert_eq!(
        reader.get_ref().seek_calls,
        [SeekFrom::Current(-2), SeekFrom::Current(-2)]
    );

    let mut next = [0; 2];
    let mut read = pin!(reader.read_exact(&mut next));
    assert!(matches!(poll_once(read.as_mut()), Poll::Ready(Ok(()))));
    assert_eq!(&next, b"23");
}

#[test]
fn file_async_seek_reconciles_an_accepted_pending_read() {
    use core::future::poll_fn;
    use runite::fs::{self, File};

    let path = temp_path("async-seek");

    common::block_on(move || async move {
        fs::write(&path, b"0123456789")
            .await
            .expect("write fixture");
        let mut file = File::open(&path).await.expect("open fixture");
        let mut large = [0; 10];
        // Submit a read and abandon it without observing the result. Whether it
        // is still in flight afterwards is a backend property -- io_uring parks,
        // while the blocking-pool backends can finish first -- so branch on what
        // actually happened rather than assuming one of them.
        let accepted = poll_fn(|cx| {
            let poll = Pin::new(&mut file).poll_read(cx, &mut large);
            Poll::Ready(match poll {
                Poll::Pending => None,
                Poll::Ready(result) => Some(result.expect("read should succeed")),
            })
        })
        .await;

        let already_read = accepted.unwrap_or(0);
        let mut prefix = [0; 2];
        if already_read == 0 {
            // The abandoned read is still owned by the file; a later read must
            // finish it and hand back its bytes from the start of the file.
            file.read_exact(&mut prefix).await.expect("finish read");
            assert_eq!(&prefix, b"01");
        }

        // Either way the cursor must reflect exactly the bytes delivered so far.
        let consumed = if already_read == 0 { 2 } else { already_read } as u64;
        assert_eq!(
            AsyncSeekExt::seek(&mut file, SeekFrom::Current(0))
                .await
                .expect("seek through trait"),
            consumed,
            "seek must reconcile the accepted read rather than double-count it"
        );

        if already_read == 0 {
            let mut next = [0; 2];
            file.read_exact(&mut next).await.expect("read after seek");
            assert_eq!(&next, b"23");
        }
        drop(file);
        fs::remove_file(&path).await.expect("remove fixture");
    });
}

#[test]
fn file_vectored_write_cancellation_keeps_operation_identity() {
    use core::future::poll_fn;
    use runite::fs::{self, File};

    let path = temp_path("vectored-write-cancel");
    common::block_on(move || async move {
        let mut file = File::create(&path).await.expect("create fixture");
        let old = [IoSlice::new(b"old")];
        {
            let mut write = pin!(file.write_vectored(&old));
            poll_fn(|cx| {
                assert!(
                    write.as_mut().poll(cx).is_pending(),
                    "first backend poll should leave the write in flight"
                );
                Poll::Ready(())
            })
            .await;
        }

        let new = [IoSlice::new(b"new bytes")];
        assert_eq!(
            file.write_vectored(&new)
                .await
                .expect("new operation should receive its own completion"),
            b"new bytes".len()
        );
        drop(file);

        let contents = fs::read(&path).await.expect("read fixture");
        assert_eq!(contents, b"oldnew bytes");
        fs::remove_file(&path).await.expect("remove fixture");
    });
}

#[test]
fn cloned_file_seek_does_not_consume_a_live_write_completion() {
    use core::future::poll_fn;
    use runite::fs::{self, File};

    let path = temp_path("clone-seek-live-write");
    common::block_on(move || async move {
        let mut writer = File::create(&path).await.expect("create fixture");
        let mut seeker = writer.try_clone().await.expect("clone file");
        let write_result = {
            let mut write = pin!(writer.write(b"abc"));
            poll_fn(|cx| {
                assert!(write.as_mut().poll(cx).is_pending());
                Poll::Ready(())
            })
            .await;

            assert_eq!(
                seeker
                    .seek(SeekFrom::End(0))
                    .await
                    .expect("seek should wait for accepted write"),
                3
            );
            write.await
        };
        assert_eq!(write_result.expect("original write completion"), 3);
        drop(seeker);
        drop(writer);

        let contents = fs::read(&path).await.expect("read fixture");
        assert_eq!(contents, b"abc", "the live write must not be resubmitted");
        fs::remove_file(&path).await.expect("remove fixture");
    });
}

#[test]
fn cloned_file_read_does_not_consume_a_live_write_completion() {
    use core::future::poll_fn;
    use runite::fs::{self, OpenOptions};

    let path = temp_path("clone-read-live-write");
    common::block_on(move || async move {
        let mut writer = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(true)
            .open(&path)
            .await
            .expect("create fixture");
        let mut reader = writer.try_clone().await.expect("clone file");
        let write_result = {
            let mut write = pin!(writer.write(b"abc"));
            poll_fn(|cx| {
                assert!(write.as_mut().poll(cx).is_pending());
                Poll::Ready(())
            })
            .await;

            let mut byte = [0; 1];
            assert_eq!(
                reader
                    .read(&mut byte)
                    .await
                    .expect("read should wait for accepted write"),
                0
            );
            write.await
        };
        assert_eq!(write_result.expect("original write completion"), 3);
        drop(reader);
        drop(writer);

        let contents = fs::read(&path).await.expect("read fixture");
        assert_eq!(contents, b"abc", "the live write must not be resubmitted");
        fs::remove_file(&path).await.expect("remove fixture");
    });
}

#[test]
fn cloned_file_live_writes_complete_once_in_poll_order() {
    use core::future::poll_fn;
    use runite::fs::{self, File};

    let path = temp_path("clone-live-write-order");
    common::block_on(move || async move {
        let mut first_file = File::create(&path).await.expect("create fixture");
        let mut second_file = first_file.try_clone().await.expect("clone file");
        let (first_result, second_result) = {
            let mut first = pin!(first_file.write(b"A"));
            poll_fn(|cx| {
                assert!(first.as_mut().poll(cx).is_pending());
                Poll::Ready(())
            })
            .await;

            let mut second = pin!(second_file.write(b"B"));
            let mut polls = 0;
            poll_fn(|cx| {
                polls += 1;
                assert!(second.as_mut().poll(cx).is_pending());
                if polls >= 2 {
                    Poll::Ready(())
                } else {
                    Poll::Pending
                }
            })
            .await;

            (first.await, second.await)
        };
        assert_eq!(first_result.expect("first write completion"), 1);
        assert_eq!(second_result.expect("second write completion"), 1);
        drop(second_file);
        drop(first_file);

        let contents = fs::read(&path).await.expect("read fixture");
        assert_eq!(contents, b"AB");
        fs::remove_file(&path).await.expect("remove fixture");
    });
}

#[test]
fn cancelled_cloned_file_write_leaves_no_stale_queue_entry() {
    use core::future::poll_fn;
    use runite::fs::{self, File};

    let path = temp_path("clone-cancelled-write-queue");
    common::block_on(move || async move {
        let mut first_file = File::create(&path).await.expect("create fixture");
        let mut cancelled_file = first_file.try_clone().await.expect("clone file");
        let first_result = {
            let mut first = pin!(first_file.write(b"A"));
            poll_fn(|cx| {
                assert!(first.as_mut().poll(cx).is_pending());
                Poll::Ready(())
            })
            .await;

            {
                let mut cancelled = pin!(cancelled_file.write(b"discard"));
                poll_fn(|cx| {
                    assert!(cancelled.as_mut().poll(cx).is_pending());
                    Poll::Ready(())
                })
                .await;
            }
            first.await
        };

        assert_eq!(first_result.expect("first write completion"), 1);
        assert_eq!(
            cancelled_file
                .seek(SeekFrom::End(0))
                .await
                .expect("cancelled queue entry must not block seek"),
            1
        );
        drop(cancelled_file);
        drop(first_file);

        let contents = fs::read(&path).await.expect("read fixture");
        assert_eq!(contents, b"A");
        fs::remove_file(&path).await.expect("remove fixture");
    });
}

#[test]
fn cloned_file_direct_poll_writes_have_distinct_owners() {
    use core::future::poll_fn;
    use runite::fs::{self, File};

    let path = temp_path("clone-direct-poll-write");
    common::block_on(move || async move {
        let mut first_file = File::create(&path).await.expect("create fixture");
        let mut second_file = first_file.try_clone().await.expect("clone file");
        poll_fn(|cx| {
            assert!(AsyncWrite::poll_write(Pin::new(&mut first_file), cx, b"A").is_pending());
            Poll::Ready(())
        })
        .await;

        let mut polls = 0;
        poll_fn(|cx| {
            polls += 1;
            assert!(AsyncWrite::poll_write(Pin::new(&mut second_file), cx, b"B").is_pending());
            if polls >= 2 {
                Poll::Ready(())
            } else {
                Poll::Pending
            }
        })
        .await;

        assert_eq!(
            poll_fn(|cx| AsyncWrite::poll_write(Pin::new(&mut first_file), cx, b"A"))
                .await
                .expect("first direct write"),
            1
        );
        assert_eq!(
            poll_fn(|cx| AsyncWrite::poll_write(Pin::new(&mut second_file), cx, b"B"))
                .await
                .expect("second direct write"),
            1
        );
        drop(second_file);
        drop(first_file);

        let contents = fs::read(&path).await.expect("read fixture");
        assert_eq!(contents, b"AB");
        fs::remove_file(&path).await.expect("remove fixture");
    });
}

/// `AsyncRead`/`AsyncWrite`/`AsyncSeek`/`AsyncBufRead` forward through `&mut T`,
/// `Box<T>`, and `Pin<Box<T>>`.
///
/// Without these, wrapping a *borrowed* reader was impossible — `BufReader::new`
/// takes ownership, so code that only had a `&mut File` had to give up the file
/// or restructure. Several tests and examples in this repository took ownership
/// purely to work around it.
#[runite::test]
async fn async_io_traits_forward_through_pointers() {
    let path = temp_path("pointer-forwarding");
    std::fs::write(&path, b"forwarded through a pointer").expect("seed file");

    let mut file = runite::fs::File::open(&path).await.expect("open");

    // `&mut File` as an `AsyncRead`: the borrow is enough, the file is not moved.
    {
        let mut buffered = BufReader::new(&mut file);
        let mut first = String::new();
        buffered
            .read_line(&mut first)
            .await
            .expect("read through &mut");
        assert_eq!(first, "forwarded through a pointer");
    }

    // `file` is still ours, and a `&mut` to it satisfies `AsyncSeek` on its own.
    // Routed through a generic so the bound is what is being tested, rather
    // than method resolution picking an inherent method.
    async fn rewind<S: AsyncSeek + Unpin>(mut seeker: S) -> io::Result<u64> {
        seeker.seek(SeekFrom::Start(0)).await
    }
    let position = rewind(&mut file).await.expect("seek through &mut");
    assert_eq!(position, 0);

    // `Box<T>` and `Pin<Box<T>>` forward too.
    let mut boxed: Box<runite::fs::File> = Box::new(file);
    let mut via_box = Vec::new();
    boxed.read_to_end(&mut via_box).await.expect("read via Box");
    assert_eq!(via_box, b"forwarded through a pointer");

    let mut pinned = Box::pin(boxed);
    pinned
        .seek(SeekFrom::Start(10))
        .await
        .expect("seek via Pin");
    let mut via_pin = Vec::new();
    pinned
        .read_to_end(&mut via_pin)
        .await
        .expect("read via Pin<Box<_>>");
    assert_eq!(via_pin, b"through a pointer");

    let _ = std::fs::remove_file(&path);
}

/// The forwarding impls must not lose an override. A writer that counts
/// vectored writes and cancellation generations sees the same calls through a
/// `&mut` as it does directly — if the macro had let a default implementation
/// stand in, a runtime-backed writer would quietly lose cancellation safety.
#[test]
fn forwarding_preserves_overridden_write_hooks() {
    use std::cell::Cell;
    use std::rc::Rc;

    #[derive(Default)]
    struct Counts {
        vectored: Cell<usize>,
        generations: Cell<u64>,
    }

    struct Recorder(Rc<Counts>);

    impl AsyncWrite for Recorder {
        fn poll_write(
            self: Pin<&mut Self>,
            _cx: &mut Context<'_>,
            buf: &[u8],
        ) -> Poll<io::Result<usize>> {
            Poll::Ready(Ok(buf.len()))
        }

        fn poll_write_vectored(
            self: Pin<&mut Self>,
            _cx: &mut Context<'_>,
            bufs: &[IoSlice<'_>],
        ) -> Poll<io::Result<usize>> {
            self.0.vectored.set(self.0.vectored.get() + 1);
            Poll::Ready(Ok(bufs.iter().map(|slice| slice.len()).sum()))
        }

        fn poll_write_operation(
            self: Pin<&mut Self>,
            _cx: &mut Context<'_>,
            buf: &[u8],
            generation: u64,
        ) -> Poll<io::Result<usize>> {
            self.0.generations.set(generation);
            Poll::Ready(Ok(buf.len()))
        }

        fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
            Poll::Ready(Ok(()))
        }

        fn poll_close(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
            Poll::Ready(Ok(()))
        }
    }

    let counts = Rc::new(Counts::default());
    let mut recorder = Recorder(Rc::clone(&counts));
    let mut cx = Context::from_waker(Waker::noop());

    let mut borrowed = &mut recorder;
    let slices = [IoSlice::new(b"ab"), IoSlice::new(b"cd")];
    let written = Pin::new(&mut borrowed).poll_write_vectored(&mut cx, &slices);
    assert!(
        matches!(written, Poll::Ready(Ok(4))),
        "vectored override should run, got {written:?}"
    );
    assert_eq!(
        counts.vectored.get(),
        1,
        "the override must not be replaced by the trait default"
    );

    let written = Pin::new(&mut borrowed).poll_write_operation(&mut cx, b"xyz", 42);
    assert!(matches!(written, Poll::Ready(Ok(3))), "got {written:?}");
    assert_eq!(
        counts.generations.get(),
        42,
        "the cancellation generation must survive the forward"
    );
}
