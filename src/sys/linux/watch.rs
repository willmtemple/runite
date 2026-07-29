//! Linux filesystem watching, backed by inotify.
//!
//! The inotify descriptor is an ordinary pollable fd, so it is driven by the
//! runtime's own driver through `fd::wait_readable` rather than by a dedicated
//! thread. That is the whole reason to implement this here rather than adopt a
//! general-purpose watcher crate: those spawn a thread per watcher on every
//! platform, including the one where it is unnecessary.

use core::task::{Context, Poll};
use std::collections::HashMap;
use std::collections::VecDeque;
use std::ffi::CString;
use std::io;
use std::os::fd::{AsRawFd, FromRawFd, OwnedFd};
use std::path::{Path, PathBuf};

use crate::fs::watch::{Event, EventKind, Recursive, WatchId};
use crate::io::IoFuture;

/// Big enough for many events per read; inotify refuses a buffer that cannot
/// hold one complete event, and a larger buffer means fewer syscalls under a
/// burst, which is exactly when it matters.
const BUFFER_BYTES: usize = 16 * 1024;

/// One registered watch. A recursive watch owns several kernel watches — one
/// per directory — and reports events for all of them under the same id.
struct Registration {
    /// Kernel watch descriptors to the directory each one covers, so an event
    /// can be resolved back to a full path.
    descriptors: HashMap<i32, PathBuf>,
}

pub(crate) struct Watcher {
    fd: OwnedFd,
    next_id: u64,
    registrations: HashMap<u64, Registration>,
    /// Which registration a kernel watch descriptor belongs to. Separate from
    /// `registrations` so an incoming event is one lookup rather than a scan.
    owner: HashMap<i32, u64>,
    /// Events decoded from the last read but not yet yielded. One read can
    /// carry many events, and `Stream` yields one at a time.
    pending: VecDeque<io::Result<Event>>,
    readable: Option<IoFuture<()>>,
}

impl Watcher {
    pub(crate) fn new() -> io::Result<Self> {
        // SAFETY: no pointer arguments; returns a fresh fd or -1.
        let raw = unsafe { libc::inotify_init1(libc::IN_NONBLOCK | libc::IN_CLOEXEC) };
        if raw == -1 {
            return Err(io::Error::last_os_error());
        }
        // SAFETY: `raw` is a fresh descriptor that nothing else owns.
        let fd = unsafe { OwnedFd::from_raw_fd(raw) };
        Ok(Self {
            fd,
            next_id: 1,
            registrations: HashMap::new(),
            owner: HashMap::new(),
            pending: VecDeque::new(),
            readable: None,
        })
    }

    pub(crate) fn watch(&mut self, path: &Path, recursive: Recursive) -> io::Result<WatchId> {
        let id = self.next_id;
        self.next_id += 1;
        let mut registration = Registration {
            descriptors: HashMap::new(),
        };

        self.add_watch(&mut registration, path)?;
        if recursive == Recursive::Yes {
            // inotify does not recurse, so recursion is a walk plus one watch
            // per directory. A directory created during the walk may be missed;
            // that race is inherent and documented rather than papered over.
            let mut queue = vec![path.to_path_buf()];
            while let Some(directory) = queue.pop() {
                let entries = match std::fs::read_dir(&directory) {
                    Ok(entries) => entries,
                    // A directory that vanished mid-walk is not an error: the
                    // tree is live, and failing the whole registration because
                    // one subdirectory went away would be worse than watching
                    // the rest.
                    Err(_) => continue,
                };
                for entry in entries.flatten() {
                    if entry.file_type().is_ok_and(|kind| kind.is_dir()) {
                        let child = entry.path();
                        if self.add_watch(&mut registration, &child).is_ok() {
                            queue.push(child);
                        }
                    }
                }
            }
        }

        for descriptor in registration.descriptors.keys() {
            self.owner.insert(*descriptor, id);
        }
        self.registrations.insert(id, registration);
        Ok(WatchId(id))
    }

    fn add_watch(&self, registration: &mut Registration, path: &Path) -> io::Result<()> {
        let c_path = CString::new(path.as_os_str().as_encoded_bytes())
            .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "path contains a NUL byte"))?;
        const MASK: u32 = libc::IN_CREATE
            | libc::IN_DELETE
            | libc::IN_DELETE_SELF
            | libc::IN_MODIFY
            | libc::IN_ATTRIB
            | libc::IN_MOVED_FROM
            | libc::IN_MOVED_TO
            | libc::IN_MOVE_SELF
            | libc::IN_CLOSE_WRITE;
        // SAFETY: `c_path` is a valid NUL-terminated path and outlives the call.
        let descriptor =
            unsafe { libc::inotify_add_watch(self.fd.as_raw_fd(), c_path.as_ptr(), MASK) };
        if descriptor == -1 {
            return Err(io::Error::last_os_error());
        }
        registration
            .descriptors
            .insert(descriptor, path.to_path_buf());
        Ok(())
    }

    pub(crate) fn unwatch(&mut self, id: WatchId) -> io::Result<()> {
        let Some(registration) = self.registrations.remove(&id.0) else {
            // Already gone. Not an error: a caller unwatching twice, or
            // unwatching a path the kernel already dropped, has the outcome it
            // asked for.
            return Ok(());
        };
        for descriptor in registration.descriptors.keys() {
            self.owner.remove(descriptor);
            // SAFETY: both arguments are plain values; a stale descriptor
            // simply returns -1, which is the "already gone" case above.
            unsafe {
                libc::inotify_rm_watch(self.fd.as_raw_fd(), *descriptor);
            }
        }
        Ok(())
    }

    pub(crate) fn poll_next(
        &mut self,
        context: &mut Context<'_>,
    ) -> Poll<Option<io::Result<Event>>> {
        loop {
            if let Some(event) = self.pending.pop_front() {
                return Poll::Ready(Some(event));
            }

            let mut buffer = [0u8; BUFFER_BYTES];
            // SAFETY: `fd` is open and `buffer` points to `BUFFER_BYTES`
            // writable bytes.
            let read = unsafe {
                libc::read(
                    self.fd.as_raw_fd(),
                    buffer.as_mut_ptr().cast::<libc::c_void>(),
                    buffer.len(),
                )
            };

            if read > 0 {
                self.readable = None;
                self.decode(&buffer[..read as usize]);
                continue;
            }
            if read == 0 {
                return Poll::Ready(None);
            }

            let error = io::Error::last_os_error();
            match error.kind() {
                io::ErrorKind::Interrupted => continue,
                io::ErrorKind::WouldBlock => {
                    let raw = self.fd.as_raw_fd();
                    let future = self
                        .readable
                        .get_or_insert_with(|| Box::pin(super::fd::wait_readable(raw)));
                    match future.as_mut().poll(context) {
                        Poll::Ready(Ok(())) => {
                            self.readable = None;
                            continue;
                        }
                        Poll::Ready(Err(error)) => {
                            self.readable = None;
                            return Poll::Ready(Some(Err(error)));
                        }
                        Poll::Pending => return Poll::Pending,
                    }
                }
                _ => return Poll::Ready(Some(Err(error))),
            }
        }
    }

    /// Decodes one read's worth of `inotify_event` records.
    fn decode(&mut self, bytes: &[u8]) {
        const HEADER: usize = std::mem::size_of::<libc::inotify_event>();
        let mut offset = 0;
        while offset + HEADER <= bytes.len() {
            // SAFETY: inotify writes whole `inotify_event` records, and the
            // bound above guarantees a full header is present. Read
            // unaligned because the kernel packs records back to back.
            let raw = unsafe {
                std::ptr::read_unaligned(bytes[offset..].as_ptr().cast::<libc::inotify_event>())
            };
            let name_len = raw.len as usize;
            let name_start = offset + HEADER;
            let name_end = name_start + name_len;
            if name_end > bytes.len() {
                break;
            }

            if raw.mask & libc::IN_Q_OVERFLOW != 0 {
                // The kernel dropped events. Reported as an event rather than
                // an error: the watch is still live, and ending the stream over
                // a transient burst would be worse than telling the caller to
                // rescan.
                self.pending.push_back(Ok(Event {
                    path: PathBuf::new(),
                    kind: EventKind::Overflow,
                }));
                offset = name_end;
                continue;
            }

            if let Some(path) = self.resolve(raw.wd, &bytes[name_start..name_end]) {
                self.pending.push_back(Ok(Event {
                    path,
                    kind: kind_of(raw.mask),
                }));
            }
            offset = name_end;
        }
    }

    /// Joins an event's entry name onto the directory its watch covers.
    fn resolve(&self, descriptor: i32, name: &[u8]) -> Option<PathBuf> {
        let id = self.owner.get(&descriptor)?;
        let base = self.registrations.get(id)?.descriptors.get(&descriptor)?;
        // The name is NUL-padded to an alignment boundary.
        let name = name.split(|byte| *byte == 0).next().unwrap_or_default();
        if name.is_empty() {
            return Some(base.clone());
        }
        use std::os::unix::ffi::OsStrExt;
        Some(base.join(std::ffi::OsStr::from_bytes(name)))
    }
}

fn kind_of(mask: u32) -> EventKind {
    if mask & (libc::IN_CREATE | libc::IN_MOVED_TO) != 0 {
        EventKind::Created
    } else if mask & (libc::IN_DELETE | libc::IN_DELETE_SELF | libc::IN_MOVED_FROM) != 0 {
        EventKind::Removed
    } else if mask & (libc::IN_MODIFY | libc::IN_ATTRIB | libc::IN_CLOSE_WRITE) != 0 {
        EventKind::Modified
    } else if mask & libc::IN_MOVE_SELF != 0 {
        // The watched path itself moved. Neither created nor removed from this
        // watch's point of view, and the new location is not reported.
        EventKind::Other
    } else {
        EventKind::Other
    }
}
