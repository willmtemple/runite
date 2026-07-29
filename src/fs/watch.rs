//! Filesystem change notification.
//!
//! **Linux only for now.** The macOS and Windows backends are in progress; this
//! module is not exposed on those targets rather than being exposed and
//! failing, so a portable application will not compile against a watcher that
//! cannot work.
//!
//! A [`Watcher`] reports changes to paths you name. It is a [`Stream`] of
//! [`Event`]s, driven by the runtime's own driver where the platform allows —
//! on Linux the inotify descriptor is registered with the same reactor
//! everything else uses, so watching costs no additional thread.
//!
//! # This is a primitive, not a file-watching solution
//!
//! Events are reported as the kernel produces them. That means, unavoidably:
//!
//! - **One logical change can produce several events.** A text editor saving a
//!   file may write, rename, and change attributes; you will see all of it.
//! - **Event kinds are not portable.** Linux distinguishes far more than
//!   Windows does. Code that branches on [`EventKind`] rather than on the path
//!   will behave differently across platforms.
//! - **Nothing is debounced or coalesced.** Any window would be wrong for
//!   somebody, and a debounce policy belongs to the application that knows what
//!   it is debouncing. Build it above this with the timers you already have.
//!
//! The reliable question to ask an event is "what path should I look at again",
//! not "what exactly happened to it". Treating a watcher as a hint to re-read
//! state is correct on every platform; treating it as a precise log is correct
//! on none.
//!
//! # Losing events is normal, and is reported
//!
//! Every backend has a bounded kernel-side queue, and a burst can overflow it.
//! When that happens the watcher yields [`EventKind::Overflow`] rather than an
//! error or silence: some changes were missed, the watch is still live, and the
//! application should rescan whatever it cares about. An error would end the
//! stream over a transient burst; silence would let an application believe it
//! is up to date when it is not.

use core::pin::Pin;
use core::task::{Context, Poll};
use std::io;
use std::path::{Path, PathBuf};

use crate::io::Stream;

/// Whether a watch covers a directory's descendants.
///
/// Only Windows implements recursion in the kernel. Linux and the BSDs watch a
/// single directory each, so recursion is emulated by walking the tree and
/// adding a watch per directory — which costs a kernel watch (Linux) or a file
/// descriptor (BSD) per directory, and races: a file created inside a new
/// subdirectory between its creation and the watch being installed is not
/// reported, though later changes to it are.
///
/// Prefer [`Recursive::No`] and name the paths you care about when you can.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Recursive {
    /// Watch only the named path, and a directory's immediate entries.
    No,
    /// Watch the named directory and every directory beneath it.
    Yes,
}

/// What a [`Watcher`] observed.
///
/// Deliberately coarse. The set of kinds a platform can distinguish varies
/// enormously — Windows reports little more than "something changed here" —
/// so a kind that cannot be produced everywhere would be a portability trap
/// rather than information.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum EventKind {
    /// A path appeared.
    Created,
    /// A path's contents or metadata changed.
    Modified,
    /// A path went away, by deletion or by being moved out of a watched
    /// directory.
    Removed,
    /// The kernel dropped events because its queue overflowed.
    ///
    /// Some changes were missed and cannot be recovered. The watch is still
    /// live; rescan whatever state depends on it. [`Event::path`] is the
    /// watched path when the platform identifies one and empty otherwise, so
    /// do not rely on it here.
    Overflow,
    /// Something happened that does not map onto the kinds above.
    Other,
}

/// A single observed change.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Event {
    /// The path the event concerns.
    ///
    /// For a watch on a directory this is the affected entry, joined onto the
    /// watched path. For [`EventKind::Overflow`] it may be empty.
    pub path: PathBuf,
    /// What happened.
    pub kind: EventKind,
}

impl Event {
    /// Whether this event means changes were missed and a rescan is needed.
    ///
    /// Worth branching on explicitly: it is the one event kind that says
    /// nothing about a particular path and invalidates assumptions about all
    /// of them.
    pub fn needs_rescan(&self) -> bool {
        matches!(self.kind, EventKind::Overflow)
    }
}

/// Identifies one registered watch, for [`Watcher::unwatch`].
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct WatchId(pub(crate) u64);

/// Watches paths for changes.
///
/// Tied to the runtime thread that created it and intentionally `!Send`, like
/// the rest of runite's resources. See the [module documentation](self) for what
/// a watcher does and does not promise.
///
/// # Examples
///
/// ```no_run
/// # async fn example() -> std::io::Result<()> {
/// use runite::fs::watch::{Recursive, Watcher};
/// use runite::io::StreamExt;
///
/// let mut watcher = Watcher::new()?;
/// watcher.watch(".config".as_ref(), Recursive::No)?;
///
/// while let Some(event) = watcher.next().await {
///     let event = event?;
///     if event.needs_rescan() {
///         // Changes were missed; re-read everything that matters.
///     } else {
///         println!("{} changed", event.path.display());
///     }
/// }
/// # Ok(())
/// # }
/// ```
pub struct Watcher {
    inner: crate::sys::current::watch::Watcher,
}

impl Watcher {
    /// Creates a watcher with no paths registered.
    ///
    /// # Errors
    ///
    /// Returns the platform error from creating the notification handle —
    /// commonly a per-user instance limit on Linux.
    pub fn new() -> io::Result<Self> {
        Ok(Self {
            inner: crate::sys::current::watch::Watcher::new()?,
        })
    }

    /// Registers `path`.
    ///
    /// Watching a path that does not exist fails. To learn when something
    /// appears, watch its parent directory: a create is reported against the
    /// directory that gained the entry, not against the entry that did not yet
    /// exist.
    ///
    /// Registering the same path twice returns two ids, both live; the events
    /// are reported once each.
    ///
    /// # Errors
    ///
    /// Returns the platform error, commonly "not found" for a missing path or
    /// a per-user watch limit on Linux.
    pub fn watch(&mut self, path: &Path, recursive: Recursive) -> io::Result<WatchId> {
        self.inner.watch(path, recursive)
    }

    /// Removes a watch.
    ///
    /// Events already queued for it may still be yielded; a watcher cannot
    /// retract what the kernel has already reported.
    ///
    /// # Errors
    ///
    /// Returns the platform error. Removing an id that is already gone is not
    /// an error.
    pub fn unwatch(&mut self, id: WatchId) -> io::Result<()> {
        self.inner.unwatch(id)
    }
}

impl Stream for Watcher {
    type Item = io::Result<Event>;

    fn poll_next(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.get_mut().inner.poll_next(context)
    }
}

impl std::fmt::Debug for Watcher {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.debug_struct("Watcher").finish_non_exhaustive()
    }
}
