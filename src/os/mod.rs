//! OS-specific extensions to runite types, mirroring [`std::os`].
//!
//! Most platform-flavored functionality lives elsewhere in the crate —
//! `runite::fd` exposes descriptor readiness waits, `runite::signal::unix`
//! exposes POSIX signal streams, and the I/O types implement the `std::os`
//! fd-interop traits directly. This module hosts the surfaces that need
//! dedicated extension traits: the Linux io_uring tuning, the Unix process
//! extensions, and the Windows filesystem extensions.

#[cfg(target_os = "linux")]
pub mod linux;
#[cfg(unix)]
pub mod unix;
#[cfg(windows)]
pub mod windows;
