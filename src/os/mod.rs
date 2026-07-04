//! OS-specific extensions to runite types, mirroring [`std::os`].
//!
//! Unix-flavored functionality lives elsewhere in the crate today —
//! `runite::fd` exposes descriptor readiness waits, `runite::signal::unix`
//! exposes POSIX signal streams, and the I/O types implement the `std::os`
//! fd-interop traits directly. This module hosts the surfaces that need
//! dedicated extension traits, currently the Windows filesystem extensions.

#[cfg(windows)]
pub mod windows;
