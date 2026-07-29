//! Unix-specific extensions to runite types, mirroring [`std::os::unix`].
//!
//! Most Unix functionality is reachable without an extension trait:
//! [`runite::fd`](crate::fd) exposes descriptor readiness waits,
//! [`runite::signal::unix`](crate::signal::unix) exposes POSIX signal streams,
//! and the I/O types implement the [`std::os::fd`] interop traits directly.
//! This module hosts the surfaces that do need one.

pub mod process;
