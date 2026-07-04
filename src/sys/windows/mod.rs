//! Windows backend: IOCP-driven overlapped I/O plus blocking-pool offload for
//! operations with no overlapped form. See `docs/WINDOWS.md` for the design.
//!
//! Unlike the Unix backends there is no `fd` module: readiness waits are a
//! descriptor concept with no completion-port analogue, so `runite::fd` does
//! not exist on Windows.

pub mod channel;
pub mod fs;
pub mod net;
pub(crate) mod overlapped;
pub mod process;
