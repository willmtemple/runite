//! Internal and public operation-layer building blocks.
//!
//! The operation layer defines logical work units that bridge user-facing APIs and platform
//! backends without leaking platform details upward.

pub(crate) mod completion;
pub mod fs;
pub mod net;
