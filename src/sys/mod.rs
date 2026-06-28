//! Platform backend implementations.

pub(crate) mod blocking;

#[cfg(target_os = "linux")]
pub mod linux;
#[cfg(all(target_os = "macos", target_arch = "aarch64"))]
pub mod macos;

#[cfg(target_os = "linux")]
pub use linux as current;
#[cfg(all(target_os = "macos", target_arch = "aarch64"))]
pub use macos as current;
