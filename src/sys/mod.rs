//! Platform backend implementations.

pub(crate) mod blocking;

#[cfg(all(target_os = "linux", target_arch = "x86_64"))]
pub mod linux;
#[cfg(all(target_os = "macos", target_arch = "aarch64"))]
pub mod macos;

#[cfg(all(target_os = "linux", target_arch = "x86_64"))]
pub use linux as current;
#[cfg(all(target_os = "macos", target_arch = "aarch64"))]
pub use macos as current;
