#[doc(hidden)]
pub mod runtime_shared;

#[cfg(target_os = "linux")]
pub mod linux;
#[cfg(all(target_os = "macos", target_arch = "aarch64"))]
pub mod macos_aarch64;

#[cfg(target_os = "linux")]
pub use linux as current;
#[cfg(all(target_os = "macos", target_arch = "aarch64"))]
pub use macos_aarch64 as current;
