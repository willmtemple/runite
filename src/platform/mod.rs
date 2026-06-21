#[doc(hidden)]
pub mod runtime_shared;

#[cfg(all(target_os = "linux", target_arch = "x86_64"))]
pub mod linux_x86_64;
#[cfg(all(target_os = "macos", target_arch = "aarch64"))]
pub mod macos_aarch64;

#[cfg(all(target_os = "linux", target_arch = "x86_64"))]
pub use linux_x86_64 as current;
#[cfg(all(target_os = "macos", target_arch = "aarch64"))]
pub use macos_aarch64 as current;
