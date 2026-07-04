//! Windows-specific extensions for [`crate::fs`] types, mirroring
//! [`std::os::windows::fs`].

/// Windows-specific extensions to [`crate::fs::OpenOptions`], mirroring
/// [`std::os::windows::fs::OpenOptionsExt`].
///
/// The configured values are forwarded to `CreateFileW` when the file is
/// opened. runite always adds `FILE_FLAG_OVERLAPPED` to the custom flags so
/// the handle can be driven by the completion-port backend.
///
/// # Examples
///
/// ```no_run
/// use runite::os::windows::fs::OpenOptionsExt;
///
/// runite::spawn(async {
///     let mut options = runite::fs::OpenOptions::new();
///     options.read(true);
///     // FILE_SHARE_READ | FILE_SHARE_WRITE
///     options.share_mode(0x1 | 0x2);
///     let _file = options.open("data.bin").await.unwrap();
/// });
/// runite::run();
/// ```
pub trait OpenOptionsExt {
    /// Overrides the access rights passed to `CreateFileW` with `access`,
    /// replacing the rights derived from `read`/`write`/`append`. See
    /// [`std::os::windows::fs::OpenOptionsExt::access_mode`].
    fn access_mode(&mut self, access: u32) -> &mut Self;

    /// Overrides the share mode passed to `CreateFileW`. See
    /// [`std::os::windows::fs::OpenOptionsExt::share_mode`].
    fn share_mode(&mut self, share: u32) -> &mut Self;

    /// Adds extra flags to the `dwFlagsAndAttributes` argument of
    /// `CreateFileW` (`FILE_FLAG_OVERLAPPED` is always added). See
    /// [`std::os::windows::fs::OpenOptionsExt::custom_flags`].
    fn custom_flags(&mut self, flags: u32) -> &mut Self;

    /// Sets the file attribute bits used when a new file is created. See
    /// [`std::os::windows::fs::OpenOptionsExt::attributes`].
    fn attributes(&mut self, attributes: u32) -> &mut Self;

    /// Sets the `SECURITY_SQOS_PRESENT` quality-of-service flags. See
    /// [`std::os::windows::fs::OpenOptionsExt::security_qos_flags`].
    fn security_qos_flags(&mut self, flags: u32) -> &mut Self;
}

impl OpenOptionsExt for crate::fs::OpenOptions {
    fn access_mode(&mut self, access: u32) -> &mut Self {
        self.platform_options_mut().access_mode = Some(access);
        self
    }

    fn share_mode(&mut self, share: u32) -> &mut Self {
        self.platform_options_mut().share_mode = Some(share);
        self
    }

    fn custom_flags(&mut self, flags: u32) -> &mut Self {
        self.platform_options_mut().custom_flags = flags;
        self
    }

    fn attributes(&mut self, attributes: u32) -> &mut Self {
        self.platform_options_mut().attributes = attributes;
        self
    }

    fn security_qos_flags(&mut self, flags: u32) -> &mut Self {
        self.platform_options_mut().security_qos_flags = flags;
        self
    }
}

/// Windows-specific extensions to [`crate::fs::Metadata`], mirroring
/// [`std::os::windows::fs::MetadataExt`].
pub trait MetadataExt {
    /// Returns the raw `dwFileAttributes` bits for the file, as reported by
    /// `GetFileInformationByHandle`. See
    /// [`std::os::windows::fs::MetadataExt::file_attributes`].
    fn file_attributes(&self) -> u32;
}

impl MetadataExt for crate::fs::Metadata {
    fn file_attributes(&self) -> u32 {
        self.platform_metadata().file_attributes
    }
}
