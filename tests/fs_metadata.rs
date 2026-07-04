//! Tests for the fs semantic gaps closed in release plan 3.8:
//! `symlink_metadata`, `File::seek`, and full-`st_mode` `Metadata::mode`.

#![cfg(unix)]

use std::io::SeekFrom;
use std::os::unix::fs::symlink;

/// `S_IFMT` / `S_IFREG` as octal literals (libc is not a dev-dependency).
const S_IFMT: u32 = 0o170000;
const S_IFREG: u32 = 0o100000;

fn temp_path(label: &str) -> std::path::PathBuf {
    std::env::temp_dir().join(format!("runite-{label}-{}", std::process::id()))
}

/// `metadata` follows symlinks; `symlink_metadata` reports the link itself.
#[runite::test]
async fn symlink_metadata_does_not_follow() {
    let dir = temp_path("symlink-meta");
    std::fs::create_dir_all(&dir).expect("mkdir");
    let target = dir.join("target.txt");
    let link = dir.join("link");
    std::fs::write(&target, b"data").expect("write target");
    let _ = std::fs::remove_file(&link);
    symlink(&target, &link).expect("symlink");

    let followed = runite::fs::metadata(&link).await.expect("metadata");
    assert!(followed.is_file(), "metadata should follow to the file");
    assert!(!followed.is_symlink());

    let link_meta = runite::fs::symlink_metadata(&link)
        .await
        .expect("symlink_metadata");
    assert!(link_meta.is_symlink(), "symlink_metadata should see the link");
    assert!(!link_meta.is_file());

    std::fs::remove_dir_all(&dir).expect("cleanup");
}

/// `Metadata::mode` includes the file-type bits, not just permissions, and is
/// consistent across backends.
#[runite::test]
async fn mode_includes_file_type_bits() {
    let path = temp_path("mode");
    std::fs::write(&path, b"x").expect("write");
    let meta = runite::fs::metadata(&path).await.expect("metadata");
    assert_eq!(
        meta.mode() & S_IFMT,
        S_IFREG,
        "mode() should carry the regular-file type bits (full st_mode)"
    );
    std::fs::remove_file(&path).expect("cleanup");
}

/// `File::seek` repositions the shared kernel cursor for sequential reads.
#[runite::test]
async fn file_seek_repositions_cursor() {
    let path = temp_path("seek");
    std::fs::write(&path, b"0123456789").expect("write");

    let mut file = runite::fs::File::open(&path).await.expect("open");

    assert_eq!(file.seek(SeekFrom::Start(4)).await.expect("seek start"), 4);
    let mut buf = [0u8; 3];
    file.read_exact(&mut buf).await.expect("read after seek");
    assert_eq!(&buf, b"456");

    // The read advanced the cursor to 7.
    assert_eq!(
        file.seek(SeekFrom::Current(0)).await.expect("seek current"),
        7
    );
    assert_eq!(file.seek(SeekFrom::End(-2)).await.expect("seek end"), 8);

    std::fs::remove_file(&path).expect("cleanup");
}
