//! End-to-end filesystem tests exercising the public `runite::fs` API.

mod common;

use common::block_on;
use runite::fs::{self, File, OpenOptions};

fn temp_path(name: &str) -> std::path::PathBuf {
    let mut dir = std::env::temp_dir();
    let unique = format!(
        "runite-fs-it-{}-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("system clock after epoch")
            .as_nanos(),
        name,
    );
    dir.push(unique);
    dir
}

#[test]
fn write_then_read_roundtrip() {
    let path = temp_path("roundtrip");
    let read_path = path.clone();
    let contents = block_on(move || async move {
        let mut file = File::create(&path).await.expect("create file");
        file.write_all(b"runite filesystem")
            .await
            .expect("write all");
        file.sync_all().await.expect("sync all");
        drop(file);

        let mut reopened = File::open(&read_path).await.expect("reopen file");
        let mut buf = Vec::new();
        reopened.read_to_end(&mut buf).await.expect("read to end");
        let _ = fs::remove_file(&read_path).await;
        buf
    });
    assert_eq!(contents, b"runite filesystem");
}

#[test]
fn positional_read_and_metadata() {
    let path = temp_path("positional");
    let work_path = path.clone();
    let (len, slice) = block_on(move || async move {
        let mut file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(true)
            .open(&work_path)
            .await
            .expect("open file");
        file.write_all(b"0123456789").await.expect("write payload");
        file.sync_data().await.expect("sync data");

        let meta = file.metadata().await.expect("metadata");

        let mut buf = [0u8; 4];
        file.read_exact_at(2, &mut buf)
            .await
            .expect("positional read");

        let _ = fs::remove_file(&work_path).await;
        (meta.len(), buf)
    });

    assert_eq!(len, 10);
    assert_eq!(&slice, b"2345");
}

#[test]
fn read_and_write_free_functions() {
    let path = temp_path("free-fns");
    let work_path = path.clone();
    let text = block_on(move || async move {
        fs::write(&work_path, b"top-level helpers")
            .await
            .expect("fs::write");
        let text = fs::read_to_string(&work_path)
            .await
            .expect("fs::read_to_string");
        let _ = fs::remove_file(&work_path).await;
        text
    });
    assert_eq!(text, "top-level helpers");
}
