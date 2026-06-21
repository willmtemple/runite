use runite::fs::{self, File};
use std::path::PathBuf;

fn preview(bytes: &[u8]) -> String {
    String::from_utf8_lossy(bytes).replace('\n', "\\n")
}

#[runite::async_main]
async fn main() {
    let manifest_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let cargo_toml = manifest_dir.join("Cargo.toml");
    let src_dir = manifest_dir.join("src");

    println!("manifest dir: {}", manifest_dir.display());

    let cargo_meta = fs::metadata(&cargo_toml)
        .await
        .expect("Cargo.toml metadata should load");
    println!(
        "Cargo.toml: {} bytes, file={}, empty={}",
        cargo_meta.len(),
        cargo_meta.is_file(),
        cargo_meta.is_empty()
    );

    let mut file = File::open(&cargo_toml)
        .await
        .expect("Cargo.toml should open for reading");
    let file_meta = file
        .metadata()
        .await
        .expect("opened file metadata should load");
    println!("opened file metadata size: {}", file_meta.len());

    let mut sequential = vec![0; 96];
    let sequential_read = file
        .read(&mut sequential)
        .await
        .expect("sequential read should succeed");
    sequential.truncate(sequential_read);
    println!(
        "sequential read ({sequential_read} bytes): {}",
        preview(&sequential)
    );

    let cloned = file.try_clone().await.expect("file clone should succeed");
    let mut positioned = [0u8; 48];
    let positioned_read = cloned
        .read_at(0, &mut positioned)
        .await
        .expect("positioned read should succeed");
    println!(
        "positioned read ({positioned_read} bytes): {}",
        preview(&positioned[..positioned_read])
    );

    let cargo_text = fs::read_to_string(&cargo_toml)
        .await
        .expect("read_to_string should succeed");
    println!("Cargo.toml line count: {}", cargo_text.lines().count());

    let mut dir = fs::read_dir(&src_dir)
        .await
        .expect("src directory should be readable");
    let mut entries = Vec::new();
    while let Some(entry) = dir
        .next_entry()
        .await
        .expect("read_dir stream should succeed")
    {
        let metadata = entry.metadata().await.expect("entry metadata should load");
        let kind = if metadata.is_dir() { "dir" } else { "file" };
        entries.push((entry.file_name().to_string_lossy().into_owned(), kind));
    }
    entries.sort_by(|left, right| left.0.cmp(&right.0));

    println!("src entries:");
    for (name, kind) in entries.iter().take(8) {
        println!("  - {name} ({kind})");
    }
}
