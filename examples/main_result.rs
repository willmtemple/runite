//! Verifies `#[runite::main]` honors a `Result` return type: an `Err` reported
//! here would exit non-zero via `Termination` instead of silently exiting 0.
//! (This example returns `Ok`, so it exits 0.)

#[runite::main]
async fn main() -> std::io::Result<()> {
    let contents = runite::fs::read_to_string("Cargo.toml").await?;
    assert!(!contents.is_empty());
    Ok(())
}
