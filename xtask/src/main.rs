//! Repository automation for runite.
//!
//! Run via the matching `mise run` task so the pinned Rust and
//! `cargo-public-api` versions are used.

mod api_report;
mod command;
mod release_verify;
mod targets;

fn main() -> std::process::ExitCode {
    command::entry()
}
