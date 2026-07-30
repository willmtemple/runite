use std::collections::BTreeSet;
use std::path::{Path, PathBuf};
use std::process::{Command, ExitCode, Output};

use crate::{api_report, release_verify};

pub(crate) fn entry() -> ExitCode {
    let args: Vec<String> = std::env::args().skip(1).collect();
    let result = dispatch(&args);

    match result {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            eprintln!("xtask: {error}");
            ExitCode::FAILURE
        }
    }
}

fn dispatch(args: &[String]) -> Result<(), String> {
    match args.first().map(String::as_str) {
        Some("api-report") => api_report::run(&args[1..]),
        Some("release-verify") | Some("package-verify") => release_verify::run(&args[1..]),
        Some("publish-shape") => release_verify::publish_shape(&args[1..]),
        Some("help") | Some("--help") | Some("-h") => {
            usage();
            Ok(())
        }
        Some(other) => Err(format!("unknown subcommand `{other}`")),
        None => {
            usage();
            Err("a subcommand is required".to_owned())
        }
    }
}

fn usage() {
    eprintln!(
        "usage:\n  xtask api-report [--check]\n  \
         xtask release-verify [--msrv] [--publish-dry-run]\n  \
         xtask publish-shape [--allow-dirty] <package>..."
    );
}

pub(crate) fn workspace_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .expect("xtask must be a direct workspace member")
        .to_path_buf()
}

pub(crate) fn parse_flags<'a>(
    args: &'a [String],
    allowed: &[&str],
) -> Result<BTreeSet<&'a str>, String> {
    let mut parsed = BTreeSet::new();
    for arg in args {
        if !allowed.contains(&arg.as_str()) {
            return Err(format!("unknown option `{arg}`"));
        }
        parsed.insert(arg.as_str());
    }
    Ok(parsed)
}

pub(crate) fn run_output(command: &mut Command, label: &str) -> Result<Output, String> {
    let output = command
        .output()
        .map_err(|error| format!("could not run {label}: {error}"))?;
    if output.status.success() {
        Ok(output)
    } else {
        Err(format!(
            "{label} failed with {}:\n{}{}",
            output.status,
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        ))
    }
}

pub(crate) fn run_status(command: &mut Command, label: &str) -> Result<(), String> {
    let output = run_output(command, label)?;
    if !output.stdout.is_empty() {
        print!("{}", String::from_utf8_lossy(&output.stdout));
    }
    if !output.stderr.is_empty() {
        eprint!("{}", String::from_utf8_lossy(&output.stderr));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_known_flags_once() {
        let args = vec!["--check".to_owned(), "--check".to_owned()];
        let flags = parse_flags(&args, &["--check"]).unwrap();
        assert_eq!(flags.into_iter().collect::<Vec<_>>(), ["--check"]);
    }

    #[test]
    fn rejects_unknown_flags() {
        let args = vec!["--publish".to_owned()];
        assert_eq!(
            parse_flags(&args, &["--check"]).unwrap_err(),
            "unknown option `--publish`"
        );
    }
}
