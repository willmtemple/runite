use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::path::Path;
use std::process::Command;

use crate::command::{parse_flags, run_output, run_status, workspace_root};
use crate::targets::{SUPPORTED_TARGETS, Target};

const CRATE: &str = "runite";
const OUTPUT: &str = "docs/public-api.md";
/// Exact nightly used for rustdoc-JSON generation. The rendered item listing
/// varies across rustdoc versions, so bump this deliberately and regenerate
/// the report in the same change.
const RUSTDOC_TOOLCHAIN: &str = "nightly-2026-07-01";
const OMIT: &str = "blanket-impls,auto-trait-impls,auto-derived-impls";

struct ApiSurface {
    target: Target,
    feature_sets: BTreeMap<ApiFeatureSet, BTreeSet<String>>,
}

#[derive(Clone, Copy, Eq, Ord, PartialEq, PartialOrd)]
enum ApiFeatureSet {
    Default,
    Hyper,
    FuturesCompat,
    All,
}

const API_FEATURE_SETS: [ApiFeatureSet; 4] = [
    ApiFeatureSet::Default,
    ApiFeatureSet::Hyper,
    ApiFeatureSet::FuturesCompat,
    ApiFeatureSet::All,
];

impl ApiFeatureSet {
    fn label(self) -> &'static str {
        match self {
            Self::Default => "default",
            Self::Hyper => "hyper",
            Self::FuturesCompat => "futures-compat",
            Self::All => "all-features",
        }
    }
}

impl ApiSurface {
    fn api(&self, feature_set: ApiFeatureSet) -> &BTreeSet<String> {
        self.feature_sets
            .get(&feature_set)
            .expect("every API feature set must be collected")
    }
}

pub(crate) fn run(args: &[String]) -> Result<(), String> {
    let check = parse_flags(args, &["--check"])?;
    let mut surfaces = Vec::with_capacity(SUPPORTED_TARGETS.len());

    for target in SUPPORTED_TARGETS {
        let mut feature_sets = BTreeMap::new();
        for feature_set in API_FEATURE_SETS {
            println!(
                "xtask: collecting {} API for {}",
                feature_set.label(),
                target.triple
            );
            feature_sets.insert(feature_set, run_public_api(target, feature_set)?);
            check_api_probe(target, feature_set)?;
        }

        let default = feature_sets
            .get(&ApiFeatureSet::Default)
            .expect("default API must be collected");
        for feature_set in [
            ApiFeatureSet::Hyper,
            ApiFeatureSet::FuturesCompat,
            ApiFeatureSet::All,
        ] {
            let enabled = feature_sets
                .get(&feature_set)
                .expect("enabled API must be collected");
            if !default.is_subset(enabled) {
                let removed = default
                    .difference(enabled)
                    .take(20)
                    .cloned()
                    .collect::<Vec<_>>()
                    .join("\n");
                return Err(format!(
                    "enabling {} removed default API on {}:\n{removed}",
                    feature_set.label(),
                    target.triple
                ));
            }
        }
        let all_features = feature_sets
            .get(&ApiFeatureSet::All)
            .expect("all-features API must be collected");
        for feature_set in [ApiFeatureSet::Hyper, ApiFeatureSet::FuturesCompat] {
            let enabled = feature_sets
                .get(&feature_set)
                .expect("enabled API must be collected");
            if !enabled.is_subset(all_features) {
                return Err(format!(
                    "{} API is not a subset of all-features on {}",
                    feature_set.label(),
                    target.triple
                ));
            }
        }
        validate_surface(target, default, all_features)?;
        surfaces.push(ApiSurface {
            target,
            feature_sets,
        });
    }
    validate_portability(&surfaces)?;

    let rendered = render(&surfaces);
    let output = workspace_root().join(OUTPUT);
    if check.contains("--check") {
        let current = fs::read_to_string(&output).unwrap_or_default();
        if current != rendered {
            return Err(format!(
                "{OUTPUT} is out of date; run `mise run api-report` and commit the result"
            ));
        }
        println!("xtask: {OUTPUT} is up to date");
        return Ok(());
    }

    fs::write(&output, rendered).map_err(|error| format!("failed to write {OUTPUT}: {error}"))?;
    println!("xtask: wrote {OUTPUT}");
    Ok(())
}

fn run_public_api(target: Target, feature_set: ApiFeatureSet) -> Result<BTreeSet<String>, String> {
    let root = workspace_root();
    let mut command = Command::new("cargo");
    command.current_dir(root).args([
        &format!("+{RUSTDOC_TOOLCHAIN}"),
        "public-api",
        "--package",
        CRATE,
        "--target",
        target.triple,
        "--omit",
        OMIT,
        "--color",
        "never",
    ]);
    match feature_set {
        ApiFeatureSet::Default => {}
        ApiFeatureSet::Hyper => {
            command.args(["--no-default-features", "--features", "hyper"]);
        }
        ApiFeatureSet::FuturesCompat => {
            command.args(["--no-default-features", "--features", "futures-compat"]);
        }
        ApiFeatureSet::All => {
            command.arg("--all-features");
        }
    }

    let output = run_output(&mut command, "cargo public-api")?;
    let surface = String::from_utf8(output.stdout)
        .map_err(|error| format!("cargo public-api emitted non-UTF-8 output: {error}"))?;
    Ok(surface
        .lines()
        .map(str::trim_end)
        .filter(|line| !line.is_empty())
        .map(str::to_owned)
        .collect())
}

fn validate_surface(
    target: Target,
    default: &BTreeSet<String>,
    all_features: &BTreeSet<String>,
) -> Result<(), String> {
    for needle in [
        "pub use runite::WorkerJoin",
        "pub use runite::WorkerJoinError",
        "pub trait runite::io::AsyncBufRead",
        "pub trait runite::io::AsyncSeek",
        "poll_read_vectored",
        "poll_write_vectored",
    ] {
        require_api(all_features, target, needle)?;
    }

    let windows_items = [
        "pub mod runite::os::windows",
        "pub mod runite::os::windows::fs",
        "pub trait runite::os::windows::fs::OpenOptionsExt",
        "pub trait runite::os::windows::fs::MetadataExt",
        "pub mod runite::signal::windows",
    ];
    let unix_items = [
        "pub mod runite::fd",
        "pub mod runite::net::unix",
        "pub mod runite::os::unix",
        "pub mod runite::os::unix::process",
        "pub trait runite::os::unix::process::CommandExt",
        "pub mod runite::signal::unix",
    ];

    if target.triple.contains("windows") {
        for needle in windows_items {
            require_api(default, target, needle)?;
        }
        for needle in unix_items {
            reject_api(default, target, needle)?;
        }
    } else {
        for needle in unix_items {
            require_api(default, target, needle)?;
        }
        for needle in windows_items {
            reject_api(default, target, needle)?;
        }
    }
    Ok(())
}

fn require_api(surface: &BTreeSet<String>, target: Target, needle: &str) -> Result<(), String> {
    if surface.iter().any(|line| line.contains(needle)) {
        Ok(())
    } else {
        Err(format!(
            "{} API is missing required item containing `{needle}`",
            target.triple
        ))
    }
}

fn reject_api(surface: &BTreeSet<String>, target: Target, needle: &str) -> Result<(), String> {
    if surface.iter().any(|line| line.contains(needle)) {
        Err(format!(
            "{} API unexpectedly contains platform item `{needle}`",
            target.triple
        ))
    } else {
        Ok(())
    }
}

fn validate_portability(surfaces: &[ApiSurface]) -> Result<(), String> {
    let mut unix_surfaces = surfaces
        .iter()
        .filter(|surface| !surface.target.triple.contains("windows"));
    if let Some(reference) = unix_surfaces.next() {
        for surface in unix_surfaces {
            for feature_set in API_FEATURE_SETS {
                if reference.api(feature_set) != surface.api(feature_set) {
                    return Err(format!(
                        "{} and {} {} public surfaces differ outside genuine OS extension naming",
                        reference.target.triple,
                        surface.target.triple,
                        feature_set.label()
                    ));
                }
            }
        }
    }

    for feature_set in API_FEATURE_SETS {
        let mut portable = surfaces[0].api(feature_set).clone();
        for surface in &surfaces[1..] {
            let target = surface.api(feature_set);
            portable = portable.intersection(target).cloned().collect();
        }

        for surface in surfaces {
            let target = surface.api(feature_set);
            let unexpected = target
                .difference(&portable)
                .filter(|line| !is_platform_extension(surface.target, line))
                .take(20)
                .cloned()
                .collect::<Vec<_>>();
            if !unexpected.is_empty() {
                return Err(format!(
                    "{} {} surface differs from the portable API outside an \
                     approved OS extension:\n{}",
                    surface.target.triple,
                    feature_set.label(),
                    unexpected.join("\n")
                ));
            }
        }
    }
    Ok(())
}

fn is_platform_extension(target: Target, line: &str) -> bool {
    if target.triple.contains("windows") {
        line.contains("std::os::windows")
            || line.contains("runite::os::windows")
            || line.contains("runite::signal::windows")
            || line.starts_with("pub fn runite::fs::Metadata::file_attributes")
            || line.starts_with("pub fn runite::fs::OpenOptions::access_mode")
            || line.starts_with("pub fn runite::fs::OpenOptions::attributes")
            || line.starts_with("pub fn runite::fs::OpenOptions::custom_flags")
            || line.starts_with("pub fn runite::fs::OpenOptions::security_qos_flags")
            || line.starts_with("pub fn runite::fs::OpenOptions::share_mode")
    } else {
        line.contains("std::os::fd")
            || line.contains("std::os::unix")
            || line.contains("runite::fd")
            || line.contains("runite::net::unix")
            || line.contains("runite::net::Unix")
            || line.contains("runite::os::unix")
            || line.contains("runite::signal::unix")
            || line.starts_with("pub fn runite::process::ExitStatus::signal")
            // `CommandExt::pre_exec` is reachable as an inherent-looking method
            // on `Command`, so it renders without the trait's module path.
            || line.starts_with("pub unsafe fn runite::process::Command::pre_exec")
    }
}

fn check_api_probe(target: Target, feature_set: ApiFeatureSet) -> Result<(), String> {
    let root = workspace_root();
    let probe = root.join("target/xtask-api-probe");
    fs::create_dir_all(probe.join("src"))
        .map_err(|error| format!("failed to create API probe directory: {error}"))?;
    fs::write(
        probe.join("Cargo.toml"),
        api_probe_manifest(&root).as_bytes(),
    )
    .map_err(|error| format!("failed to write API probe manifest: {error}"))?;
    fs::write(probe.join("src/lib.rs"), API_PROBE.as_bytes())
        .map_err(|error| format!("failed to write API probe source: {error}"))?;

    let mut command = Command::new("cargo");
    command
        .current_dir(&root)
        .args(["check", "--quiet", "--manifest-path"])
        .arg(probe.join("Cargo.toml"))
        .args(["--target", target.triple]);
    if feature_set != ApiFeatureSet::Default {
        command.args(["--features", feature_set.label()]);
    }
    run_status(
        &mut command,
        &format!(
            "{} {} API compile probe",
            target.triple,
            feature_set.label()
        ),
    )
}

fn api_probe_manifest(root: &Path) -> String {
    let path = root
        .display()
        .to_string()
        .replace('\\', "\\\\")
        .replace('"', "\\\"");
    format!(
        "[package]\n\
         name = \"runite-api-probe\"\n\
         version = \"0.0.0\"\n\
         edition = \"2024\"\n\
         publish = false\n\n\
         [workspace]\n\n\
         [features]\n\
         hyper = [\"runite/hyper\"]\n\
         futures-compat = [\"runite/futures-compat\"]\n\
         all-features = [\"hyper\", \"futures-compat\"]\n\n\
         [dependencies]\n\
         runite = {{ path = \"{path}\", default-features = false }}\n"
    )
}

const API_PROBE: &str = r#"
#![deny(warnings)]
#![allow(dead_code)]

use core::future::Future;
use runite::{
    AbortHandle, IntervalHandle, JoinHandle, ThreadHandle, TimeoutHandle, WorkerHandle, WorkerJoin,
    WorkerJoinError,
};

fn assert_worker_join_future<F: Future<Output = Result<(), WorkerJoinError>>>() {}

fn handle_contract(
    thread: &ThreadHandle,
    worker: &WorkerHandle,
    join: &JoinHandle<()>,
    abort: &AbortHandle,
    timeout: &TimeoutHandle,
    interval: &IntervalHandle,
) {
    let _ = thread.queue_macrotask(|| {});
    let _ = thread.is_closed();
    let _ = thread.is_current();

    let _ = worker.queue_macrotask(|| {});
    let _ = worker.is_finished();
    let _: WorkerJoin = worker.join();
    let _: ThreadHandle = worker.thread();
    assert_worker_join_future::<WorkerJoin>();

    join.abort();
    let _ = join.is_finished();
    let _: AbortHandle = join.abort_handle();
    abort.abort();
    let _ = abort.is_finished();
    timeout.cancel();
    interval.cancel();

    let setup = WorkerJoinError::SetupPanicked;
    let _ = setup.is_setup_panicked();
    let _ = setup.is_runtime_panicked();
}

fn issue_9_traits<
    R: runite::io::AsyncRead + runite::io::AsyncBufRead + runite::io::AsyncSeek,
    W: runite::io::AsyncWrite,
>() {
}

#[cfg(unix)]
fn unix_contract(file: &runite::fs::File) {
    use std::os::fd::{AsFd, AsRawFd, OwnedFd};

    fn file_traits<T: AsFd + AsRawFd + TryFrom<OwnedFd, Error = std::io::Error>>() {}
    file_traits::<runite::fs::File>();
    let _: fn(OwnedFd) -> std::io::Result<runite::fs::File> = runite::fs::File::from_owned;
    let _ = runite::fd::wait_readable(file);
    let _ = core::mem::size_of::<runite::net::unix::UnixStream>();
    let _ = runite::signal::unix::SignalKind::Interrupt;
}

#[cfg(windows)]
fn windows_contract(
    options: &mut runite::fs::OpenOptions,
    metadata: &runite::fs::Metadata,
) {
    use runite::os::windows::fs::{MetadataExt, OpenOptionsExt};
    use std::os::windows::io::{
        AsHandle, AsRawHandle, AsRawSocket, AsSocket, OwnedHandle, OwnedSocket,
    };

    options
        .access_mode(0)
        .share_mode(0)
        .custom_flags(0)
        .attributes(0)
        .security_qos_flags(0);
    let _: u32 = MetadataExt::file_attributes(metadata);

    fn file_traits<T: AsHandle + AsRawHandle + TryFrom<OwnedHandle, Error = std::io::Error>>() {}
    fn socket_traits<T: AsSocket + AsRawSocket + TryFrom<OwnedSocket, Error = std::io::Error>>() {}
    file_traits::<runite::fs::File>();
    socket_traits::<runite::net::TcpStream>();
    socket_traits::<runite::net::TcpListener>();
    socket_traits::<runite::net::UdpSocket>();
    socket_traits::<runite::net::TcpSocket>();

    let _: fn(OwnedHandle) -> std::io::Result<runite::fs::File> =
        runite::fs::File::from_owned;
    let _: fn(OwnedSocket) -> std::io::Result<runite::net::TcpStream> =
        runite::net::TcpStream::from_owned;
    let _: fn() -> std::io::Result<runite::signal::windows::CtrlC> =
        runite::signal::windows::ctrl_c;
    let _: fn() -> std::io::Result<runite::signal::windows::CtrlBreak> =
        runite::signal::windows::ctrl_break;
}
"#;

fn render(surfaces: &[ApiSurface]) -> String {
    let mut portable = surfaces[0].api(ApiFeatureSet::Default).clone();
    for surface in &surfaces[1..] {
        portable = portable
            .intersection(surface.api(ApiFeatureSet::Default))
            .cloned()
            .collect();
    }

    let mut all_items = BTreeSet::new();
    for surface in surfaces {
        all_items.extend(surface.api(ApiFeatureSet::All).iter().cloned());
    }
    let modules: Vec<String> = all_items
        .iter()
        .filter_map(|line| declared_module(line))
        .collect();

    let mut out = String::new();
    out.push_str("# `runite` public API surface\n\n");
    out.push_str(&format!(
        "_Generated by `xtask api-report` (`mise run api-report`) with \
         `cargo-public-api`, Rust `{RUSTDOC_TOOLCHAIN}`, and auto-trait, blanket, \
         and derived impls omitted. Do not edit by hand._\n\n"
    ));
    out.push_str(
        "The portable default section is the exact intersection of the four \
         supported targets. A target's default surface is that section plus its \
         target delta. The `hyper`, `futures-compat`, and combined all-features \
         surfaces are reconstructed by adding their labeled additions. This \
         decomposition makes platform-only APIs an \
         explicit review boundary: deltas are reserved for genuine OS interop \
         (`fd`, Unix sockets/signals, Windows handles/sockets/signals and \
         `os::windows`).\n\n",
    );
    out.push_str("## Surface summary\n\n");
    out.push_str(
        "| Target | Default | `hyper` | `futures-compat` | All features | Default target delta |\n",
    );
    out.push_str("| --- | ---: | ---: | ---: | ---: | ---: |\n");
    for surface in surfaces {
        let default = surface.api(ApiFeatureSet::Default);
        let target_delta = default.difference(&portable).count();
        out.push_str(&format!(
            "| {} (`{}`) | {} | {} | {} | {} | {} |\n",
            surface.target.label,
            surface.target.triple,
            default.len(),
            surface.api(ApiFeatureSet::Hyper).len(),
            surface.api(ApiFeatureSet::FuturesCompat).len(),
            surface.api(ApiFeatureSet::All).len(),
            target_delta
        ));
    }
    out.push_str(&format!(
        "\nPortable default items: **{}**\n\n",
        portable.len()
    ));
    out.push_str("## Compile-checked handle contract\n\n");
    out.push_str(
        "`cargo-public-api` lists re-exported handle types but currently omits \
         their inherent methods because their definitions live in a private \
         implementation module. `xtask api-report` therefore compiles this \
         contract for every target with default, each individual feature, and \
         all features:\n\n\
         - `ThreadHandle::{queue_macrotask, is_closed, is_current}`\n\
         - `WorkerHandle::{queue_macrotask, is_finished, join, thread}`\n\
         - `WorkerJoin: Future<Output = Result<(), WorkerJoinError>>`\n\
         - `WorkerJoinError::{is_setup_panicked, is_runtime_panicked}`\n\
         - `JoinHandle::{abort, is_finished, abort_handle}` and \
         `AbortHandle::{abort, is_finished}`\n\
         - `TimeoutHandle::cancel` and `IntervalHandle::cancel`\n\
         - Windows handle/socket adoption traits and \
         `os::windows::fs`/`signal::windows` APIs\n\n",
    );

    render_grouped(&mut out, "Portable default surface", &portable, &modules);
    for surface in surfaces {
        let delta = surface
            .api(ApiFeatureSet::Default)
            .difference(&portable)
            .cloned()
            .collect::<BTreeSet<_>>();
        render_grouped(
            &mut out,
            &format!(
                "{} default target delta (`{}`)",
                surface.target.label, surface.target.triple
            ),
            &delta,
            &modules,
        );
    }
    for feature_set in [
        ApiFeatureSet::Hyper,
        ApiFeatureSet::FuturesCompat,
        ApiFeatureSet::All,
    ] {
        for surface in surfaces {
            let additions = surface
                .api(feature_set)
                .difference(surface.api(ApiFeatureSet::Default))
                .cloned()
                .collect::<BTreeSet<_>>();
            render_grouped(
                &mut out,
                &format!(
                    "{} `{}` additions (`{}`)",
                    surface.target.label,
                    feature_set.label(),
                    surface.target.triple
                ),
                &additions,
                &modules,
            );
        }
    }
    out
}

fn render_grouped(out: &mut String, title: &str, items: &BTreeSet<String>, modules: &[String]) {
    out.push_str(&format!("## {title}\n\n"));
    out.push_str(&format!("Items: **{}**\n\n", items.len()));
    let mut groups: BTreeMap<String, Vec<&str>> = BTreeMap::new();
    for item in items {
        groups
            .entry(module_of(item, modules))
            .or_default()
            .push(item);
    }
    for (module, module_items) in groups {
        out.push_str(&format!("### `{module}`\n\n```rust\n"));
        for item in module_items {
            out.push_str(item);
            out.push('\n');
        }
        out.push_str("```\n\n");
    }
}

fn declared_module(line: &str) -> Option<String> {
    let rest = line.strip_prefix("pub mod ")?;
    Some(rest.split_whitespace().next().unwrap_or(rest).to_string())
}

fn module_of(line: &str, modules: &[String]) -> String {
    if let Some(module) = declared_module(line) {
        return module;
    }
    let path = item_path(line);
    let segments: Vec<&str> = path.split("::").collect();
    for end in (1..=segments.len()).rev() {
        let candidate = segments[..end].join("::");
        if modules.contains(&candidate) {
            return candidate;
        }
    }
    CRATE.to_string()
}

fn item_path(line: &str) -> String {
    let needle = format!("{CRATE}::");
    let Some(start) = line.find(&needle) else {
        return CRATE.to_string();
    };
    let tail = &line[start..];
    let end = tail
        .find(|character: char| {
            !(character.is_alphanumeric() || character == '_' || character == ':')
        })
        .unwrap_or(tail.len());
    tail[..end].trim_end_matches(':').to_string()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn target(triple: &str) -> Target {
        *SUPPORTED_TARGETS
            .iter()
            .find(|target| target.triple == triple)
            .expect("test target must be supported")
    }

    #[test]
    fn groups_items_under_the_longest_public_module() {
        let modules = vec![
            "runite".to_owned(),
            "runite::io".to_owned(),
            "runite::io::compat".to_owned(),
        ];
        assert_eq!(
            module_of("pub struct runite::io::compat::Compat<T>", &modules),
            "runite::io::compat"
        );
        assert_eq!(
            module_of("pub fn runite::io::copy()", &modules),
            "runite::io"
        );
    }

    #[test]
    fn permits_only_genuine_platform_extension_lines() {
        assert!(is_platform_extension(
            target("aarch64-unknown-linux-gnu"),
            "impl std::os::fd::AsFd for runite::fs::File"
        ));
        assert!(is_platform_extension(
            target("x86_64-pc-windows-msvc"),
            "pub fn runite::fs::OpenOptions::security_qos_flags(&mut self, u32) -> &mut Self"
        ));
        assert!(!is_platform_extension(
            target("x86_64-pc-windows-msvc"),
            "pub fn runite::fs::read()"
        ));
    }
}
