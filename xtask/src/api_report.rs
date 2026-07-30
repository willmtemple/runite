use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::path::Path;
use std::process::Command;

use crate::command::{parse_flags, run_output, run_status, workspace_root};
use crate::targets::{SUPPORTED_TARGETS, Target};

const CRATE: &str = "runite";
const OUTPUT: &str = "docs/public-api.md";
/// Auto-trait and derived impls, split into their own report.
///
/// `cargo public-api` synthesizes five to seven of these lines for every public
/// type, which is why [`OMIT`] drops them: mixed into [`OUTPUT`] they bury the
/// items a reviewer is actually reading. Dropping them entirely was worse —
/// losing `Send` on a guard or `Clone` on a config is a semver break the drift
/// gate could not see, and the omission had already been narrated by hand in
/// two shipping documents. A second file keeps both properties: the surface
/// stays readable, and the breaks still fail `--check`.
const OUTPUT_TRAITS: &str = "docs/public-api-traits.md";
/// Exact nightly used for rustdoc-JSON generation. The rendered item listing
/// varies across rustdoc versions, so bump this deliberately and regenerate
/// the report in the same change.
const RUSTDOC_TOOLCHAIN: &str = "nightly-2026-07-01";
const OMIT: &str = "blanket-impls,auto-trait-impls,auto-derived-impls";
/// What the second pass keeps. Blanket impls stay omitted in both: they are a
/// property of the foreign trait, not of runite's types, so a change in one is
/// not a change to this crate's surface.
const OMIT_TRAITS: &str = "blanket-impls";

struct ApiSurface {
    target: Target,
    feature_sets: BTreeMap<ApiFeatureSet, BTreeSet<String>>,
    trait_sets: BTreeMap<ApiFeatureSet, BTreeSet<String>>,
}

#[derive(Clone, Copy, Eq, Ord, PartialEq, PartialOrd)]
enum ApiFeatureSet {
    Default,
    Hyper,
    FuturesCompat,
    Rustls,
    All,
}

const API_FEATURE_SETS: [ApiFeatureSet; 5] = [
    ApiFeatureSet::Default,
    ApiFeatureSet::Hyper,
    ApiFeatureSet::FuturesCompat,
    ApiFeatureSet::Rustls,
    ApiFeatureSet::All,
];

/// Every optional feature, in report order. `All` is derived from these rather
/// than listed, so adding a feature above cannot leave the checks behind.
const OPTIONAL_FEATURE_SETS: [ApiFeatureSet; 3] = [
    ApiFeatureSet::Hyper,
    ApiFeatureSet::FuturesCompat,
    ApiFeatureSet::Rustls,
];

impl ApiFeatureSet {
    fn label(self) -> &'static str {
        match self {
            Self::Default => "default",
            Self::Hyper => "hyper",
            Self::FuturesCompat => "futures-compat",
            Self::Rustls => "rustls",
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

    fn traits(&self, feature_set: ApiFeatureSet) -> &BTreeSet<String> {
        self.trait_sets
            .get(&feature_set)
            .expect("every API feature set must be collected")
    }
}

pub(crate) fn run(args: &[String]) -> Result<(), String> {
    let check = parse_flags(args, &["--check"])?;
    let mut surfaces = Vec::with_capacity(SUPPORTED_TARGETS.len());

    for target in SUPPORTED_TARGETS {
        let mut feature_sets = BTreeMap::new();
        let mut trait_sets = BTreeMap::new();
        for feature_set in API_FEATURE_SETS {
            println!(
                "xtask: collecting {} API for {}",
                feature_set.label(),
                target.triple
            );
            let items = run_public_api(target, feature_set, OMIT)?;
            // The second pass reuses the rustdoc JSON the first one built, so
            // it costs a parse rather than a documentation run.
            let with_traits = run_public_api(target, feature_set, OMIT_TRAITS)?;
            trait_sets.insert(
                feature_set,
                with_traits.difference(&items).cloned().collect(),
            );
            feature_sets.insert(feature_set, items);
            check_api_probe(target, feature_set)?;
        }

        let default = feature_sets
            .get(&ApiFeatureSet::Default)
            .expect("default API must be collected");
        for feature_set in OPTIONAL_FEATURE_SETS
            .iter()
            .copied()
            .chain([ApiFeatureSet::All])
        {
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
        for feature_set in OPTIONAL_FEATURE_SETS {
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
            trait_sets,
        });
    }
    validate_portability(&surfaces)?;

    let outputs = [
        (OUTPUT, render(&surfaces)),
        (OUTPUT_TRAITS, render_traits(&surfaces)),
    ];
    for (path, rendered) in outputs {
        let output = workspace_root().join(path);
        if check.contains("--check") {
            let current = fs::read_to_string(&output).unwrap_or_default();
            if current != rendered {
                return Err(format!(
                    "{path} is out of date; run `mise run api-report` and commit the result"
                ));
            }
            println!("xtask: {path} is up to date");
            continue;
        }
        fs::write(&output, rendered).map_err(|error| format!("failed to write {path}: {error}"))?;
        println!("xtask: wrote {path}");
    }
    Ok(())
}

fn run_public_api(
    target: Target,
    feature_set: ApiFeatureSet,
    omit: &str,
) -> Result<BTreeSet<String>, String> {
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
        omit,
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
        ApiFeatureSet::Rustls => {
            command.args(["--no-default-features", "--features", "rustls"]);
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
    let linux_items = [
        "pub mod runite::os::linux",
        "pub trait runite::os::linux::BuilderExt",
    ];

    if target.triple.contains("windows") {
        for needle in windows_items {
            require_api(default, target, needle)?;
        }
        for needle in unix_items.iter().chain(&linux_items) {
            reject_api(default, target, needle)?;
        }
    } else {
        for needle in unix_items {
            require_api(default, target, needle)?;
        }
        for needle in windows_items {
            reject_api(default, target, needle)?;
        }
        // `os::linux` is the only surface that splits the Unix targets, so it
        // is checked from both sides rather than only asserted present.
        for needle in linux_items {
            if target.triple.contains("linux") {
                require_api(default, target, needle)?;
            } else {
                reject_api(default, target, needle)?;
            }
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

/// Items exempted from the "every Unix surface is identical" rule.
///
/// **This list should be empty**, and is. An entry here is a suspended
/// promise: a public item that exists on one Unix target and not another
/// because the work is unfinished, so each one must name the issue that
/// deletes it again.
///
/// An `os::linux` / `os::unix` / `os::windows` module is *not* an exemption
/// and must never be listed here. Those modules are the sanctioned way to
/// expose API that only one platform can implement — permanent, named for the
/// platform in the path a caller has to type, and checked from both sides by
/// `validate_surface`, which requires each one on its own targets and rejects
/// it everywhere else. An exemption is the opposite: unnamed, unchecked, and
/// meant to disappear. `is_platform_extension` recognises the `os::*` modules;
/// this list exists only for the divergences it cannot.
const PORTABILITY_EXEMPTIONS: &[(&str, &str)] = &[];

fn is_exempt(line: &str) -> bool {
    PORTABILITY_EXEMPTIONS
        .iter()
        .any(|(needle, _)| line.contains(needle))
}

fn validate_portability(surfaces: &[ApiSurface]) -> Result<(), String> {
    // The Unix targets are held to a stricter standard than the portable
    // intersection below: everything except a deliberate Unix split must match
    // exactly, so a `#[cfg(target_os)]` that leaks a difference between Linux
    // and macOS is caught even though both are Unix.
    let mut unix_surfaces = surfaces
        .iter()
        .filter(|surface| !surface.target.triple.contains("windows"));
    if let Some(reference) = unix_surfaces.next() {
        for surface in unix_surfaces {
            for feature_set in API_FEATURE_SETS {
                if unix_comparable_part(reference, feature_set)
                    != unix_comparable_part(surface, feature_set)
                {
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

/// A target's surface with only its deliberate Unix splits removed.
///
/// Deliberately *not* `is_platform_extension`: most of what that excuses —
/// `os::unix`, `fd`, `net::unix`, `signal::unix` — is common to every Unix
/// target, and leaving those lines in the comparison is what makes a
/// `#[cfg(target_os)]` hidden inside one of them fail this check. Only the
/// namespaces that are Linux-only by design come out.
fn unix_comparable_part(surface: &ApiSurface, feature_set: ApiFeatureSet) -> BTreeSet<&str> {
    surface
        .api(feature_set)
        .iter()
        .map(String::as_str)
        .filter(|line| !is_unix_split(surface.target, line))
        .collect()
}

/// Public API that exists on some Unix targets and not others on purpose.
///
/// io_uring tuning has no kqueue counterpart, so `os::linux` is the one
/// namespace in this category. It is an OS extension, not a
/// [`PORTABILITY_EXEMPTIONS`] entry: it is permanent, the platform is in the
/// path the caller types, and `validate_surface` requires it on Linux while
/// rejecting it on macOS and Windows, so widening the split fails there.
///
/// The `starts_with` patterns here and in [`is_platform_extension`] end at the
/// argument list. Without it the pattern is a prefix on the *name*, so a new
/// Linux-only `Builder::ring_entries_max` would inherit `ring_entries`'
/// approval and never reach the Unix-identity check it exists to fail.
fn is_unix_split(target: Target, line: &str) -> bool {
    is_exempt(line)
        || (target.triple.contains("linux")
            && (line.contains("runite::os::linux")
                // Like `Command::pre_exec`, `BuilderExt::ring_entries` renders
                // as an inherent-looking method on its receiver.
                || line.starts_with("pub fn runite::Builder::ring_entries(")))
}

fn is_platform_extension(target: Target, line: &str) -> bool {
    // An exempted item is not an OS extension, it is unfinished — but both
    // checks have to agree on that, or one fails while the other passes.
    if is_exempt(line) {
        return true;
    }
    if target.triple.contains("windows") {
        line.contains("std::os::windows")
            || line.contains("runite::os::windows")
            || line.contains("runite::signal::windows")
            || line.starts_with("pub fn runite::fs::Metadata::file_attributes(")
            || line.starts_with("pub fn runite::fs::OpenOptions::access_mode(")
            || line.starts_with("pub fn runite::fs::OpenOptions::attributes(")
            || line.starts_with("pub fn runite::fs::OpenOptions::custom_flags(")
            || line.starts_with("pub fn runite::fs::OpenOptions::security_qos_flags(")
            || line.starts_with("pub fn runite::fs::OpenOptions::share_mode(")
    } else {
        // The Linux-only namespaces are excused here too, so this check and
        // the Unix-identity one above cannot disagree about what is approved.
        is_unix_split(target, line)
            || line.contains("std::os::fd")
            || line.contains("std::os::unix")
            || line.contains("runite::fd")
            || line.contains("runite::net::unix")
            || line.contains("runite::net::Unix")
            || line.contains("runite::os::unix")
            || line.contains("runite::signal::unix")
            || line.starts_with("pub fn runite::process::ExitStatus::signal(")
            // `CommandExt::pre_exec` is reachable as an inherent-looking method
            // on `Command`, so it renders without the trait's module path.
            || line.starts_with("pub unsafe fn runite::process::Command::pre_exec(")
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
         rustls = [\"runite/rustls\"]\n\
         all-features = [\"hyper\", \"futures-compat\", \"rustls\"]\n\n\
         [dependencies]\n\
         runite = {{ path = \"{path}\", default-features = false }}\n"
    )
}

const API_PROBE: &str = r#"
#![deny(warnings)]
#![allow(dead_code)]

use core::fmt::{Debug, Display};
use core::future::Future;
use core::hash::Hash;
use runite::{
    AbortHandle, CancelOnDrop, IntervalHandle, JoinError, JoinHandle, QueueError, RuntimeId,
    ThreadHandle, TimeoutHandle, TimerCancel, TurnId, WorkerHandle, WorkerJoin, WorkerJoinError,
    YieldNow,
};

fn assert_worker_join_future<F: Future<Output = Result<(), WorkerJoinError>>>() {}
fn assert_join_future<F: Future<Output = Result<u32, JoinError>>>() {}
fn assert_yield_future<F: Future<Output = ()>>() {}
fn assert_debug<T: Debug>() {}
fn assert_error<T: std::error::Error + Display>() {}
fn assert_id<T: Copy + Debug + Display + Eq + Ord + Hash>() {}

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
    let _: ThreadHandle = thread.clone();

    let _ = worker.queue_macrotask(|| {});
    let _ = worker.is_finished();
    let _: WorkerJoin = worker.join();
    let _: ThreadHandle = worker.thread();
    assert_worker_join_future::<WorkerJoin>();

    join.abort();
    let _ = join.is_finished();
    let _: AbortHandle = join.abort_handle();
    assert_join_future::<JoinHandle<u32>>();
    abort.abort();
    let _ = abort.is_finished();
    let _: AbortHandle = abort.clone();
    timeout.cancel();
    interval.cancel();
    let _: TimeoutHandle = timeout.clone();
    let _: IntervalHandle = interval.clone();

    let setup = WorkerJoinError::SetupPanicked;
    let _ = setup.is_setup_panicked();
    let _ = setup.is_runtime_panicked();
    let _ = WorkerJoinError::RuntimePanicked;

    assert_debug::<ThreadHandle>();
    assert_debug::<WorkerHandle>();
    assert_debug::<WorkerJoin>();
    assert_debug::<JoinHandle<()>>();
    assert_debug::<AbortHandle>();
    assert_debug::<TimeoutHandle>();
    assert_debug::<IntervalHandle>();
}

/// The cancel-on-drop guard, whose whole surface is inherent methods and
/// operator impls that `cargo public-api` cannot see through the private module
/// its type is defined in.
fn cancel_on_drop_contract(timeout: TimeoutHandle, interval: IntervalHandle) {
    let guard: CancelOnDrop<TimeoutHandle> = timeout.cancel_on_drop();
    // `Deref`, so the guard is usable wherever the token was.
    let _: &TimeoutHandle = &guard;
    guard.cancel();

    let guard: CancelOnDrop<IntervalHandle> = interval.cancel_on_drop();
    let token: IntervalHandle = guard.into_inner();
    TimerCancel::cancel_timer(&token);
    assert_debug::<CancelOnDrop<IntervalHandle>>();
}

/// Errors, identifiers, and the small futures. Every one of these types is
/// re-exported from a private module, so the report shows the name and nothing
/// else.
fn value_contract() {
    assert_error::<QueueError>();
    let _ = QueueError::Closed;
    let _ = QueueError::Full;

    assert_id::<RuntimeId>();
    assert_id::<TurnId>();
    let _: Option<RuntimeId> = runite::current_runtime_id();
    let _: Option<TurnId> = runite::current_turn();

    assert_yield_future::<YieldNow>();
    let _: YieldNow = runite::yield_now();
    assert_debug::<YieldNow>();

    runite::on_shutdown(|| {});
    runite::shutdown();
}

// `CancelOnDrop` and `TimerCancel` are re-exported the same way the handles
// are, so `cargo-public-api` shows only the bare names — including the `Clone`
// supertrait that makes `into_inner` total. Pin the whole shape here.
fn timer_guard_contract(timeout: TimeoutHandle, interval: IntervalHandle) {
    // `H: TimerCancel` alone must imply `Clone`; naming both bounds here would
    // pass even if the supertrait were dropped.
    fn assert_token_is_clone<H: TimerCancel>() {
        fn requires_clone<C: Clone>() {}
        requires_clone::<H>();
    }
    assert_token_is_clone::<TimeoutHandle>();
    assert_token_is_clone::<IntervalHandle>();

    let guard: CancelOnDrop<TimeoutHandle> = timeout.cancel_on_drop();
    let _: TimeoutHandle = guard.into_inner();

    let guard: CancelOnDrop<IntervalHandle> = interval.cancel_on_drop();
    guard.cancel_timer();
    guard.cancel();
}

fn issue_9_traits<
    R: runite::io::AsyncRead + runite::io::AsyncBufRead + runite::io::AsyncSeek,
    W: runite::io::AsyncWrite,
>() {
}

fn builder_contract() -> std::io::Result<()> {
    let builder: runite::Builder = runite::Builder::new();
    let runtime: runite::Runtime = builder.build()?;
    runtime.run();
    runtime.run_until_stalled();
    runtime.run_ready_tasks();
    let _: u32 = runtime.block_on(async { 1u32 });
    Ok(())
}

#[cfg(target_os = "linux")]
fn linux_builder_contract() {
    use runite::os::linux::BuilderExt;

    let _: runite::Builder = runite::Builder::new().ring_entries(32);
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
         and derived impls omitted — those are diffed in \
         [`public-api-traits.md`](public-api-traits.md). Do not edit by hand._\n\n"
    ));
    out.push_str(
        "The portable default section is the exact intersection of the four \
         supported targets. A target's default surface is that section plus its \
         target delta. The `hyper`, `futures-compat`, `rustls`, and combined \
         all-features surfaces are reconstructed by adding their labeled \
         additions. This \
         decomposition makes platform-only APIs an \
         explicit review boundary: deltas are reserved for genuine OS interop \
         (`fd`, Unix sockets/signals, Windows handles/sockets/signals and \
         `os::windows`).\n\n",
    );
    out.push_str("## Surface summary\n\n");
    out.push_str(
        "| Target | Default | `hyper` | `futures-compat` | `rustls` | All features | \
         Default target delta |\n",
    );
    out.push_str("| --- | ---: | ---: | ---: | ---: | ---: | ---: |\n");
    for surface in surfaces {
        let default = surface.api(ApiFeatureSet::Default);
        let target_delta = default.difference(&portable).count();
        out.push_str(&format!(
            "| {} (`{}`) | {} | {} | {} | {} | {} | {} |\n",
            surface.target.label,
            surface.target.triple,
            default.len(),
            surface.api(ApiFeatureSet::Hyper).len(),
            surface.api(ApiFeatureSet::FuturesCompat).len(),
            surface.api(ApiFeatureSet::Rustls).len(),
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
        "`cargo-public-api` lists re-exported types but currently omits \
         everything *on* them — inherent methods, variants, and trait impls — \
         because their definitions live in a private implementation module. \
         Every type below reaches the crate root through `platform::\
         runtime_shared`, so the sections further down name it and stop. \
         `xtask api-report` therefore compiles this contract for every target \
         with default, each individual feature, and all features, which is what \
         makes these members part of the gated surface rather than an \
         unwitnessed promise:\n\n\
         - `ThreadHandle::{queue_macrotask, is_closed, is_current}`, `Clone`\n\
         - `WorkerHandle::{queue_macrotask, is_finished, join, thread}`\n\
         - `WorkerJoin: Future<Output = Result<(), WorkerJoinError>>`\n\
         - `WorkerJoinError::{SetupPanicked, RuntimePanicked, \
         is_setup_panicked, is_runtime_panicked}`\n\
         - `JoinHandle::{abort, is_finished, abort_handle}` and \
         `JoinHandle<T>: Future<Output = Result<T, JoinError>>`\n\
         - `AbortHandle::{abort, is_finished}`, `Clone`\n\
         - `TimeoutHandle`/`IntervalHandle`: `cancel`, `cancel_on_drop`, \
         `Clone`\n\
         - `CancelOnDrop::{cancel, into_inner}`, `Deref` to its token, and \
         `TimerCancel::cancel_timer`\n\
         - `QueueError::{Closed, Full}` as an `Error`, and `RuntimeId`/`TurnId` \
         as `Copy + Display + Ord + Hash` identifiers\n\
         - `YieldNow: Future<Output = ()>`, plus `yield_now`, `current_turn`, \
         `current_runtime_id`, `on_shutdown` and `shutdown`\n\
         - `Debug` on every handle above\n\
         - `Builder`/`Runtime` construction and every loop entry point on \
         them, plus `os::linux::BuilderExt` reached through a `Builder` on \
         Linux\n\
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
    for feature_set in OPTIONAL_FEATURE_SETS
        .iter()
        .copied()
        .chain([ApiFeatureSet::All])
    {
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

/// Renders the auto-trait and derived impls, decomposed the same way as
/// [`render`] so the two files read against each other.
fn render_traits(surfaces: &[ApiSurface]) -> String {
    let mut portable = surfaces[0].traits(ApiFeatureSet::Default).clone();
    for surface in &surfaces[1..] {
        portable = portable
            .intersection(surface.traits(ApiFeatureSet::Default))
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
    out.push_str("# `runite` auto-trait and derived impls\n\n");
    out.push_str(&format!(
        "_Generated by `xtask api-report` (`mise run api-report`) with \
         `cargo-public-api`, Rust `{RUSTDOC_TOOLCHAIN}`. Do not edit by hand._\n\n"
    ));
    out.push_str(
        "The companion to [`public-api.md`](public-api.md), which omits these \
         so that the items a reviewer reads are not buried under five to seven \
         synthesized lines per type. They are still public API: losing `Send` \
         on a guard, `Unpin` on a future, or a derived `Clone` breaks callers \
         exactly as removing a method does, so they are diffed here instead of \
         going unwitnessed. The decomposition matches the other file — portable \
         intersection, per-target delta, per-feature additions.\n\n\
         Blanket impls are omitted from both files: those follow from a foreign \
         trait's own definition rather than from anything runite declares.\n\n\
         **This file cannot see types re-exported from a private module.** \
         `cargo-public-api` renders those as a bare `pub use` with nothing \
         attached, so the auto traits of `ThreadHandle`, `JoinHandle`, \
         `CancelOnDrop` and their neighbours are gated by the `trybuild` cases \
         in `tests/ui/` instead — `pass/auto_traits.rs` for the ones that must \
         stay `Send`, `fail/join_handle_send.rs` and \
         `fail/cancel_on_drop_send.rs` for the ones that must not.\n\n",
    );

    out.push_str("## Summary\n\n");
    out.push_str("| Target | Default | `hyper` | `futures-compat` | `rustls` | All features |\n");
    out.push_str("| --- | ---: | ---: | ---: | ---: | ---: |\n");
    for surface in surfaces {
        out.push_str(&format!(
            "| {} (`{}`) | {} | {} | {} | {} | {} |\n",
            surface.target.label,
            surface.target.triple,
            surface.traits(ApiFeatureSet::Default).len(),
            surface.traits(ApiFeatureSet::Hyper).len(),
            surface.traits(ApiFeatureSet::FuturesCompat).len(),
            surface.traits(ApiFeatureSet::Rustls).len(),
            surface.traits(ApiFeatureSet::All).len(),
        ));
    }
    out.push('\n');

    render_grouped(&mut out, "Portable default impls", &portable, &modules);
    for surface in surfaces {
        let delta = surface
            .traits(ApiFeatureSet::Default)
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
    for feature_set in OPTIONAL_FEATURE_SETS
        .iter()
        .copied()
        .chain([ApiFeatureSet::All])
    {
        for surface in surfaces {
            let additions = surface
                .traits(feature_set)
                .difference(surface.traits(ApiFeatureSet::Default))
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

    /// A prefix that stops at the method name approves every method starting
    /// with it, so a new Linux-only knob could inherit `ring_entries`'
    /// exemption without anyone deciding it should have one.
    #[test]
    fn extension_prefixes_stop_at_the_argument_list() {
        let linux = target("x86_64-unknown-linux-gnu");
        assert!(is_unix_split(
            linux,
            "pub fn runite::Builder::ring_entries(self, u32) -> Self"
        ));
        assert!(!is_unix_split(
            linux,
            "pub fn runite::Builder::ring_entries_max(self, u32) -> Self"
        ));
        assert!(!is_platform_extension(
            linux,
            "pub fn runite::Builder::ring_entries_max(self, u32) -> Self"
        ));
        assert!(!is_platform_extension(
            target("x86_64-pc-windows-msvc"),
            "pub fn runite::fs::OpenOptions::share_mode_default(&mut self) -> &mut Self"
        ));
    }

    #[test]
    fn os_linux_is_an_extension_only_on_linux() {
        assert!(is_platform_extension(
            target("aarch64-unknown-linux-gnu"),
            "pub trait runite::os::linux::BuilderExt"
        ));
        assert!(!is_platform_extension(
            target("aarch64-apple-darwin"),
            "pub trait runite::os::linux::BuilderExt"
        ));
    }
}
