use std::collections::BTreeSet;
use std::fs;
use std::path::Path;
use std::process::Command;

use crate::command::{parse_flags, run_output, run_status, workspace_root};
use crate::targets::SUPPORTED_TARGETS;

const RELEASE_DIR: &str = "target/xtask-release";
const MSRV: &str = "1.88.0";

pub(crate) fn run(args: &[String]) -> Result<(), String> {
    let flags = parse_flags(args, &["--msrv", "--publish-dry-run", "--allow-dirty"])?;
    // Off by default, and that default is load-bearing. `--allow-dirty` makes
    // `cargo package` write `"dirty": true` into `.cargo_vcs_info.json`, which
    // changes the bytes of the produced `.crate` and therefore its checksum.
    // The release workflow compares crates.io's recorded checksum against a
    // local repackage, so verifying from a dirty tree would "verify" an
    // artifact the real release can never reproduce — and a mismatch there
    // wedges that version permanently. Pass it only when deliberately checking
    // an uncommitted tree, and do not trust the result for release purposes.
    let allow_dirty = flags.contains("--allow-dirty");
    let root = workspace_root();
    let runite_version = package_version(&root.join("Cargo.toml"))?;
    let macro_version = package_version(&root.join("proc_macros/Cargo.toml"))?;
    if runite_version != macro_version {
        return Err(format!(
            "crate versions must move in lockstep: runite={runite_version}, \
             runite-proc-macros={macro_version}"
        ));
    }

    let release_dir = root.join(RELEASE_DIR);
    if release_dir.exists() {
        fs::remove_dir_all(&release_dir)
            .map_err(|error| format!("failed to clean {}: {error}", release_dir.display()))?;
    }
    fs::create_dir_all(&release_dir)
        .map_err(|error| format!("failed to create {}: {error}", release_dir.display()))?;

    let main_list = package_list(&root, "runite", allow_dirty)?;
    let macro_list = package_list(&root, "runite-proc-macros", allow_dirty)?;
    verify_package_entries(
        "runite package list",
        &main_list,
        &[
            "Cargo.toml",
            "README.md",
            "ARCHITECTURE.md",
            "CHANGELOG.md",
            "LICENSE-APACHE",
            "LICENSE-MIT",
            "docs/WINDOWS.md",
            "docs/MIGRATING-0.2.md",
            "docs/MIGRATING-0.3.md",
            "src/lib.rs",
        ],
    )?;
    verify_package_entries(
        "runite-proc-macros package list",
        &macro_list,
        &[
            "Cargo.toml",
            "README.md",
            "LICENSE-APACHE",
            "LICENSE-MIT",
            "src/lib.rs",
        ],
    )?;

    let package_target = release_dir.join("package");
    package_workspace(&root, &package_target, allow_dirty)?;

    let artifacts = package_target.join("package");
    let main_artifact = artifacts.join(format!("runite-{runite_version}.crate"));
    let macro_artifact = artifacts.join(format!("runite-proc-macros-{macro_version}.crate"));
    require_file(&main_artifact)?;
    require_file(&macro_artifact)?;

    let unpacked = release_dir.join("unpacked");
    fs::create_dir_all(&unpacked)
        .map_err(|error| format!("failed to create {}: {error}", unpacked.display()))?;
    unpack_crate(&root, &main_artifact, &unpacked)?;
    unpack_crate(&root, &macro_artifact, &unpacked)?;

    let main_dir = unpacked.join(format!("runite-{runite_version}"));
    let macro_dir = unpacked.join(format!("runite-proc-macros-{macro_version}"));
    let main_entries = package_files(&main_dir)?;
    let macro_entries = package_files(&macro_dir)?;
    verify_package_entries(
        "unpacked runite artifact",
        &main_entries,
        &[
            "Cargo.toml",
            "Cargo.toml.orig",
            "README.md",
            "ARCHITECTURE.md",
            "CHANGELOG.md",
            "LICENSE-APACHE",
            "LICENSE-MIT",
            "docs/WINDOWS.md",
            "docs/MIGRATING-0.2.md",
            "docs/MIGRATING-0.3.md",
            "src/lib.rs",
        ],
    )?;
    verify_package_entries(
        "unpacked runite-proc-macros artifact",
        &macro_entries,
        &[
            "Cargo.toml",
            "Cargo.toml.orig",
            "README.md",
            "LICENSE-APACHE",
            "LICENSE-MIT",
            "src/lib.rs",
        ],
    )?;
    verify_license_copy(&root, &macro_dir, "LICENSE-APACHE")?;
    verify_license_copy(&root, &macro_dir, "LICENSE-MIT")?;

    make_manifest_standalone(&macro_dir.join("Cargo.toml"))?;
    let dependency_path = format!("../runite-proc-macros-{macro_version}");
    let main_manifest = main_dir.join("Cargo.toml");
    let manifest = fs::read_to_string(&main_manifest)
        .map_err(|error| format!("failed to read {}: {error}", main_manifest.display()))?;
    let patched = standalone_manifest(&patch_proc_macro_path(&manifest, &dependency_path)?);
    fs::write(&main_manifest, patched)
        .map_err(|error| format!("failed to patch {}: {error}", main_manifest.display()))?;
    let main_lock = main_dir.join("Cargo.lock");
    let lock = fs::read_to_string(&main_lock)
        .map_err(|error| format!("failed to read {}: {error}", main_lock.display()))?;
    verify_lock_records_packaged_macro(&lock, &macro_artifact)?;
    let patched_lock = patch_proc_macro_lock(&lock, &macro_version)?;
    fs::write(&main_lock, patched_lock)
        .map_err(|error| format!("failed to patch {}: {error}", main_lock.display()))?;

    verify_unpacked_packages(
        &root,
        &release_dir,
        &main_dir,
        &macro_dir,
        flags.contains("--msrv"),
    )?;

    if flags.contains("--publish-dry-run") {
        let mut command = Command::new("cargo");
        command.current_dir(&root).args(["publish", "--dry-run"]);
        if allow_dirty {
            command.arg("--allow-dirty");
        }
        command
            .args(["--package", "runite-proc-macros", "--target-dir"])
            .arg(release_dir.join("publish-dry-run"));
        run_status(&mut command, "runite-proc-macros publish dry-run")?;
    }

    println!(
        "xtask: verified release artifacts:\n  {}\n  {}",
        main_artifact.display(),
        macro_artifact.display()
    );
    Ok(())
}

const FORBIDDEN_PACKAGE_PATHS: &[&str] = &[
    ".gitignore",
    ".github",
    ".claude",
    "target",
    "proc_macros",
    "xtask",
    "AGENTS.md",
    "CONTRIBUTING.md",
    "deny.toml",
    "mise.toml",
    "mise.lock",
    "rust-toolchain.toml",
    "SECURITY.md",
    "docs/public-api.md",
    "docs/public-api-traits.md",
];

fn package_version(manifest: &Path) -> Result<String, String> {
    let content = fs::read_to_string(manifest)
        .map_err(|error| format!("failed to read {}: {error}", manifest.display()))?;
    parse_package_version(&content)
        .ok_or_else(|| format!("{} has no [package] version", manifest.display()))
}

fn parse_package_version(manifest: &str) -> Option<String> {
    let mut in_package = false;
    for line in manifest.lines() {
        let trimmed = line.trim();
        if trimmed == "[package]" {
            in_package = true;
            continue;
        }
        if in_package && trimmed.starts_with('[') {
            return None;
        }
        if in_package && trimmed.starts_with("version") {
            let value = trimmed.strip_prefix("version")?.trim_start();
            let value = value.strip_prefix('=')?.trim();
            return value
                .strip_prefix('"')
                .and_then(|value| value.strip_suffix('"'))
                .map(str::to_owned);
        }
    }
    None
}

fn package_list(root: &Path, package: &str, allow_dirty: bool) -> Result<BTreeSet<String>, String> {
    let mut command = Command::new("cargo");
    command.current_dir(root).args(["package", "--list"]);
    if allow_dirty {
        command.arg("--allow-dirty");
    }
    command.args(["--package", package]);
    let output = run_output(&mut command, &format!("cargo package --list {package}"))?;
    let stdout = String::from_utf8(output.stdout)
        .map_err(|error| format!("package list for {package} was not UTF-8: {error}"))?;
    Ok(stdout
        .lines()
        .map(|line| line.trim_start_matches("./").to_owned())
        .filter(|line| !line.is_empty())
        .collect())
}

/// Establishes that the workspace-packaged `runite` artifact is the one
/// `cargo publish` will upload, which is what lets the release workflow compare
/// crates.io's recorded checksum against it.
///
/// `runite` depends on `runite-proc-macros` at an exact version, so the
/// `Cargo.lock` embedded in the packaged `runite` records that dependency
/// against the registry, with a checksum. When the version is not on crates.io
/// yet — the situation of every release — cargo has only one candidate to take
/// that checksum from: the sibling `.crate` it just packaged. That sibling is
/// byte-for-byte the file `cargo publish --package runite-proc-macros` uploads,
/// so the checksum cargo writes now is the checksum the registry will report
/// afterwards, and the archive is identical either way.
///
/// That identity is the reason `release-verify` does not also package `runite`
/// on its own. It cannot: packaging `runite` standalone *resolves* the exact
/// proc-macro dependency from the registry, which fails with "failed to select
/// a version for the requirement" until that version is published. Since the
/// workspace artifact is the same bytes, there is nothing to gain from trying.
///
/// The identity is asserted rather than assumed, because everything above is a
/// statement about cargo's behaviour and a release that compares the wrong
/// checksum is detected only after the first upload.
fn verify_lock_records_packaged_macro(lock: &str, macro_artifact: &Path) -> Result<(), String> {
    let recorded = proc_macro_lock_checksum(lock).ok_or_else(|| {
        "the packaged runite Cargo.lock has no checksum for runite-proc-macros, so the \
         artifact cannot be the one `cargo publish` uploads"
            .to_owned()
    })?;
    let packaged = fs::read(macro_artifact)
        .map_err(|error| format!("failed to read {}: {error}", macro_artifact.display()))?;
    let digest = crate::sha256::hex_digest(&packaged);
    if recorded == digest {
        Ok(())
    } else {
        Err(format!(
            "the packaged runite Cargo.lock records runite-proc-macros checksum {recorded}, \
             but the packaged proc-macro artifact hashes to {digest}; the published `runite` \
             would not match this artifact"
        ))
    }
}

/// The `checksum` recorded for `runite-proc-macros` in a packaged lockfile.
fn proc_macro_lock_checksum(lock: &str) -> Option<String> {
    let mut in_target = false;
    for line in lock.lines() {
        if line == "[[package]]" {
            in_target = false;
        } else if line == "name = \"runite-proc-macros\"" {
            in_target = true;
        } else if in_target {
            if let Some(value) = line.strip_prefix("checksum = ") {
                return Some(value.trim_matches('"').to_owned());
            }
        }
    }
    None
}

fn package_workspace(root: &Path, target: &Path, allow_dirty: bool) -> Result<(), String> {
    let mut command = Command::new("cargo");
    command.current_dir(root).args(["package", "--no-verify"]);
    if allow_dirty {
        command.arg("--allow-dirty");
    }
    command
        .args(["--workspace", "--exclude", "xtask", "--target-dir"])
        .arg(target);
    run_status(&mut command, "package release workspace")
}

fn unpack_crate(root: &Path, artifact: &Path, destination: &Path) -> Result<(), String> {
    let mut command = Command::new("tar");
    command
        .current_dir(root)
        .arg("-xzf")
        .arg(artifact)
        .arg("-C")
        .arg(destination);
    run_status(
        &mut command,
        &format!(
            "unpack {}",
            artifact.file_name().unwrap_or_default().to_string_lossy()
        ),
    )
}

fn require_file(path: &Path) -> Result<(), String> {
    if path.is_file() {
        Ok(())
    } else {
        Err(format!("required file is missing: {}", path.display()))
    }
}

fn package_files(root: &Path) -> Result<BTreeSet<String>, String> {
    let mut files = BTreeSet::new();
    collect_files(root, root, &mut files)?;
    Ok(files)
}

fn collect_files(
    root: &Path,
    directory: &Path,
    files: &mut BTreeSet<String>,
) -> Result<(), String> {
    let entries = fs::read_dir(directory)
        .map_err(|error| format!("failed to read {}: {error}", directory.display()))?;
    for entry in entries {
        let entry = entry
            .map_err(|error| format!("failed to read entry in {}: {error}", directory.display()))?;
        let path = entry.path();
        if entry
            .file_type()
            .map_err(|error| format!("failed to inspect {}: {error}", path.display()))?
            .is_dir()
        {
            collect_files(root, &path, files)?;
        } else {
            let relative = path
                .strip_prefix(root)
                .map_err(|error| format!("failed to relativize {}: {error}", path.display()))?
                .to_string_lossy()
                .replace('\\', "/");
            files.insert(relative);
        }
    }
    Ok(())
}

fn verify_package_entries(
    label: &str,
    entries: &BTreeSet<String>,
    required: &[&str],
) -> Result<(), String> {
    for path in required {
        if !entries.contains(*path) {
            return Err(format!("{label} is missing required file `{path}`"));
        }
    }
    for path in entries {
        if FORBIDDEN_PACKAGE_PATHS
            .iter()
            .any(|forbidden| path == forbidden || path.starts_with(&format!("{forbidden}/")))
        {
            return Err(format!("{label} contains process-only path `{path}`"));
        }
    }
    Ok(())
}

fn verify_license_copy(root: &Path, package: &Path, license: &str) -> Result<(), String> {
    let canonical = fs::read(root.join(license))
        .map_err(|error| format!("failed to read root {license}: {error}"))?;
    let packaged = fs::read(package.join(license))
        .map_err(|error| format!("failed to read packaged {license}: {error}"))?;
    if canonical == packaged {
        Ok(())
    } else {
        Err(format!(
            "proc-macro {license} does not match the repository license text"
        ))
    }
}

fn patch_proc_macro_path(manifest: &str, dependency_path: &str) -> Result<String, String> {
    const SECTION: &str = "[dependencies.runite-proc-macros]";
    let mut lines: Vec<String> = manifest.lines().map(str::to_owned).collect();
    let section = lines
        .iter()
        .position(|line| line.trim() == SECTION)
        .ok_or_else(|| format!("packaged manifest has no {SECTION} section"))?;
    let end = lines[section + 1..]
        .iter()
        .position(|line| line.trim().starts_with('['))
        .map(|offset| section + 1 + offset)
        .unwrap_or(lines.len());
    let path_line = format!("path = \"{}\"", dependency_path.replace('\\', "\\\\"));
    if let Some(existing) =
        (section + 1..end).find(|index| lines[*index].trim().starts_with("path"))
    {
        lines[existing] = path_line;
    } else {
        lines.insert(section + 1, path_line);
    }
    let mut patched = lines.join("\n");
    patched.push('\n');
    Ok(patched)
}

fn make_manifest_standalone(manifest: &Path) -> Result<(), String> {
    let content = fs::read_to_string(manifest)
        .map_err(|error| format!("failed to read {}: {error}", manifest.display()))?;
    fs::write(manifest, standalone_manifest(&content))
        .map_err(|error| format!("failed to patch {}: {error}", manifest.display()))
}

fn standalone_manifest(manifest: &str) -> String {
    if manifest.lines().any(|line| line.trim() == "[workspace]") {
        return manifest.to_owned();
    }
    format!("{}\n[workspace]\n", manifest.trim_end())
}

fn patch_proc_macro_lock(lock: &str, version: &str) -> Result<String, String> {
    let mut in_target = false;
    let mut found = false;
    let mut output = Vec::new();
    for line in lock.lines() {
        if line == "[[package]]" {
            in_target = false;
        } else if line == "name = \"runite-proc-macros\"" {
            in_target = true;
            found = true;
        }
        if in_target && (line.starts_with("source = ") || line.starts_with("checksum = ")) {
            continue;
        }
        output.push(line);
    }
    if !found {
        return Err("packaged lockfile has no runite-proc-macros entry".to_owned());
    }
    let version_line = format!("version = \"{version}\"");
    let target_block = output
        .windows(2)
        .any(|lines| lines[0] == "name = \"runite-proc-macros\"" && lines[1] == version_line);
    if !target_block {
        return Err(format!(
            "packaged lockfile does not contain runite-proc-macros {version}"
        ));
    }
    let mut patched = output.join("\n");
    patched.push('\n');
    Ok(patched)
}

fn verify_unpacked_packages(
    root: &Path,
    release_dir: &Path,
    main_dir: &Path,
    macro_dir: &Path,
    msrv: bool,
) -> Result<(), String> {
    let target = release_dir.join("verify-target");
    cargo_manifest(
        root,
        macro_dir,
        &target,
        &["test", "--all-features", "--locked"],
        "test unpacked runite-proc-macros",
        false,
    )?;
    cargo_manifest(
        root,
        macro_dir,
        &target,
        &["doc", "--all-features", "--no-deps", "--locked"],
        "document unpacked runite-proc-macros",
        true,
    )?;
    cargo_manifest(
        root,
        main_dir,
        &target,
        &["build", "--all-targets", "--all-features", "--locked"],
        "build unpacked runite",
        false,
    )?;
    cargo_manifest(
        root,
        main_dir,
        &target,
        &["test", "--all-features", "--locked"],
        "test unpacked runite",
        false,
    )?;

    // Cross-target checks are `--lib`, not `--all-targets`, and that is a
    // deliberate narrowing rather than an oversight.
    //
    // Anything beyond the library — tests, benches, even examples — puts
    // dev-dependencies into the build graph, and the TLS tests depend on
    // `ring`, whose build script compiles C. Cross-compiling that to
    // `aarch64-apple-darwin` or `x86_64-pc-windows-msvc` from a Linux runner
    // needs a C toolchain for those platforms, which is not something CI or a
    // developer machine can reasonably be expected to have. It is not a matter
    // of installing one package.
    //
    // What is being verified here is the promise the published crate makes: the
    // *library* builds for every supported target, its docs build, and it
    // builds on the MSRV. A consumer depending on runite from crates.io never
    // builds our tests, so cross-compiling them proved nothing about what we
    // ship. Examples, tests and benches are still built and run for the host
    // above, and the per-platform CI jobs build `--all-targets` natively on
    // macOS and Windows, which is where that coverage actually belongs.
    for release_target in SUPPORTED_TARGETS {
        cargo_manifest(
            root,
            main_dir,
            &target,
            &[
                "check",
                "--target",
                release_target.triple,
                "--lib",
                "--locked",
            ],
            &format!("check unpacked runite default ({})", release_target.triple),
            false,
        )?;
        cargo_manifest(
            root,
            main_dir,
            &target,
            &[
                "check",
                "--target",
                release_target.triple,
                "--lib",
                "--all-features",
                "--locked",
            ],
            &format!(
                "check unpacked runite all-features ({})",
                release_target.triple
            ),
            false,
        )?;
        cargo_manifest(
            root,
            main_dir,
            &target,
            &[
                "doc",
                "--target",
                release_target.triple,
                "--all-features",
                "--no-deps",
                "--locked",
            ],
            &format!("document unpacked runite ({})", release_target.triple),
            true,
        )?;
        if msrv {
            cargo_manifest_with_toolchain(
                root,
                main_dir,
                &target,
                MSRV,
                &[
                    "check",
                    "--target",
                    release_target.triple,
                    "--all-features",
                    "--locked",
                ],
                &format!("MSRV-check unpacked runite ({})", release_target.triple),
            )?;
        }
    }
    Ok(())
}

fn cargo_manifest(
    root: &Path,
    package: &Path,
    target: &Path,
    args: &[&str],
    label: &str,
    deny_doc_warnings: bool,
) -> Result<(), String> {
    let mut command = Command::new("cargo");
    command
        .current_dir(root)
        .args(args)
        .arg("--manifest-path")
        .arg(package.join("Cargo.toml"))
        .arg("--target-dir")
        .arg(target);
    if deny_doc_warnings {
        command.env("RUSTDOCFLAGS", "-Dwarnings");
    }
    run_status(&mut command, label)
}

fn cargo_manifest_with_toolchain(
    root: &Path,
    package: &Path,
    target: &Path,
    toolchain: &str,
    args: &[&str],
    label: &str,
) -> Result<(), String> {
    let mut command = Command::new("cargo");
    command
        .current_dir(root)
        .arg(format!("+{toolchain}"))
        .args(args)
        .arg("--manifest-path")
        .arg(package.join("Cargo.toml"))
        .arg("--target-dir")
        .arg(target);
    run_status(&mut command, label)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_only_the_package_version() {
        let manifest = "[workspace]\n\n[package]\nname = \"x\"\nversion = \"0.2.0\"\n\n[dependencies]\nversion = \"9\"\n";
        assert_eq!(parse_package_version(manifest).as_deref(), Some("0.2.0"));
    }

    #[test]
    fn patches_normalized_dependency_manifest() {
        let manifest = "[package]\nname = \"runite\"\n\n[dependencies.runite-proc-macros]\nversion = \"=0.2.0\"\n\n[dependencies.tracing]\nversion = \"0.1\"\n";
        let patched = patch_proc_macro_path(manifest, "../runite-proc-macros-0.2.0").unwrap();
        assert!(patched.contains(
            "[dependencies.runite-proc-macros]\npath = \"../runite-proc-macros-0.2.0\"\nversion = \"=0.2.0\""
        ));
        assert_eq!(patched.matches("path =").count(), 1);
    }

    #[test]
    fn makes_an_unpacked_manifest_a_standalone_workspace() {
        let manifest = "[package]\nname = \"runite\"\n";
        let patched = standalone_manifest(manifest);
        assert!(patched.ends_with("\n[workspace]\n"));
        assert_eq!(standalone_manifest(&patched), patched);
    }

    #[test]
    fn patches_packaged_registry_lock_to_local_proc_macro() {
        let lock = "version = 4\n\n[[package]]\nname = \"runite-proc-macros\"\nversion = \"0.2.0\"\nsource = \"registry+https://example.invalid\"\nchecksum = \"abc\"\ndependencies = []\n";
        let patched = patch_proc_macro_lock(lock, "0.2.0").unwrap();
        assert!(!patched.contains("registry+"));
        assert!(!patched.contains("checksum"));
        assert!(patched.contains("dependencies = []"));
    }

    #[test]
    fn reads_the_proc_macro_checksum_from_the_packaged_lock() {
        let lock = "version = 4\n\n[[package]]\nname = \"runite\"\nversion = \"0.3.0\"\nchecksum = \"not-this-one\"\n\n[[package]]\nname = \"runite-proc-macros\"\nversion = \"0.3.0\"\nsource = \"registry+https://example.invalid\"\nchecksum = \"abc123\"\n";
        assert_eq!(
            proc_macro_lock_checksum(lock).as_deref(),
            Some("abc123"),
            "the checksum must come from the proc-macro's own block"
        );
        let without =
            "version = 4\n\n[[package]]\nname = \"runite-proc-macros\"\nversion = \"0.3.0\"\n";
        assert_eq!(proc_macro_lock_checksum(without), None);
    }

    /// A packaged lock with no proc-macro checksum, or one naming a different
    /// archive, means the artifact `release-verify` validated is not the
    /// artifact `cargo publish` would upload.
    #[test]
    fn a_lock_that_does_not_name_the_packaged_macro_is_rejected() {
        // Process-unique: a second checkout running this suite concurrently
        // must not be reading the file while this one rewrites it.
        let artifact = std::env::temp_dir().join(format!(
            "runite-xtask-lock-check-{}.crate",
            std::process::id()
        ));
        fs::write(&artifact, b"abc").expect("write probe artifact");
        let digest = crate::sha256::hex_digest(b"abc");

        let matching = format!(
            "version = 4\n\n[[package]]\nname = \"runite-proc-macros\"\nversion = \"0.3.0\"\nsource = \"registry+x\"\nchecksum = \"{digest}\"\n"
        );
        assert!(verify_lock_records_packaged_macro(&matching, &artifact).is_ok());

        let mismatched = "version = 4\n\n[[package]]\nname = \"runite-proc-macros\"\nversion = \"0.3.0\"\nsource = \"registry+x\"\nchecksum = \"0000\"\n";
        assert!(verify_lock_records_packaged_macro(mismatched, &artifact).is_err());

        let absent =
            "version = 4\n\n[[package]]\nname = \"runite-proc-macros\"\nversion = \"0.3.0\"\n";
        assert!(verify_lock_records_packaged_macro(absent, &artifact).is_err());

        fs::remove_file(&artifact).expect("remove probe artifact");
    }

    #[test]
    fn package_assertions_require_user_files_and_reject_process_files() {
        let good = ["Cargo.toml", "README.md", "src/lib.rs"]
            .into_iter()
            .map(str::to_owned)
            .collect();
        assert!(verify_package_entries("test", &good, &["Cargo.toml", "src/lib.rs"]).is_ok());

        let mut missing = good.clone();
        missing.remove("src/lib.rs");
        assert!(verify_package_entries("test", &missing, &["src/lib.rs"]).is_err());

        let mut process = good;
        process.insert(".github/workflows/release.yml".to_owned());
        assert!(verify_package_entries("test", &process, &["Cargo.toml"]).is_err());
    }
}
