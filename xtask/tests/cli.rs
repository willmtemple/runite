use std::process::Command;

#[test]
fn help_lists_release_and_api_commands() {
    let output = Command::new(env!("CARGO_BIN_EXE_xtask"))
        .arg("--help")
        .output()
        .expect("xtask should run");
    assert!(output.status.success());
    let stderr = String::from_utf8(output.stderr).expect("help should be UTF-8");
    assert!(stderr.contains("api-report"));
    assert!(stderr.contains("release-verify"));
}

#[test]
fn unknown_commands_fail() {
    let output = Command::new(env!("CARGO_BIN_EXE_xtask"))
        .arg("not-a-command")
        .output()
        .expect("xtask should run");
    assert!(!output.status.success());
    let stderr = String::from_utf8(output.stderr).expect("error should be UTF-8");
    assert!(stderr.contains("unknown subcommand"));
}
