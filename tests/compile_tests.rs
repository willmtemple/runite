#[test]
fn ui() {
    let tests = trybuild::TestCases::new();
    tests.pass("tests/ui/pass/*.rs");
    // `ring_entries` is a Linux setting, and the attribute rejects it at
    // compile time everywhere else.
    #[cfg(target_os = "linux")]
    tests.pass("tests/ui/pass-linux/*.rs");
    tests.compile_fail("tests/ui/fail/*.rs");
}
