#![deny(warnings)]

#[runite::test]
#[cfg(all())]
async fn invalid_test(_value: usize) {}

fn main() {}
