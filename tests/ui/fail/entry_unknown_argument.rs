#![deny(warnings)]

// An unrecognized key must name itself, rather than surfacing as a parse
// failure at whatever token followed it. The attribute arguments are rejected
// before the function itself is inspected, so `#[runite::main]` reports the
// same way here without the function having to be named `main`.
#[runite::test(ring_entires = 8)]
async fn misspelled_key() {}

#[runite::main(threads = 4)]
async fn unknown_key() {}

fn main() {}
