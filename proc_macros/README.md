# runite-proc-macros

Procedural macros that back the [`runite`](https://crates.io/crates/runite) async
runtime.

> **This crate is an implementation detail of `runite` and is not intended for
> direct consumption.** Do not depend on it directly. Add `runite` instead and
> use the macros through it — for example `#[runite::main]`. The macros expand to
> code that references `runite`'s internals, so they only work when `runite` is a
> dependency, and the API offered here may change without a major version bump of
> this crate.

## Usage

Add `runite` (not this crate) to your `Cargo.toml`:

```toml
[dependencies]
runite = "0.1"
```

Then annotate your entry point. `#[runite::main]` works for both synchronous and
`async fn main`, inspecting the signature and driving the runtime accordingly:

```rust
#[runite::main]
async fn main() {
    // your async application
}
```

See the [`runite` crate documentation](https://docs.rs/runite) for the full
guide.

## License

Licensed under either of MIT or Apache-2.0 at your option. See the
[runite repository](https://github.com/willmtemple/runite) for license texts.
