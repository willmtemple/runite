//! Procedural macros consumed through `runite`.
//!
//! This crate provides the implementations for [`#[runite::main]`](main) and
//! [`#[runite::test]`](test), the attribute macros re-exported by the `runite`
//! crate. It is an implementation detail and is not intended to be used
//! directly; depend on `runite` and invoke the macros as `#[runite::main]` /
//! `#[runite::test]` instead.

#![deny(missing_docs)]

use proc_macro::TokenStream;
use proc_macro2::TokenStream as TokenStream2;
use quote::{format_ident, quote};
use syn::parse::{Parse, ParseStream};
use syn::{Error, ItemFn, LitStr, Path, Token, parse_macro_input, parse_quote};

/// Marks `fn main` as the runite entry point.
///
/// Works for both synchronous and `async` entry points. An `async fn main` has
/// its future driven to completion with `runite::block_on`, so the program
/// ends when `main`'s future resolves (like `std`'s `main`, any still-running
/// background tasks are abandoned) and the function's return value is honored:
/// an `async fn main() -> Result<…>` that returns `Err` reports a non-zero exit
/// status through [`std::process::Termination`], instead of silently exiting 0.
/// A synchronous `fn main` runs its body, drives the event loop to drain any
/// tasks it spawned via `runite::run`, then returns its value.
///
/// To use a renamed `runite` dependency, pass the path:
/// `#[runite::main(crate = "my_runite")]`.
#[proc_macro_attribute]
pub fn main(attr: TokenStream, item: TokenStream) -> TokenStream {
    expand(attr, item, EntryKind::Main)
}

/// Marks an `async fn` as a runite-driven test.
///
/// Generates a `#[test]` wrapper that drives the test's future to completion
/// with `runite::block_on`. The test function may return anything that
/// implements [`std::process::Termination`] (for example `Result<(), E>` so the
/// body can use `?`). Test attributes such as `#[ignore]` and `#[should_panic]`
/// placed below `#[runite::test]` are forwarded to the generated test.
///
/// To use a renamed `runite` dependency, pass the path:
/// `#[runite::test(crate = "my_runite")]`.
#[proc_macro_attribute]
pub fn test(attr: TokenStream, item: TokenStream) -> TokenStream {
    expand(attr, item, EntryKind::Test)
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum EntryKind {
    Main,
    Test,
}

impl EntryKind {
    fn noun(self) -> &'static str {
        match self {
            EntryKind::Main => "entry",
            EntryKind::Test => "test",
        }
    }
}

/// Parsed attribute arguments: an optional `crate = "path"` override for the
/// `runite` crate path (to support renamed dependencies).
struct EntryArgs {
    crate_path: Path,
}

impl Parse for EntryArgs {
    fn parse(input: ParseStream) -> syn::Result<Self> {
        let mut crate_path: Path = parse_quote!(::runite);
        if input.is_empty() {
            return Ok(Self { crate_path });
        }

        // `crate` is a keyword, so parse it as one rather than as an identifier.
        if !input.peek(Token![crate]) {
            return Err(input.error("runite entry attributes accept only `crate = \"...\"`"));
        }
        input.parse::<Token![crate]>()?;
        input.parse::<Token![=]>()?;
        let value: LitStr = input.parse()?;
        crate_path = value.parse()?;

        if !input.is_empty() {
            return Err(input.error("unexpected trailing tokens after `crate = \"...\"`"));
        }
        Ok(Self { crate_path })
    }
}

fn expand(attr: TokenStream, item: TokenStream, kind: EntryKind) -> TokenStream {
    let args = parse_macro_input!(attr as EntryArgs);
    let function = parse_macro_input!(item as ItemFn);
    match validate(&function, kind) {
        Ok(()) => generate(function, args.crate_path, kind).into(),
        Err(error) => error.to_compile_error().into(),
    }
}

fn validate(function: &ItemFn, kind: EntryKind) -> syn::Result<()> {
    let signature = &function.sig;

    if kind == EntryKind::Main && signature.ident != "main" {
        return Err(Error::new_spanned(
            &signature.ident,
            "runite entry attribute must be attached to a function named `main`",
        ));
    }

    let noun = kind.noun();
    if !signature.inputs.is_empty() {
        return Err(Error::new_spanned(
            &signature.inputs,
            format!("runite {noun} functions cannot take parameters"),
        ));
    }
    if !signature.generics.params.is_empty() || signature.generics.where_clause.is_some() {
        return Err(Error::new_spanned(
            &signature.generics,
            format!("runite {noun} functions cannot be generic"),
        ));
    }
    if signature.constness.is_some() {
        return Err(Error::new_spanned(
            signature.fn_token,
            format!("runite {noun} functions cannot be const"),
        ));
    }
    if signature.unsafety.is_some() {
        return Err(Error::new_spanned(
            signature.fn_token,
            format!("runite {noun} functions cannot be unsafe"),
        ));
    }
    if signature.abi.is_some() {
        return Err(Error::new_spanned(
            &signature.abi,
            format!("runite {noun} functions cannot declare an ABI"),
        ));
    }
    if signature.variadic.is_some() {
        return Err(Error::new_spanned(
            &signature.variadic,
            format!("runite {noun} functions cannot be variadic"),
        ));
    }

    Ok(())
}

fn generate(function: ItemFn, crate_path: Path, kind: EntryKind) -> TokenStream2 {
    let is_async = function.sig.asyncness.is_some();
    let original_name = function.sig.ident.clone();
    let output = function.sig.output.clone();
    let implementation_name = format_ident!("__runite_runtime_internal_{}", original_name);

    let mut implementation = function;
    implementation.sig.ident = implementation_name.clone();

    // For tests, hoist the user's attributes (e.g. `#[ignore]`,
    // `#[should_panic]`) onto the generated `#[test]` wrapper where they belong,
    // rather than leaving them on the inner implementation function.
    let wrapper_attrs = match kind {
        EntryKind::Test => std::mem::take(&mut implementation.attrs),
        EntryKind::Main => Vec::new(),
    };

    // `async` bodies are driven to completion and their value returned; sync
    // bodies run inline, then the loop is drained so spawned tasks execute, then
    // the value is returned. Both preserve the original return type so that
    // `Termination` (e.g. `Result`) governs the process exit / test outcome.
    let drive = if is_async {
        quote! { #crate_path::block_on(#implementation_name()) }
    } else {
        quote! {
            let __runite_output = #implementation_name();
            #crate_path::run();
            __runite_output
        }
    };

    let test_attr = match kind {
        // Use the fully-qualified built-in `test` attribute so the expansion is
        // robust even if `test` is shadowed at the call site.
        EntryKind::Test => quote! { #[::core::prelude::v1::test] },
        EntryKind::Main => quote! {},
    };

    quote! {
        #implementation

        #(#wrapper_attrs)*
        #test_attr
        fn #original_name() #output {
            #drive
        }
    }
}
