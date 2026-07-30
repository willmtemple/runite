//! Procedural macros consumed through `runite`.
//!
//! This crate provides the implementations for [`#[runite::main]`](main),
//! [`#[runite::test]`](test), and [`runite::select!`](select). It is an
//! implementation detail and is not intended to be used directly; depend on
//! `runite` and invoke the macros through that crate instead.

#![deny(missing_docs)]

use proc_macro::TokenStream;
use proc_macro2::{Span, TokenStream as TokenStream2};
use quote::{format_ident, quote};
use syn::parse::{Parse, ParseStream};
use syn::spanned::Spanned;
use syn::{Error, Expr, Pat, Token, parse_macro_input, parse_quote, parse_quote_spanned};

mod entry;

use entry::{EntryKind, expand};

mod keyword {
    syn::custom_keyword!(biased);
}

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
/// # Configuring the runtime
///
/// The bare attribute starts the thread's runtime lazily and with the
/// defaults. In an `async` body that still means the runtime exists before the
/// body does — `runite::block_on` installs it — so a
/// `runite::Builder::build()` there fails with
/// [`AlreadyExists`](std::io::ErrorKind::AlreadyExists). A bare *synchronous*
/// body is the exception: it runs before the trailing `runite::run()`, so a
/// `build()` in it succeeds and that `run()` drives what it built. Pass the
/// settings to the attribute instead, and it builds the runtime it names
/// before the body runs, in either shape:
///
/// ```ignore
/// #[runite::main(ring_entries = 32)]
/// async fn main() { /* ... */ }
/// ```
///
/// `ring_entries` is the io_uring submission-queue size; see
/// `runite::os::linux::BuilderExt::ring_entries` for the accepted values and
/// what they cost. It is **Linux-only**: on macOS or Windows the attribute is
/// a compile error naming the platform, for the same reason the setter lives
/// on a Linux extension trait rather than on the portable `Builder`.
///
/// A rejected setting is a startup panic here, since an entry point has
/// nowhere to return an error to. Use `runite::Builder` directly to handle it.
///
/// To use a renamed `runite` dependency, pass the path:
/// `#[runite::main(crate = "my_runite")]`. Arguments may be combined in either
/// order: `#[runite::main(crate = "my_runite", ring_entries = 32)]`.
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
/// placed below `#[runite::test]` are forwarded to the generated test harness
/// wrapper. Lint, conditional-compilation, and documentation attributes are
/// likewise attached to the wrapper; lint scope then includes the nested async
/// implementation.
///
/// # Configuring the runtime
///
/// Accepts the same settings as [`#[runite::main]`](macro@main), so a
/// configured runtime can be tested rather than only shipped:
///
/// ```ignore
/// #[runite::test(ring_entries = 8)]
/// async fn a_small_ring_still_reads_files() { /* ... */ }
/// ```
///
/// This relies on libtest giving each test its own thread, which it does
/// unless the suite is run with `--test-threads=1`. Under that flag a
/// configured test that is not the first to touch the runtime panics with
/// [`AlreadyExists`](std::io::ErrorKind::AlreadyExists) rather than quietly
/// running on someone else's runtime.
///
/// To use a renamed `runite` dependency, pass the path:
/// `#[runite::test(crate = "my_runite")]`. Arguments may be combined in either
/// order: `#[runite::test(crate = "my_runite", ring_entries = 8)]`.
#[proc_macro_attribute]
pub fn test(attr: TokenStream, item: TokenStream) -> TokenStream {
    expand(attr, item, EntryKind::Test)
}

/// Waits for the first enabled future to complete.
///
/// This is the procedural implementation behind `runite::select!`. Invoke
/// the macro through `runite`, rather than depending on this crate directly.
#[proc_macro]
pub fn select(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as SelectInput);
    match generate_select(input) {
        Ok(expansion) => expansion.into(),
        Err(error) => error.to_compile_error().into(),
    }
}

struct SelectInput {
    biased: bool,
    branches: Vec<SelectBranch>,
    fallback: Option<Expr>,
}

struct SelectBranch {
    pattern: Pat,
    future: Expr,
    condition: Option<Expr>,
    handler: Expr,
}

impl Parse for SelectInput {
    fn parse(input: ParseStream<'_>) -> syn::Result<Self> {
        let biased = if input.peek(keyword::biased) {
            input.parse::<keyword::biased>()?;
            input.parse::<Token![;]>()?;
            true
        } else {
            false
        };

        let mut branches = Vec::new();
        let mut fallback = None;
        while !input.is_empty() {
            if input.peek(Token![else]) {
                input.parse::<Token![else]>()?;
                input.parse::<Token![=>]>()?;
                fallback = Some(input.parse()?);
                if input.peek(Token![,]) {
                    input.parse::<Token![,]>()?;
                }
                if !input.is_empty() {
                    return Err(input.error("`else` must be the final select! branch"));
                }
                break;
            }

            let pattern = input.call(Pat::parse_multi_with_leading_vert)?;
            input.parse::<Token![=]>()?;
            let future = input.parse()?;
            let condition = if input.peek(Token![,]) && input.peek2(Token![if]) {
                input.parse::<Token![,]>()?;
                input.parse::<Token![if]>()?;
                Some(input.parse()?)
            } else {
                None
            };
            input.parse::<Token![=>]>()?;
            let handler = input.parse()?;
            branches.push(SelectBranch {
                pattern,
                future,
                condition,
                handler,
            });

            if input.is_empty() {
                break;
            }
            input.parse::<Token![,]>()?;
        }

        if branches.is_empty() && fallback.is_none() {
            return Err(Error::new(
                Span::call_site(),
                "runite::select! requires at least one branch or an `else =>` handler",
            ));
        }

        Ok(Self {
            biased,
            branches,
            fallback,
        })
    }
}

fn generate_select(input: SelectInput) -> syn::Result<TokenStream2> {
    let SelectInput {
        biased,
        branches,
        fallback,
    } = input;
    if branches.is_empty() {
        let fallback = fallback.expect("select parser requires a fallback when there are no arms");
        return Ok(quote!({ #fallback }));
    }

    let span = Span::mixed_site();
    let winner = format_ident!("__RuniteSelectWinner", span = span);
    let disabled = format_ident!("Disabled", span = span);
    let context = format_ident!("__runite_select_context", span = span);
    let start = format_ident!("__runite_select_start", span = span);
    let next = format_ident!("__runite_select_next", span = span);
    let start_counter = format_ident!("__RUNITE_SELECT_START", span = span);
    let selected = format_ident!("__runite_select_winner", span = span);
    let enabled_item = format_ident!("__runite_select_enabled", span = span);
    let count = branches.len();

    let variants: Vec<_> = (0..count)
        .map(|index| format_ident!("Arm{index}", span = span))
        .collect();
    let type_parameters: Vec<_> = (0..count)
        .map(|index| format_ident!("T{index}", span = span))
        .collect();
    let guards: Vec<_> = (0..count)
        .map(|index| format_ident!("__runite_select_guard_{index}", span = span))
        .collect();
    let futures: Vec<_> = (0..count)
        .map(|index| format_ident!("__runite_select_future_{index}", span = span))
        .collect();
    let enabled: Vec<_> = (0..count)
        .map(|index| format_ident!("__runite_select_enabled_{index}", span = span))
        .collect();
    let outputs: Vec<_> = (0..count)
        .map(|index| format_ident!("__runite_select_output_{index}", span = span))
        .collect();
    let matched: Vec<_> = (0..count)
        .map(|index| format_ident!("__runite_select_matched_{index}", span = span))
        .collect();
    let resolved: Vec<_> = (0..count)
        .map(|index| format_ident!("__runite_select_resolved_{index}", span = span))
        .collect();

    let guard_expressions = branches.iter().map(|branch| {
        branch
            .condition
            .clone()
            .unwrap_or_else(|| parse_quote!(true))
    });
    let future_expressions = branches.iter().map(|branch| &branch.future);
    // A slice pattern activates match ergonomics without moving the output,
    // allowing Rust itself to resolve an ambiguous bare identifier as either a
    // binding, constant, or unit variant. Explicit reference patterns are
    // checked separately by the binding-free structural shape.
    let resolution_shapes = branches
        .iter()
        .map(|branch| {
            let mut shape = branch.pattern.clone();
            prepare_pattern(&mut shape, PatternPurpose::Resolution)?;
            Ok(shape)
        })
        .collect::<syn::Result<Vec<_>>>()?;
    let structural_shapes = branches
        .iter()
        .map(|branch| {
            let mut shape = branch.pattern.clone();
            prepare_pattern(&mut shape, PatternPurpose::Structural)?;
            Ok(shape)
        })
        .collect::<syn::Result<Vec<_>>>()?;

    let poll_at_or_after = branches.iter().enumerate().map(|(index, _)| {
        poll_select_branch(
            quote!(#index >= #start),
            &winner,
            &variants[index],
            &futures[index],
            &enabled[index],
            &outputs[index],
            &matched[index],
            &resolved[index],
            &resolution_shapes[index],
            &structural_shapes[index],
            &context,
        )
    });
    let poll_before = branches.iter().enumerate().map(|(index, _)| {
        poll_select_branch(
            quote!(#index < #start),
            &winner,
            &variants[index],
            &futures[index],
            &enabled[index],
            &outputs[index],
            &matched[index],
            &resolved[index],
            &resolution_shapes[index],
            &structural_shapes[index],
            &context,
        )
    });

    let fixed_start = biased || count == 1;
    let initial_start = if fixed_start {
        quote! {}
    } else {
        quote! {
            static #start_counter: ::core::sync::atomic::AtomicUsize =
                ::core::sync::atomic::AtomicUsize::new(0);
            let mut #next = #start_counter
                .fetch_add(1, ::core::sync::atomic::Ordering::Relaxed)
                % #count;
        }
    };
    let select_start = if fixed_start {
        quote! {
            let #start = 0usize;
        }
    } else {
        quote! {
            let #start = #next;
            #next = (#next + 1) % #count;
        }
    };

    let handlers = branches.iter().enumerate().map(|(index, branch)| {
        let variant = &variants[index];
        let output = &outputs[index];
        let pattern = &branch.pattern;
        let handler = &branch.handler;
        quote! {
            #winner::#variant(#output) => {
                #[allow(unused_mut)]
                let mut #output = #output;
                #[allow(unreachable_patterns)]
                match #output {
                    #pattern => #handler,
                    _ => ::core::unreachable!(
                        "select! winner no longer matched its checked branch pattern"
                    ),
                }
            }
        }
    });
    let fallback_handler = match fallback {
        Some(handler) => quote!(#handler),
        None => quote!(::core::panic!("runite::select!: all branches are disabled")),
    };

    Ok(quote! {
        {
            #[allow(clippy::large_enum_variant)]
            enum #winner<#(#type_parameters),*> {
                #(#variants(#type_parameters),)*
                #disabled,
            }

            #(let #guards = #guard_expressions;)*

            let #selected = {
                #(let mut #futures = ::core::pin::pin!(#future_expressions);)*
                #(let mut #enabled = #guards;)*
                #initial_start

                ::core::future::poll_fn(move |#context| {
                    #select_start
                    #(#poll_at_or_after)*
                    #(#poll_before)*

                    if [#(#enabled),*]
                        .iter()
                        .all(|#enabled_item| !*#enabled_item)
                    {
                        ::core::task::Poll::Ready(#winner::#disabled)
                    } else {
                        ::core::task::Poll::Pending
                    }
                })
                .await
            };

            match #selected {
                #(#handlers,)*
                #winner::#disabled => #fallback_handler,
            }
        }
    })
}

#[allow(clippy::too_many_arguments)]
fn poll_select_branch(
    in_range: TokenStream2,
    winner: &syn::Ident,
    variant: &syn::Ident,
    future: &syn::Ident,
    enabled: &syn::Ident,
    output: &syn::Ident,
    matched: &syn::Ident,
    resolved: &syn::Ident,
    resolution_shape: &Pat,
    structural_shape: &Pat,
    context: &syn::Ident,
) -> TokenStream2 {
    quote! {
        if #in_range && #enabled {
            match ::core::future::Future::poll(#future.as_mut(), #context) {
                ::core::task::Poll::Ready(#output) => {
                    let #resolved = {
                        #[allow(unreachable_patterns, unused_variables)]
                        match ::core::slice::from_ref(&#output) {
                            [#resolution_shape] => true,
                            _ => false,
                        }
                    };
                    let #matched = #resolved && {
                        // The structural shape keeps bare identifiers that sit
                        // under a `&`, so that constants and unit variants there
                        // are actually compared. A binding in that position is
                        // unused by this test-only match.
                        #[allow(unreachable_patterns, unused_variables)]
                        match #output {
                            #structural_shape => true,
                            _ => false,
                        }
                    };
                    if #matched {
                        return ::core::task::Poll::Ready(#winner::#variant(#output));
                    }
                    #enabled = false;
                }
                ::core::task::Poll::Pending => {}
            }
        }
    }
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum PatternPurpose {
    Resolution,
    Structural,
}

fn prepare_pattern(pattern: &mut Pat, purpose: PatternPurpose) -> syn::Result<()> {
    prepare_pattern_inner(pattern, purpose, false)
}

/// `within_reference` tracks whether `pattern` sits underneath an explicit `&`.
///
/// The resolution shape cannot represent such a pattern at all (its slice
/// scrutinee borrows implicitly, and edition 2024 rejects an explicit
/// dereference there), so references are erased for that purpose. The constant
/// test therefore has to survive into the structural shape: a bare identifier
/// under a `&` is kept rather than blanked, so `&None` and `&EXPECTED` are
/// actually compared. That is sound because a bare identifier under a reference
/// only compiles when it names a constant or unit variant, or binds a `Copy`
/// value — never a move out of the borrow.
fn prepare_pattern_inner(
    pattern: &mut Pat,
    purpose: PatternPurpose,
    within_reference: bool,
) -> syn::Result<()> {
    if let Pat::Ident(binding) = pattern {
        let is_bare =
            binding.by_ref.is_none() && binding.mutability.is_none() && binding.subpat.is_none();
        if is_bare && (purpose == PatternPurpose::Resolution || within_reference) {
            return Ok(());
        }

        let replacement = if let Some((_, subpattern)) = &binding.subpat {
            let mut replacement = (**subpattern).clone();
            prepare_pattern_inner(&mut replacement, purpose, within_reference)?;
            replacement
        } else {
            parse_quote_spanned!(binding.span()=> _)
        };
        *pattern = replacement;
        return Ok(());
    }

    // The resolution shape cannot carry an explicit dereference (see above), so
    // the whole reference pattern is erased there and the structural shape does
    // the checking.
    if purpose == PatternPurpose::Resolution && matches!(pattern, Pat::Reference(_)) {
        *pattern = parse_quote_spanned!(pattern.span()=> _);
        return Ok(());
    }

    match pattern {
        Pat::Macro(_) | Pat::Verbatim(_) => Err(Error::new(
            pattern.span(),
            "pattern macros are not supported in runite::select!",
        )),
        Pat::Or(pattern) => {
            for case in &mut pattern.cases {
                prepare_pattern_inner(case, purpose, within_reference)?;
            }
            Ok(())
        }
        Pat::Paren(pattern) => prepare_pattern_inner(&mut pattern.pat, purpose, within_reference),
        Pat::Reference(pattern) => prepare_pattern_inner(&mut pattern.pat, purpose, true),
        Pat::Slice(pattern) => {
            for element in &mut pattern.elems {
                prepare_pattern_inner(element, purpose, within_reference)?;
            }
            Ok(())
        }
        Pat::Struct(pattern) => {
            for field in &mut pattern.fields {
                prepare_pattern_inner(&mut field.pat, purpose, within_reference)?;
                field.colon_token.get_or_insert_with(Default::default);
            }
            Ok(())
        }
        Pat::Tuple(pattern) => {
            for element in &mut pattern.elems {
                prepare_pattern_inner(element, purpose, within_reference)?;
            }
            Ok(())
        }
        Pat::TupleStruct(pattern) => {
            for element in &mut pattern.elems {
                prepare_pattern_inner(element, purpose, within_reference)?;
            }
            Ok(())
        }
        Pat::Type(pattern) => prepare_pattern_inner(&mut pattern.pat, purpose, within_reference),
        _ => Ok(()),
    }
}
