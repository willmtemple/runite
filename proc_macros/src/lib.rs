//! Procedural macros consumed through `runite`.
//!
//! This crate provides the implementation for [`#[runite::main]`](main), the
//! attribute macro re-exported by the `runite` crate. It is an implementation
//! detail and is not intended to be used directly; depend on `runite` and invoke
//! the macro as `#[runite::main]` instead.

#![deny(missing_docs)]

use proc_macro::TokenStream;
use proc_macro2::Span;
use quote::{format_ident, quote};
use syn::{Error, ItemFn, parse_macro_input};

/// Marks `fn main` as the runite entry point.
///
/// Works for both synchronous and `async` entry points: the macro inspects the
/// function signature and dispatches accordingly. A synchronous `fn main` is
/// queued as a task; an `async fn main` has its returned future queued onto the
/// main runtime thread. In both cases the generated real `main` then drives the
/// event loop by calling `runite::run`.
#[proc_macro_attribute]
pub fn main(attr: TokenStream, item: TokenStream) -> TokenStream {
    expand_entry(attr, item)
}

fn expand_entry(attr: TokenStream, item: TokenStream) -> TokenStream {
    if !proc_macro2::TokenStream::from(attr).is_empty() {
        return Error::new(
            Span::call_site(),
            "runite entry attributes take no arguments",
        )
        .to_compile_error()
        .into();
    }

    let function = parse_macro_input!(item as ItemFn);
    match validate_entry(&function) {
        Ok(()) => generate_entry(function).into(),
        Err(error) => error.to_compile_error().into(),
    }
}

fn validate_entry(function: &ItemFn) -> syn::Result<()> {
    let signature = &function.sig;

    if signature.ident != "main" {
        return Err(Error::new_spanned(
            &signature.ident,
            "runite entry attribute must be attached to a function named `main`",
        ));
    }

    if !signature.inputs.is_empty() {
        return Err(Error::new_spanned(
            &signature.inputs,
            "runite entry functions cannot take parameters",
        ));
    }

    if !signature.generics.params.is_empty() || signature.generics.where_clause.is_some() {
        return Err(Error::new_spanned(
            &signature.generics,
            "runite entry functions cannot be generic",
        ));
    }

    if signature.constness.is_some() {
        return Err(Error::new_spanned(
            signature.fn_token,
            "runite entry functions cannot be const",
        ));
    }

    if signature.unsafety.is_some() {
        return Err(Error::new_spanned(
            signature.fn_token,
            "runite entry functions cannot be unsafe",
        ));
    }

    if signature.abi.is_some() {
        return Err(Error::new_spanned(
            &signature.abi,
            "runite entry functions cannot declare an ABI",
        ));
    }

    if signature.variadic.is_some() {
        return Err(Error::new_spanned(
            &signature.variadic,
            "runite entry functions cannot be variadic",
        ));
    }

    Ok(())
}

fn generate_entry(mut function: ItemFn) -> proc_macro2::TokenStream {
    let is_async = function.sig.asyncness.is_some();
    let original_name = function.sig.ident.clone();
    let implementation_name = format_ident!("__runite_runtime_internal_{}", original_name);
    function.sig.ident = implementation_name.clone();

    let entry_call = if is_async {
        quote! {
            let _ = ::runite::queue_future(#implementation_name());
        }
    } else {
        quote! {
            ::runite::queue_task(|| {
                let _ = #implementation_name();
            });
        }
    };

    quote! {
        #function

        fn #original_name() {
            #entry_call
            ::runite::run();
        }
    }
}
