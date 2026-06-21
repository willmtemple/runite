use proc_macro::TokenStream;
use proc_macro2::Span;
use quote::{format_ident, quote};
use syn::{Error, ItemFn, parse_macro_input};

#[proc_macro_attribute]
pub fn main(attr: TokenStream, item: TokenStream) -> TokenStream {
    expand_entry(attr, item, EntryKind::Sync)
}

#[proc_macro_attribute]
pub fn async_main(attr: TokenStream, item: TokenStream) -> TokenStream {
    expand_entry(attr, item, EntryKind::Async)
}

#[derive(Clone, Copy)]
enum EntryKind {
    Sync,
    Async,
}

fn expand_entry(attr: TokenStream, item: TokenStream, kind: EntryKind) -> TokenStream {
    if !proc_macro2::TokenStream::from(attr).is_empty() {
        return Error::new(
            Span::call_site(),
            "runite entry attributes take no arguments",
        )
        .to_compile_error()
        .into();
    }

    let function = parse_macro_input!(item as ItemFn);
    match validate_entry(&function, kind) {
        Ok(()) => generate_entry(function, kind).into(),
        Err(error) => error.to_compile_error().into(),
    }
}

fn validate_entry(function: &ItemFn, kind: EntryKind) -> syn::Result<()> {
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

    match kind {
        EntryKind::Sync if signature.asyncness.is_some() => Err(Error::new_spanned(
            signature.asyncness,
            "#[runite::main] expects a non-async `fn main`",
        )),
        EntryKind::Async if signature.asyncness.is_none() => Err(Error::new_spanned(
            signature.fn_token,
            "#[runite::async_main] expects an `async fn main`",
        )),
        _ => Ok(()),
    }
}

fn generate_entry(mut function: ItemFn, kind: EntryKind) -> proc_macro2::TokenStream {
    let original_name = function.sig.ident.clone();
    let implementation_name = format_ident!("__runite_runtime_internal_{}", original_name);
    function.sig.ident = implementation_name.clone();

    let entry_call = match kind {
        EntryKind::Sync => quote! {
            ::runite::queue_task(|| {
                let _ = #implementation_name();
            });
        },
        EntryKind::Async => quote! {
            let _ = ::runite::queue_future(#implementation_name());
        },
    };

    quote! {
        #function

        fn #original_name() {
            #entry_call
            ::runite::run();
        }
    }
}
