use proc_macro::TokenStream;
use proc_macro2::{Span, TokenStream as TokenStream2};
use quote::{format_ident, quote};
use syn::parse::{Parse, ParseStream};
use syn::{
    Attribute, Error, ItemFn, LitStr, Path, Token, Visibility, parse_macro_input, parse_quote,
};

#[derive(Clone, Copy, PartialEq, Eq)]
pub(crate) enum EntryKind {
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

pub(crate) fn expand(attr: TokenStream, item: TokenStream, kind: EntryKind) -> TokenStream {
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
    let visibility = function.vis.clone();
    let implementation_name = format_ident!("__runite_implementation", span = Span::mixed_site());

    let mut implementation = function;
    implementation.sig.ident = implementation_name.clone();
    implementation.vis = Visibility::Inherited;

    let (implementation_attrs, wrapper_attrs) =
        partition_attributes(std::mem::take(&mut implementation.attrs), kind);
    implementation.attrs = implementation_attrs;

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
        #(#wrapper_attrs)*
        #test_attr
        #visibility fn #original_name() #output {
            #implementation
            #drive
        }
    }
}

fn partition_attributes(
    attributes: Vec<Attribute>,
    kind: EntryKind,
) -> (Vec<Attribute>, Vec<Attribute>) {
    let mut implementation = Vec::new();
    let mut wrapper = Vec::new();

    for attribute in attributes {
        let path = attribute.path();
        let is_harness =
            kind == EntryKind::Test && (path.is_ident("ignore") || path.is_ident("should_panic"));
        let is_wrapper_attribute = is_harness
            || path.is_ident("doc")
            || path.is_ident("cfg")
            || path.is_ident("cfg_attr")
            || path.is_ident("allow")
            || path.is_ident("warn")
            || path.is_ident("deny")
            || path.is_ident("forbid")
            || path.is_ident("expect");

        if is_wrapper_attribute {
            wrapper.push(attribute);
        } else {
            implementation.push(attribute);
        }
    }

    (implementation, wrapper)
}
