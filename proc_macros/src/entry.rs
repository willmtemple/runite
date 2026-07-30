use proc_macro::TokenStream;
use proc_macro2::{Span, TokenStream as TokenStream2};
use quote::{format_ident, quote};
use syn::parse::{ParseStream, Parser};
use syn::{
    Attribute, Error, Ident, ItemFn, LitInt, LitStr, Path, Token, Visibility, parse_macro_input,
    parse_quote,
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

/// Parsed attribute arguments.
///
/// `crate = "path"` names a renamed `runite` dependency. `ring_entries = N`
/// asks for a configured runtime instead of the default one; the value is kept
/// as a literal so its span, not the attribute's, carries any diagnostic.
struct EntryArgs {
    crate_path: Path,
    ring_entries: Option<LitInt>,
}

/// Every accepted key, in the order the error message lists them.
const ENTRY_KEYS: &str = "`crate = \"...\"` or `ring_entries = N`";

impl EntryArgs {
    fn parse(input: ParseStream, kind: EntryKind) -> syn::Result<Self> {
        let noun = kind.noun();
        let mut crate_path: Option<Path> = None;
        let mut ring_entries: Option<LitInt> = None;

        while !input.is_empty() {
            // `crate` is a keyword, so parse it as one rather than as an
            // identifier.
            if input.peek(Token![crate]) {
                let keyword = input.parse::<Token![crate]>()?;
                input.parse::<Token![=]>()?;
                let value: LitStr = input.parse()?;
                if crate_path.replace(value.parse()?).is_some() {
                    return Err(Error::new_spanned(
                        keyword,
                        format!("runite {noun} attribute sets `crate` twice"),
                    ));
                }
            } else {
                // Take the key as a whole token so the diagnostic points at the
                // key the caller wrote, not at wherever its value stopped
                // parsing.
                let key: Ident = input
                    .parse()
                    .map_err(|_| input.error(format!("expected {ENTRY_KEYS}")))?;
                if key != "ring_entries" {
                    return Err(Error::new_spanned(
                        &key,
                        format!("unknown runite {noun} attribute `{key}`; expected {ENTRY_KEYS}"),
                    ));
                }
                input.parse::<Token![=]>()?;
                let value: LitInt = input.parse()?;
                // Reject a non-`u32` here rather than letting it surface as a
                // type error inside the expansion.
                value.base10_parse::<u32>()?;
                if ring_entries.replace(value).is_some() {
                    return Err(Error::new_spanned(
                        &key,
                        format!("runite {noun} attribute sets `ring_entries` twice"),
                    ));
                }
            }

            if input.is_empty() {
                break;
            }
            input.parse::<Token![,]>()?;
        }

        Ok(Self {
            crate_path: crate_path.unwrap_or_else(|| parse_quote!(::runite)),
            ring_entries,
        })
    }
}

pub(crate) fn expand(attr: TokenStream, item: TokenStream, kind: EntryKind) -> TokenStream {
    let args = match (|input: ParseStream| EntryArgs::parse(input, kind)).parse(attr) {
        Ok(args) => args,
        Err(error) => return error.to_compile_error().into(),
    };
    let function = parse_macro_input!(item as ItemFn);
    match validate(&function, kind) {
        Ok(()) => generate(function, args, kind).into(),
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

/// Expands to a `let` binding for a runtime built from the attribute's
/// settings, or to nothing when the attribute had none.
///
/// The settings are all platform-specific, and a proc macro cannot see the
/// target, so the choice is made by `cfg` in the expansion. On a target that
/// does not have the knob, the fallback arm still builds a default runtime:
/// that keeps `compile_error!` the only diagnostic the caller sees, instead of
/// burying it under an unresolved-path error for `os::linux` and a
/// type error for the missing binding.
fn start_configured_runtime(
    args: &EntryArgs,
    crate_path: &Path,
    runtime: &Ident,
) -> Option<TokenStream2> {
    let entries = args.ring_entries.as_ref()?;
    let unsupported = |backend: &str| {
        format!(
            "`ring_entries` sizes the io_uring submission queue and exists only \
             on Linux; this target is {backend}. Remove it, or apply the \
             attribute under `#[cfg(target_os = \"linux\")]`."
        )
    };
    let macos = unsupported("macOS, whose backend is kqueue");
    let windows = unsupported("Windows, whose backend is IOCP");
    let other = unsupported("not Linux");

    Some(quote! {
        #[cfg(target_os = "macos")]
        ::core::compile_error!(#macos);
        #[cfg(target_os = "windows")]
        ::core::compile_error!(#windows);
        #[cfg(not(any(target_os = "linux", target_os = "macos", target_os = "windows")))]
        ::core::compile_error!(#other);

        #[cfg(target_os = "linux")]
        let #runtime = #crate_path::os::linux::BuilderExt::ring_entries(
            #crate_path::Builder::new(),
            #entries,
        );
        #[cfg(not(target_os = "linux"))]
        let #runtime = #crate_path::Builder::new();

        let #runtime = match #runtime.build() {
            ::core::result::Result::Ok(#runtime) => #runtime,
            // The implicit entry points panic on a startup failure; this one
            // reports the same failure with the configuration named, because a
            // rejected setting is the likely cause.
            ::core::result::Result::Err(error) => ::core::panic!(
                "runite: could not start the configured runtime: {error}"
            ),
        };
    })
}

fn generate(function: ItemFn, args: EntryArgs, kind: EntryKind) -> TokenStream2 {
    let crate_path = &args.crate_path;
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
    //
    // Without settings the free functions are used unchanged, so the bare
    // attribute keeps installing the thread's runtime lazily. With settings the
    // runtime has to exist before the body runs — `build` is refused once
    // anything else has started one — so it is built up front and the same two
    // shapes are driven through the token it returns.
    let runtime = format_ident!("__runite_runtime", span = Span::mixed_site());
    let start = start_configured_runtime(&args, crate_path, &runtime);
    let drive = match (&start, is_async) {
        (None, true) => quote! { #crate_path::block_on(#implementation_name()) },
        (None, false) => quote! {
            let __runite_output = #implementation_name();
            #crate_path::run();
            __runite_output
        },
        (Some(start), true) => quote! {
            #start
            #runtime.block_on(#implementation_name())
        },
        (Some(start), false) => quote! {
            #start
            let __runite_output = #implementation_name();
            #runtime.run();
            __runite_output
        },
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
