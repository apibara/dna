use proc_macro_error::abort;
use quote::quote;
use syn::spanned::Spanned;

#[proc_macro_derive(SinkOptions, attributes(sink_options))]
pub fn derive_sink_options(input: proc_macro::TokenStream) -> proc_macro::TokenStream {
    let ast = syn::parse(input).unwrap();
    impl_sink_options(&ast).into()
}

fn impl_sink_options(ast: &syn::DeriveInput) -> proc_macro2::TokenStream {
    let fields = collect_fields(ast);
    let name = &ast.ident;

    let tag_name = ast.attrs.iter().find_map(parse_sink_options);

    // TODO: We may want to keep serde-related attributes.
    let inner_fields = fields.iter().map(|field| {
        let name = &field.ident;
        let ty = &field.ty;
        quote!(#name: #ty)
    });

    let fields_initializers = fields.iter().map(|field| {
        let name = &field.ident;
        quote!(#name: options.#name)
    });

    let impl_ast = quote!(
        impl<'de> ::serde::Deserialize<'de> for #name {
            fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
            where
                D: ::serde::Deserializer<'de>,
            {
                use serde::Deserialize;

                #[derive(Deserialize)]
                #[serde(rename_all = "camelCase")]
                struct Inner {
                    #(#inner_fields),*
                }

                #[derive(Deserialize)]
                #[serde(rename_all = "camelCase")]
                struct Outer {
                    pub sink_type: String,
                    pub sink_options: Option<Inner>,
                }

                let outer = Outer::deserialize(deserializer)?;
                if outer.sink_type != #tag_name {
                    Err(serde::de::Error::custom(format!("invalid sink type: {} expected {}", outer.sink_type, #tag_name)))
                } else {
                    Ok(outer
                        .sink_options
                        .map(|options| #name { #(#fields_initializers),* })
                        .unwrap_or_default())
                }
            }
        }
    );

    impl_ast
}

fn collect_fields(ast: &syn::DeriveInput) -> Vec<syn::Field> {
    match ast.data {
        syn::Data::Struct(syn::DataStruct { ref fields, .. }) => {
            if fields.iter().any(|field| field.ident.is_none()) {
                abort!(
                    fields.span(),
                    "struct has unnamed fields";
                    help = "#[derive(SinkOptions)] can only be used with structs that have named fields";
                );
            }
            fields.iter().cloned().collect()
        }
        _ => abort!(
            ast.span(),
            "#[derive(SinkOptions)] can only be used with structs"
        ),
    }
}

fn parse_sink_options(attr: &syn::Attribute) -> Option<String> {
    if attr.path.is_ident("sink_options") {
        match attr.parse_meta() {
            Ok(syn::Meta::List(syn::MetaList { ref nested, .. })) => {
                nested.iter().find_map(|nested| match nested {
                    syn::NestedMeta::Meta(syn::Meta::NameValue(syn::MetaNameValue {
                        path,
                        lit: syn::Lit::Str(lit_str),
                        ..
                    })) if path.is_ident("tag") => Some(lit_str.value()),
                    _ => None,
                })
            }
            Ok(_) => abort!(attr.span(), "expected #[sink_options(tag = \"...\")]"),
            Err(err) => abort!(attr.span(), err),
        }
    } else {
        None
    }
}
