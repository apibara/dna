use std::pin::Pin;

use data_url::DataUrl;
use deno_ast::{MediaType, ParseParams, SourceTextInfo};
use deno_core::{
    futures::FutureExt, resolve_import, ModuleSource, ModuleSourceFuture, ModuleSpecifier,
    ModuleType,
};

pub struct WorkerModuleLoader {}

impl WorkerModuleLoader {
    pub fn new() -> Self {
        Self {}
    }
}

impl deno_core::ModuleLoader for WorkerModuleLoader {
    fn load(
        &self,
        module_specifier: &ModuleSpecifier,
        _maybe_referrer: Option<&ModuleSpecifier>,
        _is_dyn_import: bool,
    ) -> Pin<Box<ModuleSourceFuture>> {
        let module_specifier = module_specifier.clone();
        async move {
            let (module_specifier_found, code, media_type) =
                fetch_module_code(&module_specifier).await?;
            let module_type = get_module_type(media_type)?;

            let code = match media_type {
                MediaType::JavaScript
                | MediaType::Cjs
                | MediaType::Mjs
                | MediaType::Json
                | MediaType::Unknown => code,
                MediaType::TypeScript
                | MediaType::Mts
                | MediaType::Cts
                | MediaType::Tsx
                | MediaType::Jsx => {
                    let parsed = deno_ast::parse_module(ParseParams {
                        specifier: module_specifier.to_string(),
                        text_info: SourceTextInfo::from_string(code),
                        media_type,
                        capture_tokens: false,
                        scope_analysis: false,
                        maybe_syntax: None,
                    })?;
                    parsed.transpile(&Default::default())?.text
                }
                _ => return Err(deno_core::error::generic_error("Unsupported media type.")),
            };

            let module = ModuleSource::new_with_redirect(
                module_type,
                code.into(),
                &module_specifier,
                &module_specifier_found,
            );

            Ok(module)
        }
        .boxed_local()
    }

    fn resolve(
        &self,
        specifier: &str,
        referrer: &str,
        _kind: deno_core::ResolutionKind,
    ) -> Result<ModuleSpecifier, deno_core::error::AnyError> {
        resolve_import(specifier, referrer).map_err(|e| e.into())
    }
}

fn get_module_type(media_type: MediaType) -> Result<ModuleType, deno_core::error::AnyError> {
    match media_type {
        MediaType::JavaScript | MediaType::Mjs | MediaType::Cjs | MediaType::Jsx => {
            Ok(ModuleType::JavaScript)
        }
        MediaType::TypeScript
        | MediaType::Mts
        | MediaType::Cts
        | MediaType::Dts
        | MediaType::Dmts
        | MediaType::Dcts
        | MediaType::Tsx => Ok(ModuleType::JavaScript),
        MediaType::Json => Ok(ModuleType::Json),
        _ => Err(deno_core::error::generic_error("Unsupported media type.")),
    }
}

async fn fetch_module_code(
    specifier: &ModuleSpecifier,
) -> Result<(ModuleSpecifier, String, MediaType), deno_core::error::AnyError> {
    let mut module_url_found = specifier.clone();
    let (code, maybe_content_type) = match specifier.scheme() {
        "http" | "https" => {
            let res = reqwest::get(specifier.clone()).await?;
            // TODO: The HTML spec says to fail if the status is not
            // 200-299, but `error_for_status()` fails if the status is
            // 400-599. Redirect status codes are handled by reqwest,
            // but there are still status codes that are not handled.
            let res = res.error_for_status()?;
            let headers = res.headers();
            let content_type = headers
                .get("content-type")
                .map(|v| v.to_str().unwrap_or_default().to_string());
            // res.url() is the post-redirect URL.
            module_url_found = res.url().clone();
            let code = res.text().await?;
            (code, content_type)
        }
        "file" => {
            let path = match specifier.to_file_path() {
                Ok(path) => path,
                Err(_) => return Err(module_uri_error(specifier)),
            };
            let code = tokio::fs::read_to_string(path).await?;
            (code, None)
        }
        "data" => {
            let url =
                DataUrl::process(specifier.as_str()).map_err(|_| module_uri_error(specifier))?;
            match url.decode_to_vec() {
                Ok((bytes, _)) => {
                    let code = code_from_bytes(bytes)?;
                    (code, None)
                }
                Err(_) => return Err(module_uri_error(specifier)),
            }
        }
        _ => {
            return Err(deno_core::error::uri_error(format!(
                "Invalid module schema.\n  Specifier: {specifier}"
            )))
        }
    };

    let (media_type, _) = map_content_type(specifier, maybe_content_type);
    Ok((module_url_found, code, media_type))
}

fn code_from_bytes(bytes: Vec<u8>) -> Result<String, deno_core::error::AnyError> {
    let code = String::from_utf8(bytes)?;
    Ok(code)
}

pub fn map_content_type(
    specifier: &ModuleSpecifier,
    maybe_content_type: Option<String>,
) -> (MediaType, Option<String>) {
    if let Some(content_type) = maybe_content_type {
        let mut content_types = content_type.split(';');
        let content_type = content_types.next().unwrap();
        let media_type = MediaType::from_content_type(specifier, content_type);
        let charset = content_types
            .map(str::trim)
            .find_map(|s| s.strip_prefix("charset="))
            .map(String::from);

        (media_type, charset)
    } else {
        (MediaType::from_specifier(specifier), None)
    }
}

fn module_uri_error(specifier: &ModuleSpecifier) -> deno_core::error::AnyError {
    deno_core::error::uri_error(format!("Unable to read file.\n  Specifier: {specifier}"))
}
