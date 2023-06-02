use std::pin::Pin;

use deno_ast::{MediaType, ParseParams, SourceTextInfo};
use deno_core::{futures::FutureExt, ModuleSourceFuture, ModuleSpecifier, ModuleType};

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
            let path = module_specifier.to_file_path().map_err(|_| {
                deno_core::error::uri_error(format!(
                    "Invalid file path.\n  Specifier: {module_specifier}"
                ))
            })?;
            let media_type = MediaType::from_path(&path);
            let module_type = get_module_type(media_type)?;
            let code = std::fs::read_to_string(&path).map_err(|_| {
                deno_core::error::uri_error(format!(
                    "Unable to read file.\n  Specifier: {module_specifier}"
                ))
            })?;

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

            let module = deno_core::ModuleSource::new(module_type, code.into(), &module_specifier);

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
        deno_core::resolve_import(specifier, referrer).map_err(|e| e.into())
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
