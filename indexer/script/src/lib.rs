use std::{fmt, path::Path, time::Duration};

use apibara_indexer_runtime::DenoRuntime;
use deno_core::{FastString, ModuleSpecifier};
use error_stack::{Result, ResultExt};
use serde_json::Value;

pub use apibara_indexer_runtime::DenoRuntimeOptions;

pub struct ScriptOptions {
    pub runtime_options: DenoRuntimeOptions,
    pub transform_timeout: Option<Duration>,
    pub load_timeout: Option<Duration>,
}

pub struct Script {
    runtime: DenoRuntime,
    module: ModuleSpecifier,
    transform_timeout: Duration,
    load_timeout: Duration,
}

#[derive(Debug, serde::Deserialize)]
pub struct FactoryResult<F> {
    pub filter: Option<F>,
    pub data: Option<Value>,
}

#[derive(Debug)]
pub struct ScriptError;
impl error_stack::Context for ScriptError {}

impl fmt::Display for ScriptError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("indexer script operation failed")
    }
}

impl Script {
    /// Creates a [Script] from the given file.
    ///
    /// A relative file path is considered relative to the given current directory.
    pub fn from_file(
        path: &str,
        current_dir: impl AsRef<Path>,
        options: ScriptOptions,
    ) -> Result<Self, ScriptError> {
        let module = deno_core::resolve_path(path, current_dir.as_ref())
            .change_context(ScriptError)
            .attach_printable("failed to resolve Deno path")
            .attach_printable_lazy(|| format!("path: {path}"))?;
        Self::from_module(module, options)
    }

    /// Creates a [Script] from the given module specifier.
    pub fn from_module(
        module: ModuleSpecifier,
        options: ScriptOptions,
    ) -> Result<Self, ScriptError> {
        let runtime = DenoRuntime::new(module.clone(), &options.runtime_options)
            .change_context(ScriptError)?;

        let transform_timeout = options
            .transform_timeout
            .unwrap_or_else(|| Duration::from_secs(5));

        let load_timeout = options
            .load_timeout
            .unwrap_or_else(|| Duration::from_secs(60));

        Ok(Script {
            runtime,
            module,
            transform_timeout,
            load_timeout,
        })
    }

    /// Checks that the script exports a default transform function.
    pub async fn check_transform_is_exported(&mut self) -> Result<(), ScriptError> {
        let code: FastString = format!(
            r#"(async (globalThis) => {{
                const indexerIO = globalThis.Indexer.io;

                const module = await import("{0}");
                if (typeof module.default !== 'function') {{
                    return indexerIO.setOutput(1);
                }} else if (module.default.length != 1) {{
                    scriptResult = 2;
                    return indexerIO.setOutput(2);
                }} else {{
                    return indexerIO.setOutput(0);
                }}
            }})(globalThis)"#,
            self.module
        )
        .into();

        let result = self
            .runtime
            .execute_script_with_timeout(code, Value::Null, &self.load_timeout)
            .await
            .change_context(ScriptError)
            .attach_printable("failed to check if transform function is exported")?;

        match result.as_u64() {
            Some(0) => Ok(()),
            Some(1) => Err(ScriptError)
                .attach_printable("script does not export a default transform function"),
            Some(2) => {
                Err(ScriptError).attach_printable("transform function must take one argument")
            }
            Some(n) => Err(ScriptError)
                .attach_printable("internal error: script returned an invalid number")
                .attach_printable_lazy(|| format!("error code: {}", n)),
            None => {
                Err(ScriptError).attach_printable("internal error: script did not return a number")
            }
        }
    }

    /// Returns the configuration object exported by the script.
    pub async fn configuration<C>(&mut self) -> Result<C, ScriptError>
    where
        C: serde::de::DeserializeOwned,
    {
        let code: FastString = format!(
            r#"(async (globalThis) => {{
                const module = await import("{0}");
                globalThis.Indexer.io.setOutput(module.config);
            }})(globalThis)"#,
            self.module
        )
        .into();

        let configuration = self
            .runtime
            .execute_script_with_timeout(code, Value::Null, &self.load_timeout)
            .await
            .change_context(ScriptError)
            .attach_printable("failed to load indexer configuration")?;

        if Value::Null == configuration {
            return Err(ScriptError)
                .attach_printable("script did not return a configuration object")
                .attach_printable("hint: did you export `config` from the script?");
        }
        let configuration = serde_json::from_value(configuration)
            .change_context(ScriptError)
            .attach_printable("failed to deserialize configuration from script")?;

        Ok(configuration)
    }

    pub async fn transform(&mut self, data: Value) -> Result<Value, ScriptError> {
        let code: FastString = format!(
            r#"(async (globalThis) => {{
                const indexerIO = globalThis.Indexer.io;
                const module = await import("{0}");
                const t = module.default;

                const block = indexerIO.getInput();
                let output;
                if (t.constructor.name === 'AsyncFunction') {{
                    output = await t(block);
                }} else {{
                    output = t(block);
                }}

                if (typeof output === 'undefined') {{
                    output = null;
                }}

                indexerIO.setOutput(output);
            }})(globalThis)"#,
            self.module,
        )
        .into();

        self.runtime
            .execute_script_with_timeout(code, data, &self.transform_timeout)
            .await
            .change_context(ScriptError)
            .attach_printable("failed to invoke indexer transform function")
    }

    /// Returns true if the script is a factory script.
    pub async fn has_factory(&mut self) -> Result<bool, ScriptError> {
        let code: FastString = format!(
            r#"(async (globalThis) => {{
                const indexerIO = globalThis.Indexer.io;
                const module = await import("{0}");

                const hasFactory = typeof module.factory === 'function';
                const hasOneArgument = hasFactory && module.factory.length === 1;

                if (hasFactory && hasOneArgument) {{
                    return indexerIO.setOutput(1);
                }}

                return indexerIO.setOutput(0);
            }})(globalThis)"#,
            self.module
        )
        .into();

        let result = self
            .runtime
            .execute_script_with_timeout(code, Value::Null, &self.transform_timeout)
            .await
            .change_context(ScriptError)
            .attach_printable("failed to check if indexer exports factory")?;

        match result.as_u64() {
            Some(0) => Ok(false),
            Some(1) => Ok(true),
            Some(n) => Err(ScriptError)
                .attach_printable("internal error: script returned an invalid number")
                .attach_printable_lazy(|| format!("error code: {}", n)),
            None => {
                Err(ScriptError).attach_printable("internal error: script did not return a number")
            }
        }
    }

    /// Returns the configuration object exported by the script.
    pub async fn factory<F>(&mut self, data: Value) -> Result<FactoryResult<F>, ScriptError>
    where
        F: serde::de::DeserializeOwned,
    {
        let code: FastString = format!(
            r#"(async (globalThis) => {{
                const indexerIO = globalThis.Indexer.io;
                const module = await import("{0}");

                const f = module.factory;
                const block = indexerIO.getInput();
                let output;
                if (t.constructor.name === 'AsyncFunction') {{
                    output = await f(block);
                }} else {{
                    output = f(block);
                }}

                if (typeof output === 'undefined') {{
                    output = null;
                }}

                indexerIO.setOutput(output);
            }})(globalThis)"#,
            self.module,
        )
        .into();

        let result = self
            .runtime
            .execute_script_with_timeout(code, data, &self.transform_timeout)
            .await
            .change_context(ScriptError)
            .attach_printable("failed to invoke indexer factory")?;

        if Value::Null == result {
            return Ok(FactoryResult {
                filter: None,
                data: None,
            });
        }

        let result = serde_json::from_value(result)
            .change_context(ScriptError)
            .attach_printable("failed to deserialize factory result")?;

        Ok(result)
    }
}
