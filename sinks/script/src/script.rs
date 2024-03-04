use std::{fmt, path::Path, rc::Rc, time::Duration};

use deno_core::{FastString, ModuleSpecifier};
use deno_runtime::{
    permissions::{Permissions, PermissionsContainer, PermissionsOptions},
    worker::{MainWorker, WorkerOptions},
};
use error_stack::{Result, ResultExt};

use crate::{
    ext::{apibara_script, TransformState},
    module_loader::WorkerModuleLoader,
};

pub use serde_json::Value;

pub struct Script {
    worker: MainWorker,
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

#[derive(Debug, Default)]
pub struct ScriptOptions {
    /// Environment variables the script has access to.
    ///
    /// An empty list gives access to _ALL_ environment variables.
    pub allow_env: Option<Vec<String>>,
    /// Allow network access to the given hosts.
    ///
    /// An empty list gives access to _ALL_ hosts.
    pub allow_net: Option<Vec<String>>,
    /// Allow read access to the given paths.
    ///
    /// An empty list gives access to _ALL_ paths.
    pub allow_read: Option<Vec<String>>,
    /// Allow write access to the given paths.
    ///
    /// An empty list gives access to _ALL_ paths.
    pub allow_write: Option<Vec<String>>,
    /// Maximum time allowed to execute the transform function.
    pub transform_timeout: Option<Duration>,
    /// Maximum time allowed to load the indexer script.
    pub load_timeout: Option<Duration>,
}

enum ScriptTimeout {
    Transform,
    Load,
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
        let module_loader = WorkerModuleLoader::new();
        let permissions = Self::default_permissions(&options)?;
        let worker = MainWorker::bootstrap_from_options(
            module.clone(),
            permissions,
            WorkerOptions {
                module_loader: Rc::new(module_loader),
                startup_snapshot: None,
                extensions: vec![apibara_script::init_ops_and_esm()],
                ..WorkerOptions::default()
            },
        );

        let transform_timeout = options
            .transform_timeout
            .unwrap_or_else(|| Duration::from_secs(5));

        let load_timeout = options
            .load_timeout
            .unwrap_or_else(|| Duration::from_secs(60));

        Ok(Script {
            worker,
            module,
            transform_timeout,
            load_timeout,
        })
    }

    /// Checks that the script exports a default transform function.
    pub async fn check_transform_is_exported(&mut self) -> Result<(), ScriptError> {
        let code: FastString = format!(
            r#"(async (globalThis) => {{
                const module = await import("{0}");
                __script_result = 0;
                if (typeof module.default !== 'function') {{
                    __script_result = 1;
                }} else if (module.default.length != 1) {{
                    __script_result = 2;
                }}
                globalThis.Script.output_set(__script_result);
            }})(globalThis)"#,
            self.module
        )
        .into();

        let result = self
            .execute_script_with_timeout(code, Value::Null, ScriptTimeout::Load)
            .await?;

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
                globalThis.Script.output_set(module.config);
            }})(globalThis)"#,
            self.module
        )
        .into();

        let configuration = self
            .execute_script_with_timeout(code, Value::Null, ScriptTimeout::Load)
            .await?;

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

    #[tracing::instrument(skip_all, err(Debug))]
    pub async fn transform(&mut self, data: Vec<Value>) -> Result<Value, ScriptError> {
        let code: FastString = format!(
            r#"(async (globalThis) => {{
            const module = await import("{0}");
            const t = module.default;
            let output;

            if (t.constructor.name === 'AsyncFunction') {{
              const block = globalThis.Script.input_get();
              output = await t(block);
            }} else {{
              const block = globalThis.Script.input_get();
              output = t(block);
            }}
            let __script_result = output;

            if (typeof __script_result === 'undefined') {{
              __script_result = null;
            }}

            globalThis.Script.output_set(__script_result);

            globalThis.Script.output_set(__script_result);
        }})(globalThis)"#,
            self.module,
        )
        .into();

        self.execute_script_with_timeout(code, data, ScriptTimeout::Transform)
            .await
    }

    /// Returns true if the script is a factory script.
    pub async fn has_factory(&mut self) -> Result<bool, ScriptError> {
        let code: FastString = format!(
            r#"(async (globalThis) => {{
                const module = await import("{0}");
                __script_result = 0;
                const hasFactory = typeof module.factory === 'function';
                const hasOneArgument = hasFactory && module.factory.length === 1;
                if (hasFactory && hasOneArgument) {{
                    __script_result = 1;
                }}
                globalThis.Script.output_set(__script_result);
            }})(globalThis)"#,
            self.module
        )
        .into();

        let result = self
            .execute_script_with_timeout(code, Value::Null, ScriptTimeout::Load)
            .await?;

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
            const module = await import("{0}");
            const t = module.factory;
            const block = globalThis.Script.input_get();
            if (block.empty) {{
              output = undefined;
            }} else if (t.constructor.name === 'AsyncFunction') {{
              output = await t(block);
            }} else {{
              output = t(block);
            }}
            let __script_result = output;

            if (typeof __script_result === 'undefined') {{
              __script_result = null;
            }}

            globalThis.Script.output_set(__script_result);
        }})(globalThis)"#,
            self.module,
        )
        .into();

        let result = self
            .execute_script_with_timeout(code, data, ScriptTimeout::Transform)
            .await?;

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

    async fn execute_script_with_timeout(
        &mut self,
        code: FastString,
        input: Value,
        timeout: ScriptTimeout,
    ) -> Result<Value, ScriptError> {
        let state = self.worker.js_runtime.op_state();

        state.borrow_mut().put::<TransformState>(TransformState {
            input,
            output: Value::Null,
        });

        let future = async {
            match self.worker.execute_script("[script]", code) {
                Ok(_) => {}
                Err(err) => {
                    return Err(ScriptError)
                        .attach_printable("failed to execute indexer script")
                        .attach_printable_lazy(|| format!("error: {err:?}"));
                }
            }

            match self.worker.run_event_loop(false).await {
                Ok(_) => Ok(()),
                Err(err) => Err(ScriptError)
                    .attach_printable("failed to run indexer event loop")
                    .attach_printable_lazy(|| format!("error: {err:?}")),
            }
        };

        let timeout = match timeout {
            ScriptTimeout::Transform => self.transform_timeout,
            ScriptTimeout::Load => self.load_timeout,
        };

        match tokio::time::timeout(timeout, future).await {
            Ok(result) => {
                result?;
            }
            Err(_) => {
                return Err(ScriptError)
                    .attach_printable("indexer script timed out")
                    .attach_printable_lazy(|| format!("timeout: {:?}", timeout));
            }
        }

        let state = state.borrow_mut().take::<TransformState>();
        Ok(state.output)
    }

    fn default_permissions(options: &ScriptOptions) -> Result<PermissionsContainer, ScriptError> {
        let allow_env = options.allow_env.clone().map(remove_empty_strings);
        // If users use an empty hostname, allow all hosts.
        let allow_net = options.allow_net.clone().map(remove_empty_strings);
        let allow_read = options.allow_read.clone().map(|paths| {
            remove_empty_strings(paths)
                .into_iter()
                .map(Into::into)
                .collect()
        });
        let allow_write = options.allow_write.clone().map(|paths| {
            remove_empty_strings(paths)
                .into_iter()
                .map(Into::into)
                .collect()
        });

        match Permissions::from_options(&PermissionsOptions {
            allow_env,
            allow_hrtime: true,
            allow_net,
            allow_read,
            allow_write,
            prompt: false,
            ..PermissionsOptions::default()
        }) {
            Ok(permissions) => Ok(PermissionsContainer::new(permissions)),
            Err(err) => Err(ScriptError)
                .attach_printable("failed to create Deno permissions")
                .attach_printable_lazy(|| format!("error: {err:?}")),
        }
    }
}

pub fn remove_empty_strings(vec: Vec<String>) -> Vec<String> {
    vec.into_iter().filter(|s| !s.is_empty()).collect()
}

#[cfg(test)]
mod tests {
    use super::remove_empty_strings;

    #[test]
    fn test_remove_empty_string() {
        let r0 = remove_empty_strings(vec!["".to_string()]);
        assert!(r0.is_empty());
        let r1 = remove_empty_strings(vec!["abcd".to_string(), "".to_string()]);
        assert_eq!(r1.len(), 1);
    }
}
