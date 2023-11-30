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
            .execute_script_with_timeout(code, Vec::default(), ScriptTimeout::Load)
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
            .execute_script_with_timeout(code, Vec::default(), ScriptTimeout::Load)
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

    pub async fn transform(&mut self, data: Vec<Value>) -> Result<Value, ScriptError> {
        let code: FastString = format!(
            r#"(async (globalThis) => {{
            const module = await import("{0}");
            const t = module.default;
            let batchSize = globalThis.Script.batch_size();
            let output = Array(batchSize);

            if (t.constructor.name === 'AsyncFunction') {{
              let promises = Array(batchSize);
              for (let i = 0; i < batchSize; i++) {{
                promises[i] = await t(globalThis.Script.batch_get(i));
              }}
              output = await Promise.all(promises);
            }} else {{
              for (let i = 0; i < batchSize; i++) {{
                output[i] = t(globalThis.Script.batch_get(i));
              }}
            }}

            // "flatten" the results into an array of values.
            let __script_result = output.flatMap(x => x);

            if (typeof __script_result === 'undefined') {{
              __script_result = null;
            }}

            globalThis.Script.output_set(__script_result);
        }})(globalThis)"#,
            self.module,
        )
        .into();

        self.execute_script_with_timeout(code, data, ScriptTimeout::Transform)
            .await
    }

    async fn execute_script_with_timeout(
        &mut self,
        code: FastString,
        input: Vec<Value>,
        timeout: ScriptTimeout,
    ) -> Result<Value, ScriptError> {
        let state = self.worker.js_runtime.op_state();

        state.borrow_mut().put::<TransformState>(TransformState {
            input_batch: input,
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
        match Permissions::from_options(&PermissionsOptions {
            allow_env: options.allow_env.clone(),
            allow_hrtime: true,
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
