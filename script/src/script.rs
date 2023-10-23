use std::{borrow::Cow, fmt, path::Path, rc::Rc, time::Duration};

use deno_core::{op, FastString, ModuleSpecifier, OpState, ZeroCopyBuf};
use deno_runtime::{
    permissions::{Permissions, PermissionsContainer, PermissionsOptions},
    worker::{MainWorker, WorkerOptions},
};
use error_stack::{Result, ResultExt};

use crate::module_loader::WorkerModuleLoader;

deno_core::extension!(
    apibara_script,
    ops = [op_script_return],
    esm_entry_point = "ext:apibara_script/env.js",
    esm = ["env.js"],
    customizer = |ext: &mut deno_core::ExtensionBuilder| {
        ext.force_op_registration();
    },
);

pub use serde_json::Value;

pub struct Script {
    worker: MainWorker,
    module: ModuleSpecifier,
    _timeout: Duration,
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
        let permissions = Self::default_permissions(options)?;
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

        Ok(Script {
            worker,
            module,
            _timeout: Duration::from_secs(5),
        })
    }

    /// Returns the configuration object exported by the script.
    pub async fn configuration<C>(&mut self) -> Result<C, ScriptError>
    where
        C: serde::de::DeserializeOwned,
    {
        let code: FastString = format!(
            r#"(async (globalThis) => {{
                const module = await import("{0}");
                __script_result = module.config;
                globalThis.Script.return_value(__script_result);
            }})(globalThis)"#,
            self.module
        )
        .into();
        let configuration = self.execute_script(code).await?;
        let configuration = serde_json::from_value(configuration)
            .change_context(ScriptError)
            .attach_printable("failed to deserialize configuration from script")?;
        Ok(configuration)
    }

    pub async fn transform(&mut self, data: &Value) -> Result<Value, ScriptError> {
        let code: FastString = format!(
            r#"(async (globalThis) => {{
            const module = await import("{0}");
            const t = module.default;
            const data = {1};
            let __script_result;
            if (t.constructor.name === 'AsyncFunction') {{
              // "flatten" the results into an array of values.
              const r = await Promise.all(data.map(t));
              __script_result = r.flatMap(x => x);
            }} else {{
              __script_result = data.flatMap(t);
            }}
            if (typeof __script_result === 'undefined') {{
              __script_result = null;
            }}
            globalThis.Script.return_value(__script_result);
        }})(globalThis)"#,
            self.module, data,
        )
        .into();

        self.execute_script(code).await
    }

    async fn execute_script(&mut self, code: FastString) -> Result<Value, ScriptError> {
        match self.worker.execute_script("[script]", code) {
            Ok(_) => {}
            Err(err) => {
                return Err(ScriptError)
                    .attach_printable("failed to execute indexer script")
                    .attach_printable_lazy(|| format!("error: {err:?}"));
            }
        }

        // TODO:
        //  - limit amount of time
        //  - limit amount of memory
        match self.worker.run_event_loop(false).await {
            Ok(_) => {}
            Err(err) => {
                return Err(ScriptError)
                    .attach_printable("failed to run indexer event loop")
                    .attach_printable_lazy(|| format!("error: {err:?}"));
            }
        }

        let state = self.worker.js_runtime.op_state();
        let mut state = state.borrow_mut();
        let resource_table = &mut state.resource_table;
        let resource_id = resource_table
            .names()
            .find(|(_, name)| name == "__rust_ReturnValue");

        match resource_id {
            None => Ok(Value::Null),
            Some((rid, _)) => {
                let entry: Rc<ReturnValueResource> =
                    resource_table.take(rid).expect("resource entry");
                let value = Rc::try_unwrap(entry).expect("value");
                Ok(value.value)
            }
        }
    }

    fn default_permissions(options: ScriptOptions) -> Result<PermissionsContainer, ScriptError> {
        match Permissions::from_options(&PermissionsOptions {
            allow_env: options.allow_env,
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

#[derive(Debug)]
struct ReturnValueResource {
    value: Value,
}

impl deno_core::Resource for ReturnValueResource {
    fn name(&self) -> Cow<str> {
        "__rust_ReturnValue".into()
    }
}

#[op]
fn op_script_return(
    state: &mut OpState,
    args: Value,
    _buf: Option<ZeroCopyBuf>,
) -> std::result::Result<Value, deno_core::anyhow::Error> {
    let value = ReturnValueResource { value: args };
    state.resource_table.add(value);

    Ok(Value::Null)
}
