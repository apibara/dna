use std::{borrow::Cow, path::Path, rc::Rc, time::Duration};

use deno_core::{
    op, Extension, FastString, FsModuleLoader, JsRuntime, ModuleSpecifier, OpState, RuntimeOptions,
    ZeroCopyBuf,
};
use tokio::time::timeout;

pub use serde_json::Value;

pub struct Transformer {
    runtime: JsRuntime,
    module: ModuleSpecifier,
    timeout: Duration,
    resource_id: u32,
}

#[derive(Debug, thiserror::Error)]
pub enum TransformerError {
    #[error("Failed to resolve module: {0}")]
    ModuleResolution(#[from] deno_core::ModuleResolutionError),
    #[error(transparent)]
    Deno(#[from] deno_core::anyhow::Error),
    #[error("Transformer timed out: {0}")]
    Timeout(#[from] tokio::time::error::Elapsed),
}

impl Transformer {
    /// Creates a [Transformer] from the given file.
    ///
    /// A relative file path is considered relative to the given current directory.
    pub fn from_file(path: &str, current_dir: impl AsRef<Path>) -> Result<Self, TransformerError> {
        let module = deno_core::resolve_path(path, current_dir.as_ref())?;
        Self::from_module(module)
    }

    /// Creates a [Transformer] from the given module specifier.
    pub fn from_module(module: ModuleSpecifier) -> Result<Self, TransformerError> {
        let ext = Extension::builder("transformer")
            .ops(vec![(op_return::decl())])
            .build();
        let module_loader = Rc::new(FsModuleLoader);
        let mut runtime = JsRuntime::new(RuntimeOptions {
            module_loader: Some(module_loader),
            extensions: vec![ext],
            ..RuntimeOptions::default()
        });

        let module_id =
            deno_core::futures::executor::block_on(runtime.load_side_module(&module, None))?;
        let module_result = runtime.mod_evaluate(module_id);
        deno_core::futures::executor::block_on(runtime.run_event_loop(false))?;
        deno_core::futures::executor::block_on(module_result).unwrap()?;

        Ok(Transformer {
            runtime,
            module,
            timeout: Duration::from_secs(5),
            resource_id: 0,
        })
    }

    pub async fn transform(&mut self, data: &Value) -> Result<Value, TransformerError> {
        let code: FastString = format!(
            r#"(async () => {{
            const module = await import("{0}");
            const t = module.default;
            const data = {1};
            let __transform_result = t.constructor.name === 'AsyncFunction'
              ? await t(data)
              : t(data);
            if (typeof __transform_result === 'undefined') {{
              __transform_result = null;
            }}
            Deno.core.ops.op_return(__transform_result);
        }})()"#,
            self.module, data,
        )
        .into();

        self.runtime.execute_script("[transform]", code)?;
        timeout(self.timeout, self.runtime.run_event_loop(false)).await??;

        let state = self.runtime.op_state();
        let mut state = state.borrow_mut();
        let resource_table = &mut state.resource_table;

        let entry: Rc<ReturnValueResource> = resource_table
            .take(self.resource_id)
            .expect("resource entry");
        let value = Rc::try_unwrap(entry).expect("value");
        self.resource_id += 1;

        Ok(value.value)
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
fn op_return(
    state: &mut OpState,
    args: Value,
    _buf: Option<ZeroCopyBuf>,
) -> Result<Value, deno_core::anyhow::Error> {
    let value = ReturnValueResource { value: args };
    let _rid = state.resource_table.add(value);
    Ok(Value::Null)
}
