use std::{fmt, rc::Rc, time::Duration};

use deno_core::{FastString, ModuleSpecifier};
use deno_runtime::{
    permissions::{Permissions, PermissionsContainer, PermissionsOptions},
    worker::{MainWorker, WorkerOptions},
};
use error_stack::{Result, ResultExt};
use serde_json::Value;

use crate::{
    ext::{
        indexer_main,
        io::{indexer_io, TransformState},
    },
    module_loader::WorkerModuleLoader,
};

#[derive(Debug, Default)]
pub struct DenoRuntimeOptions {
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
}

#[derive(Debug)]
pub struct DenoRuntimeError;

pub struct DenoRuntime {
    worker: MainWorker,
}

impl DenoRuntime {
    pub fn new(
        module: ModuleSpecifier,
        options: &DenoRuntimeOptions,
    ) -> Result<Self, DenoRuntimeError> {
        let module_loader = WorkerModuleLoader::new();
        let permissions = Self::default_permissions(&options)?;
        let worker = MainWorker::bootstrap_from_options(
            module,
            permissions,
            WorkerOptions {
                module_loader: Rc::new(module_loader),
                startup_snapshot: None,
                extensions: vec![
                    indexer_io::init_ops_and_esm(),
                    indexer_main::init_ops_and_esm(),
                ],
                ..WorkerOptions::default()
            },
        );

        Ok(Self { worker })
    }

    pub async fn execute_script_with_timeout(
        &mut self,
        code: FastString,
        input: Value,
        timeout: &Duration,
    ) -> Result<Value, DenoRuntimeError> {
        let state = self.worker.js_runtime.op_state();

        state.borrow_mut().put::<TransformState>(TransformState {
            input,
            output: Value::Null,
        });

        let future = async {
            match self.worker.execute_script("[script]", code) {
                Ok(_) => {}
                Err(err) => {
                    return Err(DenoRuntimeError)
                        .attach_printable("failed to execute indexer script")
                        .attach_printable_lazy(|| format!("error: {err:?}"));
                }
            }

            match self.worker.run_event_loop(false).await {
                Ok(_) => Ok(()),
                Err(err) => Err(DenoRuntimeError)
                    .attach_printable("failed to run indexer event loop")
                    .attach_printable_lazy(|| format!("error: {err:?}")),
            }
        };

        match tokio::time::timeout(*timeout, future).await {
            Ok(result) => {
                result?;
            }
            Err(_) => {
                return Err(DenoRuntimeError)
                    .attach_printable("indexer script timed out")
                    .attach_printable_lazy(|| format!("timeout: {:?}", timeout));
            }
        }

        let state = state.borrow_mut().take::<TransformState>();
        Ok(state.output)
    }

    fn default_permissions(
        options: &DenoRuntimeOptions,
    ) -> Result<PermissionsContainer, DenoRuntimeError> {
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
            allow_env: options.allow_env.clone(),
            allow_hrtime: true,
            allow_net,
            allow_read,
            allow_write,
            prompt: false,
            ..PermissionsOptions::default()
        }) {
            Ok(permissions) => Ok(PermissionsContainer::new(permissions)),
            Err(err) => Err(DenoRuntimeError)
                .attach_printable("failed to create Deno permissions")
                .attach_printable_lazy(|| format!("error: {err:?}")),
        }
    }
}

impl error_stack::Context for DenoRuntimeError {}

impl fmt::Display for DenoRuntimeError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("Deno runtime error")
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
