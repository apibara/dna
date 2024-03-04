deno_core::extension!(
    apibara_script,
    ops = [
        ops::op_input_get,
        ops::op_output_set,
    ],
    esm_entry_point = "ext:apibara_script/env.js",
    esm = [dir "js", "env.js"],
);

pub struct TransformState {
    pub input: serde_json::Value,
    pub output: serde_json::Value,
}

mod ops {
    use deno_core::op2;

    use super::TransformState;

    #[op2]
    #[serde]
    pub fn op_input_get(#[state] state: &TransformState) -> serde_json::Value {
        state.input.clone()
    }

    #[op2]
    pub fn op_output_set(#[state] state: &mut TransformState, #[serde] value: serde_json::Value) {
        state.output = value;
    }
}
