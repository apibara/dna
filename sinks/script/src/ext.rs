deno_core::extension!(
    apibara_script,
    ops = [
        ops::op_batch_size,
        ops::op_batch_get,
        ops::op_output_set,
    ],
    esm_entry_point = "ext:apibara_script/env.js",
    esm = [dir "js", "env.js"],
);

pub struct TransformState {
    pub input_batch: Vec<serde_json::Value>,
    pub output: serde_json::Value,
}

mod ops {
    use deno_core::op2;

    use super::TransformState;

    #[op2(fast)]
    pub fn op_batch_size(#[state] state: &TransformState) -> u32 {
        state.input_batch.len() as u32
    }

    #[op2]
    #[serde]
    pub fn op_batch_get(#[state] state: &TransformState, index: u32) -> serde_json::Value {
        let index = index as usize;
        if index > state.input_batch.len() {
            serde_json::Value::Null
        } else {
            state.input_batch[index].clone()
        }
    }

    #[op2]
    pub fn op_output_set(#[state] state: &mut TransformState, #[serde] value: serde_json::Value) {
        state.output = value;
    }
}
