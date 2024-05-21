use deno_core::op2;

deno_core::extension!(
    indexer_io,
    ops = [
        op_get_input,
        op_set_output,
    ],
    esm_entry_point = "ext:indexer_io/io.js",
    esm = [dir "js", "io.js"],
);

pub struct TransformState {
    pub input: serde_json::Value,
    pub output: serde_json::Value,
}

#[op2]
#[serde]
pub fn op_get_input(#[state] state: &TransformState) -> serde_json::Value {
    state.input.clone()
}

#[op2]
pub fn op_set_output(#[state] state: &mut TransformState, #[serde] value: serde_json::Value) {
    state.output = value;
}
