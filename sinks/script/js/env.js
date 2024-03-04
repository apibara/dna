const core = globalThis.Deno.core;
const ops = core.ops;

function input_get() {
  return ops.op_input_get();
}

function output_set(value) {
  ops.op_output_set(value);
}

globalThis.Script = {
  input_get,
  output_set,
};
