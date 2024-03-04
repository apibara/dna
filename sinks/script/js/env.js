const core = globalThis.Deno.core;
const ops = core.ops;

function batch_size() {
  return ops.op_batch_size();
}

function batch_get(index) {
  return ops.op_batch_get(index);
}

function output_set(value) {
  ops.op_output_set(value);
}

globalThis.Script = {
  batch_size,
  batch_get,
  output_set,
};
