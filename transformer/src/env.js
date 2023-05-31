const core = globalThis.Deno.core;
const ops = core.ops;

function return_value(value) {
  ops.op_transform_return(value);
}

globalThis.Transform = { return_value }
