const core = globalThis.Deno.core;
const ops = core.ops;

function return_value(value) {
  ops.op_script_return(value);
}

globalThis.Script = { return_value }
