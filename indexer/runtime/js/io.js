const core = globalThis.Deno.core;
const ops = core.ops;

const indexerIO = {
  getInput: () => ops.op_get_input(),
  setOutput: (value) => ops.op_set_output(value),
};

export { indexerIO };
