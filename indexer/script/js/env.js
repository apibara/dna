const core = globalThis.Deno.core;
const ops = core.ops;

function input_get() {
  return ops.op_input_get();
}

function output_set(value) {
  ops.op_output_set(value);
}

class KeyValue {
  static get(key) {
    console.log("GET", key);
    return ops.kv_get(key);
  }

  static set(key, value) {
    console.log("SET", key, value);
    ops.kv_set(key, value);
  }

  static del(key) {
    console.log("DEL", key);
    ops.kv_del(key);
  }
}

globalThis.Script = {
  input_get,
  output_set,
  kv: KeyValue,
};
