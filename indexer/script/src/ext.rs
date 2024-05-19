use std::sync::Arc;

use rusqlite::Connection;

deno_core::extension!(
    apibara_script,
    ops = [
        ops::op_input_get,
        ops::op_output_set,
        ops::kv_get,
        ops::kv_set,
        ops::kv_del,
    ],
    esm_entry_point = "ext:apibara_script/env.js",
    esm = [dir "js", "env.js"],
);

pub struct TransformState {
    pub input: serde_json::Value,
    pub output: serde_json::Value,
}

pub struct KeyValueState {
    pub connection: Arc<Connection>,
}

mod ops {
    use deno_core::op2;
    use rusqlite::OptionalExtension;

    use super::{KeyValueState, TransformState};

    #[op2]
    #[serde]
    pub fn op_input_get(#[state] state: &TransformState) -> serde_json::Value {
        state.input.clone()
    }

    #[op2]
    pub fn op_output_set(#[state] state: &mut TransformState, #[serde] value: serde_json::Value) {
        state.output = value;
    }

    #[op2]
    #[serde]
    pub fn kv_get(
        #[state] state: &mut KeyValueState,
        #[string] key: String,
    ) -> Option<serde_json::Value> {
        state
            .connection
            .query_row("select value from kvs where key = ?", [&key], |row| {
                let value: String = row.get(0)?;
                println!("value = {}", value);
                let value: serde_json::Value = serde_json::from_str(&value).unwrap();
                Ok(value)
            })
            .optional()
            .unwrap()
    }

    #[op2]
    pub fn kv_set(
        #[state] state: &mut KeyValueState,
        #[string] key: String,
        #[serde] value: serde_json::Value,
    ) {
        let serialized = serde_json::to_string(&value).unwrap();
        state
            .connection
            .execute(
                "insert into kvs(key, value) values(?1, ?2) on conflict (key) do update set value = excluded.value",
                (&key, &serialized),
            )
            .unwrap();
    }

    #[op2(fast)]
    pub fn kv_del(#[state] state: &mut KeyValueState, #[string] key: String) {
        state
            .connection
            .execute("delete from kvs where key = ?", [&key])
            .unwrap();
    }
}
