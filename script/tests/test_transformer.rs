// TODO:
// - [ ] starknet.js import
// - [x] import from local file
// - [x] import json
use std::fs;

use apibara_script::{Script, ScriptError, ScriptOptions};
use assert_matches::assert_matches;
use serde_json::json;
use tempfile::NamedTempFile;

fn write_source(ext: &str, code: &str) -> NamedTempFile {
    let tempfile = tempfile::Builder::new()
        .suffix(&format!(".{}", ext))
        .tempfile()
        .unwrap();
    fs::write(tempfile.path(), code).unwrap();
    tempfile
}

async fn new_script_with_code_and_options(
    ext: &str,
    code: &str,
    options: ScriptOptions,
) -> (NamedTempFile, Script) {
    let file = write_source(ext, code);
    let script = Script::from_file(
        file.path().to_str().unwrap(),
        std::env::current_dir().unwrap(),
        options,
    )
    .unwrap();
    (file, script)
}

async fn new_script_with_code(ext: &str, code: &str) -> (NamedTempFile, Script) {
    new_script_with_code_and_options(ext, code, Default::default()).await
}

#[tokio::test]
async fn test_identity() {
    let (_file, mut script) = new_script_with_code(
        "js",
        r#"
        export default function (data) {
            console.log('data', data);
            return data;
        }
        "#,
    )
    .await;
    let input = json!({
        "foo": "bar",
        "baz": 42,
    });
    let result = script.transform(&input).await.unwrap();
    assert_eq!(result, input);
}

#[tokio::test]
async fn test_return_data_is_different() {
    let (_file, mut script) = new_script_with_code(
        "js",
        r#"
        export default function (data) {
            return data.map(({ foo }) => {
                return { foo };
            });
        }
        "#,
    )
    .await;
    let input = json!([{
        "foo": "bar",
        "baz": 42,
    }, {
        "foo": "bux",
    }]);
    let result = script.transform(&input).await.unwrap();
    let expected = json!([{
        "foo": "bar",
    }, {
        "foo": "bux",
    }]);
    assert_eq!(result, expected);
}

#[tokio::test]
async fn test_import_library_over_http() {
    let (_file, mut script) = new_script_with_code(
        "js",
        r#"
        import capitalizeKeys from 'https://cdn.jsdelivr.net/gh/stdlib-js/utils-capitalize-keys@deno/mod.js';
                                          //
        export default function (data) {
            return data.map((block) => {
                return capitalizeKeys(block);
            });
        }
        "#,
    )
    .await;
    let input = json!([{
        "foo": "bar",
        "baz": 42,
    }, {
        "foo": "bux",
    }]);
    let result = script.transform(&input).await.unwrap();
    let expected = json!([{
        "Foo": "bar",
        "Baz": 42,
    }, {
        "Foo": "bux",
    }]);
    assert_eq!(result, expected);
}

#[tokio::test]
async fn test_typescript() {
    let (_file, mut script) = new_script_with_code(
        "ts",
        r#"
        interface Data {
            foo: string;
            bar: number;
        }

        export default function (data: Data): Data {
            return data;
        }
        "#,
    )
    .await;
    let input = json!({
        "foo": "bar",
        "baz": 42,
    });
    let result = script.transform(&input).await.unwrap();
    assert_eq!(result, input);
}

#[tokio::test]
async fn test_import_data() {
    let json_file = write_source(
        "json",
        r#"{
      "is_json": true
    }"#,
    );

    let code = r#"
        import data from "<JSON_FILE>" assert { type: "json" };

        export default function (event: Event): Data {
          return data;
        }
        "#
    .replace("<JSON_FILE>", json_file.path().to_str().unwrap());

    let (_file, mut script) = new_script_with_code("ts", &code).await;
    let input = json!({
        "key": vec!["0x99cd8bde557814842a3121e8ddfd433a539b8c9f14bf31ebf108d12e6196e9"],
        "data": vec![
            "0x4391e7c963a1dced0d206278464778711f2ad480b34f22e1d658fb3f6ac81f3",
            "0x1176a1bd84444c89232ec27754698e5d2e7e1a7f1539f12027f28b23ec9f3d8",
            "0x1df635bc07855",
            "0x0"
        ],
    });
    let result = script.transform(&input).await.unwrap();
    let expected = json!({
        "is_json": true
    });
    assert_eq!(result, expected);
}

#[tokio::test]
async fn test_use_starknet_js() {
    let (_file, mut script) = new_script_with_code(
        "js",
        r#"
        import { hash } from 'https://esm.sh/starknet';

        export default function (data) {
          const result = hash.getSelectorFromName(data.key);
          return { result };
        }
        "#,
    )
    .await;
    let input = json!([{
        "key": "Transfer",
    }]);
    let result = script.transform(&input).await.unwrap();
    let expected = json!({
        "result": "0x1d2460186f7233c927e7db2dcc703c0e500b653ca82273b7bfad8045d85a470",
    });
    assert_eq!(result, expected);
}

#[tokio::test]
async fn test_net_is_denied() {
    let (_file, mut script) = new_script_with_code(
        "js",
        r#"
        export default function (data) {
          const result = fetch('https://httpbin.org/get');
          return { result };
        }
        "#,
    )
    .await;
    let input = json!({});
    let result = script.transform(&input).await;
    assert_matches!(result, Err(ScriptError::Deno(_)));
}

#[tokio::test]
async fn test_read_is_denied() {
    let (_file, mut script) = new_script_with_code(
        "js",
        r#"
        export default function (data) {
          const result = Deno.readDir('.');
          return { result };
        }
        "#,
    )
    .await;
    let input = json!({});
    let result = script.transform(&input).await;
    assert_matches!(result, Err(ScriptError::Deno(_)));
}

#[tokio::test]
async fn test_write_is_denied() {
    let (_file, mut script) = new_script_with_code(
        "js",
        r#"
        export default function (data) {
          const result = Deno.writeTextFile('test.txt', 'test failed');
          return { result };
        }
        "#,
    )
    .await;
    let input = json!({});
    let result = script.transform(&input).await;
    assert_matches!(result, Err(ScriptError::Deno(_)));
}

#[tokio::test]
async fn test_run_is_denied() {
    let (_file, mut script) = new_script_with_code(
        "js",
        r#"
        export default function (data) {
          const result = Deno.run({ cmd: ['ls'] });
          return { result };
        }
        "#,
    )
    .await;
    let input = json!({});
    let result = script.transform(&input).await;
    assert_matches!(result, Err(ScriptError::Deno(_)));
}

#[tokio::test]
async fn test_sys_is_denied() {
    let (_file, mut script) = new_script_with_code(
        "js",
        r#"
        export default function (data) {
          const result = Deno.osRelease();
          return { result };
        }
        "#,
    )
    .await;
    let input = json!({});
    let result = script.transform(&input).await;
    assert_matches!(result, Err(ScriptError::Deno(_)));
}

#[tokio::test]
async fn test_env_is_denied_by_default() {
    let (_file, mut script) = new_script_with_code(
        "js",
        r#"
        export default function (data) {
          const result = Deno.env.toObject();
          return { result };
        }
        "#,
    )
    .await;
    let input = json!({});
    let result = script.transform(&input).await;
    assert_matches!(result, Err(ScriptError::Deno(_)));
}

#[tokio::test]
async fn test_env_can_access_some_variables() {
    let (_file, mut script) = new_script_with_code_and_options(
        "js",
        r#"
        export default function (data) {
          const result = Deno.env.get('CARGO');
          return { result };
        }
        "#,
        ScriptOptions {
            allow_env: Some(vec!["CARGO".to_string()]),
            ..Default::default()
        },
    )
    .await;
    let input = json!({});
    script.transform(&input).await.unwrap();
}
