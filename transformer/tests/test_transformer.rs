use std::fs;

use apibara_transformer::Transformer;

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

async fn new_transformer_with_code(ext: &str, code: &str) -> (NamedTempFile, Transformer) {
    let file = write_source(ext, code);
    let transformer = Transformer::from_file(
        file.path().to_str().unwrap(),
        std::env::current_dir().unwrap(),
    )
    .unwrap();
    (file, transformer)
}

#[tokio::test]
async fn test_identity() {
    let (_file, mut transformer) = new_transformer_with_code(
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
    let result = transformer.transform(&input).await.unwrap();
    assert_eq!(result, input);
}

#[tokio::test]
async fn test_return_data_is_different() {
    let (_file, mut transformer) = new_transformer_with_code(
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
    let result = transformer.transform(&input).await.unwrap();
    let expected = json!([{
        "foo": "bar",
    }, {
        "foo": "bux",
    }]);
    assert_eq!(result, expected);
}

/*
#[tokio::test]
async fn test_import_library_over_http() {
    let (_file, mut transformer) = new_transformer_with_code(
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
    let result = transformer.transform(&input).await.unwrap();
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
    let (_file, mut transformer) = new_transformer_with_code(
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
    let result = transformer.transform(&input).await.unwrap();
    assert_eq!(result, input);
}
*/
