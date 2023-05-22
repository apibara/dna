use std::fs;

use apibara_transformer::Transformer;

use serde_json::json;
use tempfile::NamedTempFile;

fn write_source(code: &str) -> NamedTempFile {
    let tempfile = tempfile::Builder::new().suffix(".js").tempfile().unwrap();
    fs::write(tempfile.path(), code).unwrap();
    tempfile
}

async fn new_transformer_with_code(code: &str) -> Transformer {
    let file = write_source(code);
    Transformer::from_file(
        file.path().to_str().unwrap(),
        std::env::current_dir().unwrap(),
    )
    .await
    .unwrap()
}

#[tokio::test]
async fn test_identity() {
    let mut transformer = new_transformer_with_code(
        r#"
        export default function (data) {
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
    let mut transformer = new_transformer_with_code(
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
