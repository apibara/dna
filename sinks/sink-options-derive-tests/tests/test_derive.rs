use apibara_sink_common::FullOptionsFromScript;
use apibara_sink_options_derive::SinkOptions;

#[derive(Default, Debug, PartialEq, SinkOptions)]
#[sink_options(tag = "test")]
struct TestOptions {
    pub foo_bar: Option<String>,
}

#[test]
pub fn test_deserialize_empty_options() {
    let json = r#"
        {
            "network": "starknet",
            "filter": {
                "header": { "weak": false }
            },
            "sinkType": "test"
        }
        "#;

    let options = serde_json::from_str::<FullOptionsFromScript<TestOptions>>(json).unwrap();
    assert_eq!(options.sink, TestOptions { foo_bar: None });
}

#[test]
pub fn test_deserialize_non_empty_options() {
    let json = r#"
        {
            "network": "starknet",
            "filter": {
                "header": { "weak": false }
            },
            "sinkType": "test",
            "sinkOptions": {
                "fooBar": "bar"
            }
        }
        "#;

    let options = serde_json::from_str::<FullOptionsFromScript<TestOptions>>(json).unwrap();
    assert_eq!(
        options.sink,
        TestOptions {
            foo_bar: Some("bar".to_string())
        }
    );
}

#[test]
pub fn test_deserialize_invalid_sink_type() {
    let json = r#"
        {
            "network": "starknet",
            "filter": {
                "header": { "weak": false }
            },
            "sinkType": "invalid-sink"
        }
        "#;

    let res = serde_json::from_str::<FullOptionsFromScript<TestOptions>>(json);
    assert!(res.is_err());
}
