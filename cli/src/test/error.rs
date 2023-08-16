use std::io::Write;

use colored::Colorize;
use serde_json::Value;
use similar_asserts::serde_impl::Debug as SimilarAssertsDebug;
use similar_asserts::SimpleDiff;
use tracing::warn;

const TRUNCATE_THRESHOLD: usize = 1000;
const DIFF_TRUNCATE_THRESHOLD: usize = 2000;

fn truncate(s: &str, threshold: usize, write_tempfile: bool) -> String {
    if s.chars().count() <= threshold {
        return s.to_owned();
    }

    let mut full_output_hint = "".to_owned();

    if write_tempfile {
        match tempfile::Builder::new()
            .prefix("apibara-test-diff-")
            .suffix(".patch")
            .tempfile()
            .map_err(|err| err.to_string())
            .and_then(|keep| keep.keep().map_err(|err| err.to_string()))
            .and_then(|(mut file, path)| match file.write_all(s.as_bytes()) {
                Ok(_) => Ok(path),
                Err(err) => Err(err.to_string()),
            }) {
            Ok(path) => full_output_hint = format!(", full output written to `{}`", path.display()),
            Err(err) => {
                warn!(err =? err, "Cannot create tempfile to save long diff");
            }
        }
    }

    let start = &s[..threshold / 2];
    let end = &s[s.len() - threshold / 2..];

    let truncated_prefix = format!("<truncated{}>\n", full_output_hint).yellow();

    let truncated_msg = format!(
        "\n ... \n {} chars truncated{}\n ... \n",
        s.chars().count() - threshold,
        full_output_hint,
    )
    .yellow();

    format!("{truncated_prefix} {start} {truncated_msg} {end}")
}

pub fn get_assertion_error(expected_outputs: &[Value], found_outputs: &[Value]) -> String {
    let left = format!("{:#?}", SimilarAssertsDebug(&expected_outputs));
    let right = format!("{:#?}", SimilarAssertsDebug(&found_outputs));

    let left_label = "expected";
    let right_label = "found";
    let label_padding = left_label.chars().count().max(right_label.chars().count());

    let diff = SimpleDiff::from_str(left.as_str(), right.as_str(), left_label, right_label);

    let assert_fail = format!(
        "assertion failed: `({} == {})`'\n {:<label_padding$}: `{:?}`\n {:<label_padding$}: `{:?}`",
        left_label,
        right_label,
        left_label,
        truncate(&left, TRUNCATE_THRESHOLD, false),
        right_label,
        truncate(&right, TRUNCATE_THRESHOLD, false),
        label_padding = label_padding,
    )
    .bright_red();

    let diff_str = truncate(&diff.to_string(), DIFF_TRUNCATE_THRESHOLD, true);

    format!("{}\n\n{}", &assert_fail, &diff_str)
}
