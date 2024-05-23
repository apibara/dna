use float_cmp::approx_eq;
use serde_json::Value;

use super::snapshot::TestOptions;

const DEFAULT_FP_DECIMALS: i64 = 20;

/// Compare two outputs, taking into account test options like floating point precision.
pub fn outputs_are_equal(expected: &Value, actual: &Value, options: &TestOptions) -> bool {
    let visitor = EqualityVisitor::from_options(options);
    visitor.eq(expected, actual)
}

struct EqualityVisitor {
    floating_point_decimals: Option<i64>,
}

impl EqualityVisitor {
    pub fn from_options(options: &TestOptions) -> Self {
        Self {
            floating_point_decimals: options.floating_point_decimals,
        }
    }

    pub fn eq(&self, expected: &Value, actual: &Value) -> bool {
        match (expected, actual) {
            (Value::Number(expected), Value::Number(actual)) => self.number_eq(expected, actual),
            (Value::Array(expected), Value::Array(actual)) => self.array_eq(expected, actual),
            (Value::Object(expected), Value::Object(actual)) => self.object_eq(expected, actual),
            (expected, actual) => expected == actual,
        }
    }

    fn number_eq(&self, expected: &serde_json::Number, actual: &serde_json::Number) -> bool {
        let decimals = self.floating_point_decimals.unwrap_or(DEFAULT_FP_DECIMALS);

        match (expected.as_f64(), actual.as_f64()) {
            (Some(expected), Some(actual)) => {
                let epsilon = 10.0_f64.powi(-decimals as i32);
                approx_eq!(f64, expected, actual, epsilon = epsilon)
            }
            _ => expected == actual,
        }
    }

    fn array_eq(&self, expected: &[Value], actual: &[Value]) -> bool {
        expected.len() == actual.len()
            && expected
                .iter()
                .zip(actual.iter())
                .all(|(expected, actual)| self.eq(expected, actual))
    }

    fn object_eq(
        &self,
        expected: &serde_json::Map<String, Value>,
        actual: &serde_json::Map<String, Value>,
    ) -> bool {
        expected.len() == actual.len()
            && expected
                .iter()
                .all(|(key, expected)| match actual.get(key) {
                    Some(actual) => self.eq(expected, actual),
                    None => false,
                })
    }
}
