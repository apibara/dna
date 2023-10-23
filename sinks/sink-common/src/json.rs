use serde_json::Value;

pub trait ValueExt {
    /// If `Self` is an array of objects, returns the associated vector of values.
    /// Returns `None` otherwise.
    fn as_array_of_objects(&self) -> Option<&Vec<Value>>;
}

impl ValueExt for Value {
    fn as_array_of_objects(&self) -> Option<&Vec<Value>> {
        let Some(values) = self.as_array() else {
            return None;
        };

        if values.iter().all(|v| v.is_object()) {
            Some(values)
        } else {
            None
        }
    }
}
