pub fn normalize_prefix(prefix: Option<String>) -> String {
    let prefix = prefix.unwrap_or_default();
    if prefix.ends_with("/") || prefix.is_empty() {
        prefix
    } else {
        format!("{}/", prefix)
    }
}
