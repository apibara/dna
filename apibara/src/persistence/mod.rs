//! Common types used when persisting data.
mod mongo;

use std::{fmt::Display, str::FromStr};

use anyhow::{Error, Result};
use once_cell::sync::Lazy;
use regex::Regex;
use serde::{Deserialize, Serialize};

pub use self::mongo::MongoPersistence;

// The id must be kebab-case
// must start with an alphabetic character
// can contain alphanumeric characters and -
// must end with an alphanumeric character
static ID_REGEX: Lazy<Regex> =
    Lazy::new(|| Regex::new("[[:alpha:]]([[:alnum:]]|-)*[[:alnum:]]").unwrap());

#[derive(Debug, Clone, Serialize, Deserialize)]
/// Unique id that is also human readable.
pub struct Id(String);

impl Id {
    /// Parse and create a new Id.
    pub fn new(s: &str) -> Result<Self> {
        if ID_REGEX.is_match(s) {
            return Ok(Id(s.to_string()));
        }
        Err(Error::msg("invalid Id"))
    }

    /// Turn the id into a string.
    pub fn into_string(self) -> String {
        self.0
    }

    /// Returns the indexer id a `&str`.
    pub fn to_str(&self) -> &str {
        &self.0
    }
}

impl Display for Id {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl FromStr for Id {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Id::new(s)
    }
}
