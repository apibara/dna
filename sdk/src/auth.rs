use snafu::Snafu;
use tonic::metadata::errors::InvalidMetadataValue;

/// The default environment variable name is `DNA_TOKEN`.
pub const DEFAULT_ENV_VAR_NAME: &str = "DNA_TOKEN";

/// The error returned by implementors of the [BearerTokenProvider] trait.
#[derive(Snafu, Debug)]
pub enum BearerTokenError {
    /// The bearer token format is invalid and cannot be serialized to an header.
    #[snafu(display("Invalid bearer token format"))]
    InvalidBearerTokenFormat { source: InvalidMetadataValue },
    /// An error external to this crate.
    ///
    /// Provide a user-friendly message and the source error.
    #[snafu(display("{message}"))]
    External {
        message: String,
        source: Box<dyn std::error::Error + Send + Sync>,
    },
}

/// An object that provides a bearer token to authenticate with the DNA server.
pub trait BearerTokenProvider: Send + Sync {
    /// Get the bearer token.
    ///
    /// Notice that this method is _not_ async. If you need to fetch the token
    /// asynchronously, you should do it at initialization time.
    fn get_token(&self) -> Result<String, BearerTokenError>;
}

/// A [BearerTokenProvider] that provides a static bearer token.
#[derive(Debug, Clone)]
pub struct StaticBearerToken {
    token: String,
}

/// A [BearerTokenProvider] that provides a bearer token from an environment variable.
///
/// By default, the environment variable name is `DNA_TOKEN`.
#[derive(Debug, Clone)]
pub struct BearerTokenFromEnv {
    env_var_name: String,
}

impl StaticBearerToken {
    /// Create a new [StaticBearerToken] with the given token.
    pub fn new(token: String) -> Self {
        StaticBearerToken { token }
    }
}

impl BearerTokenProvider for StaticBearerToken {
    fn get_token(&self) -> Result<String, BearerTokenError> {
        Ok(self.token.clone())
    }
}

impl Default for StaticBearerToken {
    fn default() -> Self {
        StaticBearerToken::new(DEFAULT_ENV_VAR_NAME.to_string())
    }
}

impl BearerTokenProvider for BearerTokenFromEnv {
    fn get_token(&self) -> Result<String, BearerTokenError> {
        let token =
            std::env::var(&self.env_var_name).map_err(|err| BearerTokenError::External {
                message: format!(
                    "Failed to get bearer token from '{}' env variable",
                    &self.env_var_name
                ),
                source: Box::new(err),
            })?;
        Ok(token)
    }
}

impl Default for BearerTokenFromEnv {
    fn default() -> Self {
        BearerTokenFromEnv::new_with_var_name(DEFAULT_ENV_VAR_NAME.to_string())
    }
}

impl BearerTokenFromEnv {
    /// Create a new [BearerTokenFromEnv] with the default environment variable name.
    pub fn new() -> Self {
        Self::default()
    }

    /// Create a new [BearerTokenFromEnv] with the given environment variable name.
    pub fn new_with_var_name(env_var_name: String) -> Self {
        BearerTokenFromEnv { env_var_name }
    }
}

impl From<InvalidMetadataValue> for BearerTokenError {
    fn from(source: InvalidMetadataValue) -> Self {
        BearerTokenError::InvalidBearerTokenFormat { source }
    }
}
