use base64::prelude::*;
use flate2::read::ZlibDecoder;
use serde::de::DeserializeOwned;
use serde_json::{Map, Value};
use std::fmt;
use std::io::Read;
use thiserror::Error;

pub const DAGSTER_PIPES_CONTEXT_ENV_VAR: &str = "DAGSTER_PIPES_CONTEXT";
pub const DAGSTER_PIPES_MESSAGES_ENV_VAR: &str = "DAGSTER_PIPES_MESSAGES";

/// Load params passed from the orchestration process by the context injector and
/// message reader. These params are used to respectively bootstrap implementations of
/// [`LoadContext`](crate::LoadContext) and [`PipesMessageWriter`](crate::MessageWriter).
pub trait LoadParams {
    /// Whether or not this process has been provided with provided with information
    /// to create a `PipesContext` or should instead return a mock.
    fn is_dagster_pipes_process(&self) -> bool;
    /// Load params passed by the orchestration-side context injector.
    fn load_context_params(&self) -> Result<Map<String, Value>, ParamsError>;
    /// Load params passed by the orchestration-side message reader.
    fn load_message_params(&self) -> Result<Map<String, Value>, ParamsError>;
}

#[derive(Debug, Error)]
#[error("{} parameter \"{}\" is {}", .origin, .param, .source)]
pub struct ParamsError {
    pub param: String,
    pub origin: ParamOrigin,
    pub source: ParamsErrorKind,
}

#[derive(Debug)]
#[non_exhaustive]
pub enum ParamOrigin {
    Cli,
    EnvVar,
}

impl fmt::Display for ParamOrigin {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Cli => write!(f, "cli"),
            Self::EnvVar => write!(f, "env var"),
        }
    }
}

#[derive(Debug, Error)]
#[non_exhaustive]
pub enum ParamsErrorKind {
    #[error("not present")]
    #[non_exhaustive]
    NotPresent,

    #[error("invalid")]
    #[non_exhaustive]
    Invalid {
        source: Box<dyn std::error::Error + Send + Sync>,
    },
}

impl From<std::env::VarError> for ParamsErrorKind {
    fn from(value: std::env::VarError) -> Self {
        match value {
            std::env::VarError::NotPresent => Self::NotPresent,
            std::env::VarError::NotUnicode(_) => Self::Invalid {
                source: Box::new(value),
            },
        }
    }
}

impl From<base64::DecodeError> for ParamsErrorKind {
    fn from(value: base64::DecodeError) -> Self {
        Self::Invalid {
            source: Box::new(value),
        }
    }
}

impl From<std::io::Error> for ParamsErrorKind {
    fn from(value: std::io::Error) -> Self {
        Self::Invalid {
            source: Box::new(value),
        }
    }
}

impl From<serde_json::Error> for ParamsErrorKind {
    fn from(value: serde_json::Error) -> Self {
        Self::Invalid {
            source: Box::new(value),
        }
    }
}

#[derive(Debug, Default)]
pub struct EnvVarLoader;

impl EnvVarLoader {
    pub fn new() -> Self {
        Self
    }
}

impl LoadParams for EnvVarLoader {
    fn is_dagster_pipes_process(&self) -> bool {
        std::env::var(DAGSTER_PIPES_CONTEXT_ENV_VAR).is_ok()
    }

    fn load_context_params(&self) -> Result<Map<String, Value>, ParamsError> {
        let param = std::env::var(DAGSTER_PIPES_CONTEXT_ENV_VAR).map_err(|source| ParamsError {
            param: DAGSTER_PIPES_CONTEXT_ENV_VAR.to_string(),
            origin: ParamOrigin::Cli,
            source: ParamsErrorKind::from(source),
        })?;
        let result = decode_env_var(&param).map_err(|source| ParamsError {
            param: DAGSTER_PIPES_CONTEXT_ENV_VAR.to_string(),
            origin: ParamOrigin::Cli,
            source,
        })?;
        Ok(result)
    }

    fn load_message_params(&self) -> Result<Map<String, Value>, ParamsError> {
        let param =
            std::env::var(DAGSTER_PIPES_MESSAGES_ENV_VAR).map_err(|source| ParamsError {
                param: DAGSTER_PIPES_MESSAGES_ENV_VAR.to_string(),
                origin: ParamOrigin::Cli,
                source: ParamsErrorKind::from(source),
            })?;
        let result = decode_env_var(&param).map_err(|source| ParamsError {
            param: DAGSTER_PIPES_MESSAGES_ENV_VAR.to_string(),
            origin: ParamOrigin::Cli,
            source,
        })?;
        Ok(result)
    }
}

// translation of
// `<https://github.com/dagster-io/dagster/blob/258d9ca0db/python_modules/dagster-pipes/dagster_pipes/__init__.py#L354-L367>`
fn decode_env_var<T>(param: &str) -> Result<T, ParamsErrorKind>
where
    T: DeserializeOwned,
{
    let zlib_compressed_slice = BASE64_STANDARD.decode(param)?;
    let mut decoder = ZlibDecoder::new(&zlib_compressed_slice[..]);
    let mut json_str = String::new();
    decoder.read_to_string(&mut json_str)?;
    let decoded = serde_json::from_str(&json_str)?;
    Ok(decoded)
}
