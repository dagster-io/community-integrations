mod context_loader;
mod params_loader;
mod types;
mod types_ext;
mod writer;

use std::collections::HashMap;

use serde_json::json;
use serde_json::Map;
use serde_json::Value;
use thiserror::Error;
use types::PipesException;

use crate::context_loader::{DefaultLoader as PipesDefaultContextLoader, PayloadErrorKind};
use crate::params_loader::{EnvVarLoader as PipesEnvVarParamsLoader, ParamsError};
use crate::types::{Method, PipesContextData, PipesMessage};
use crate::writer::message_writer::{
    get_opened_payload, DefaultWriter as PipesDefaultMessageWriter,
};
use crate::writer::message_writer_channel::MessageWriteError;

pub use crate::context_loader::LoadContext;
pub use crate::params_loader::{
    LoadParams, DAGSTER_PIPES_CONTEXT_ENV_VAR, DAGSTER_PIPES_MESSAGES_ENV_VAR,
};
pub use crate::types::{AssetCheckSeverity, PipesMetadataValue};
pub use crate::writer::message_writer::{DefaultWriter, MessageWriter};
pub use crate::writer::message_writer_channel::MessageWriterChannel;

const DAGSTER_PIPES_VERSION: &str = "0.1";

// partial translation of
// https://github.com/dagster-io/dagster/blob/258d9ca0db/python_modules/dagster-pipes/dagster_pipes/__init__.py#L859-L871
#[derive(Debug)]
pub struct PipesContext<W>
where
    W: MessageWriter,
{
    pub data: PipesContextData,
    message_channel: W::Channel,
}

impl<W> PipesContext<W>
where
    W: MessageWriter,
{
    pub fn new(
        context_data: PipesContextData,
        message_params: Map<String, Value>,
        message_writer: &W,
    ) -> Result<Self, MessageWriteError> {
        let mut message_channel = message_writer.open(message_params);
        let opened_payload = get_opened_payload(message_writer);
        let opened_message = PipesMessage::new(Method::Opened, Some(opened_payload));
        message_channel.write_message(opened_message)?;

        Ok(Self {
            data: context_data,
            message_channel,
        })
    }

    pub fn close(&mut self, exc: Option<PipesException>) -> Result<(), MessageWriteError> {
        let params = exc.map(|e| {
            HashMap::from([
                ("cause", e.cause.map(|c| json!(c))),
                ("context", e.context.map(|c| json!(c))),
                ("message", e.message.map(|m| json!(m))),
                ("name", e.name.map(|n| json!(n))),
                ("stack", e.stack.map(|s| json!(s))),
            ])
        });
        let closed_message = PipesMessage::new(Method::Closed, params);
        self.message_channel.write_message(closed_message)
    }

    pub fn report_asset_materialization(
        &mut self,
        asset_key: &str,
        metadata: HashMap<&str, PipesMetadataValue>,
        data_version: Option<&str>,
    ) -> Result<(), MessageWriteError> {
        let params: HashMap<&str, Option<serde_json::Value>> = HashMap::from([
            ("asset_key", Some(json!(asset_key))),
            ("metadata", Some(json!(metadata))),
            ("data_version", data_version.map(|version| json!(version))),
        ]);

        let msg = PipesMessage::new(Method::ReportAssetMaterialization, Some(params));
        self.message_channel.write_message(msg)
    }

    pub fn report_asset_check(
        &mut self,
        check_name: &str,
        passed: bool,
        asset_key: &str,
        severity: &AssetCheckSeverity,
        metadata: HashMap<&str, PipesMetadataValue>,
    ) -> Result<(), MessageWriteError> {
        let params: HashMap<&str, Option<serde_json::Value>> = HashMap::from([
            ("asset_key", Some(json!(asset_key))),
            ("check_name", Some(json!(check_name))),
            ("passed", Some(json!(passed))),
            ("severity", Some(json!(severity))),
            ("metadata", Some(json!(metadata))),
        ]);

        let msg = PipesMessage::new(Method::ReportAssetCheck, Some(params));
        self.message_channel.write_message(msg)
    }
}

impl<W> Drop for PipesContext<W>
where
    W: MessageWriter,
{
    fn drop(&mut self) {
        let _ = self.close(None);
    }
}

#[derive(Debug, Error)]
#[non_exhaustive]
pub enum DagsterPipesError {
    #[error("dagster pipes failed to load params: {0}")]
    #[non_exhaustive]
    ParamsLoader(#[from] ParamsError),

    #[error("dagster pipes failed to load context: {0}")]
    #[non_exhaustive]
    ContextLoader(#[from] PayloadErrorKind),

    #[error("dagster pipes failed to write message: {0}")]
    #[non_exhaustive]
    MessageWriter(#[from] MessageWriteError),
}

// partial translation of
// https://github.com/dagster-io/dagster/blob/258d9ca0db/python_modules/dagster-pipes/dagster_pipes/__init__.py#L798-L838
#[must_use]
pub fn open_dagster_pipes() -> Result<PipesContext<PipesDefaultMessageWriter>, DagsterPipesError> {
    let params_loader = PipesEnvVarParamsLoader::new();
    let context_loader = PipesDefaultContextLoader::new();
    let message_writer = PipesDefaultMessageWriter::new();

    let context_params = params_loader.load_context_params()?;
    let message_params = params_loader.load_message_params()?;

    let context_data = context_loader.load_context(context_params)?;

    let context = PipesContext::new(context_data, message_params, &message_writer)?;
    Ok(context)
}

#[cfg(test)]
mod tests {
    use rstest::rstest;
    use std::collections::HashMap;
    use std::fs;
    use tempfile::NamedTempFile;
    use writer::message_writer_channel::{DefaultChannel, FileChannel};

    use super::*;

    #[test]
    fn test_write_pipes_metadata() {
        let asset_metadata = HashMap::from([
            ("int", PipesMetadataValue::from(100)),
            ("float", PipesMetadataValue::from(100.0)),
            ("bool", PipesMetadataValue::from(true)),
            ("none", PipesMetadataValue::null()),
            ("timestamp", PipesMetadataValue::from_timestamp(1000.0)),
            ("text", PipesMetadataValue::from("hello".to_string())),
            (
                "url",
                PipesMetadataValue::from_url("http://someurl.com".to_string()),
            ),
            (
                "path",
                PipesMetadataValue::from_path("file://some/path".to_string()),
            ),
            (
                "notebook",
                PipesMetadataValue::from_notebook("notebook".to_string()),
            ),
            (
                "json_object",
                PipesMetadataValue::from(HashMap::from([(
                    "key".to_string(),
                    Some(json!("value")),
                )])),
            ),
            (
                "json_array",
                PipesMetadataValue::from(vec![Some(json!({"key": "value"}))]),
            ),
            ("md", PipesMetadataValue::from_md("## markdown".to_string())),
            (
                "dagster_run",
                PipesMetadataValue::from_dagster_run("1234".to_string()),
            ),
            (
                "asset",
                PipesMetadataValue::from_asset("some_asset".to_string()),
            ),
            ("job", PipesMetadataValue::from_job("some_job".to_string())),
        ]);

        let file = NamedTempFile::new().unwrap();
        let mut context: PipesContext<DefaultWriter> = PipesContext {
            message_channel: DefaultChannel::File(FileChannel::new(file.path().into())),
            data: PipesContextData {
                asset_keys: Some(vec!["asset1".to_string()]),
                run_id: "012345".to_string(),
                ..Default::default()
            },
        };
        context
            .report_asset_materialization("asset1", asset_metadata, Some("v1"))
            .expect("Failed to report asset materialization");

        assert_eq!(
            serde_json::from_str::<PipesMessage>(&fs::read_to_string(file.path()).unwrap())
                .unwrap(),
            PipesMessage::new(
                Method::ReportAssetMaterialization,
                Some(HashMap::from([
                    ("asset_key", Some(json!("asset1"))),
                    (
                        "metadata",
                        Some(json!({
                            "int": {
                                "raw_value": 100,
                                "type": "int"
                            },
                            "float": {
                                "raw_value": 100.0,
                                "type": "float"
                            },
                            "bool": {
                                "raw_value": true,
                                "type": "bool"
                            },
                            "none": {
                                "raw_value": null,
                                "type": "null"
                            },
                            "timestamp": {
                                "raw_value": 1000.0,
                                "type": "timestamp"
                            },
                            "text": {
                                "raw_value": "hello",
                                "type": "text"
                            },
                            "url": {
                                "raw_value": "http://someurl.com",
                                "type": "url"
                            },
                            "path": {
                                "raw_value": "file://some/path",
                                "type": "path"
                            },
                            "notebook": {
                                "raw_value": "notebook",
                                "type": "notebook"
                            },
                            "json_object": {
                                "raw_value": {"key": "value"},
                                "type": "json"
                            },
                            "json_array": {
                                "raw_value": [{"key": "value"}],
                                "type": "json"
                            },
                            "md": {
                                "raw_value": "## markdown",
                                "type": "md"
                            },
                            "dagster_run": {
                                "raw_value": "1234",
                                "type": "dagster_run"
                            },
                            "asset": {
                                "raw_value": "some_asset",
                                "type": "asset"
                            },
                            "job": {
                                "raw_value": "some_job",
                                "type": "job"
                            }
                        }))
                    ),
                    ("data_version", Some(json!("v1"))),
                ])),
            )
        );
    }

    #[rstest]
    #[case(
        Some(PipesException {
            cause: Box::new(None),
            context: Box::new(None),
            message: Some("error".to_string()),
            name: Some("Error".to_string()),
            stack: Some(vec!["line1".to_string(), "line2".to_string()]),
        }),
        json!({
            "__dagster_pipes_version": "0.1",
            "method": "closed",
            "params": {
                "cause": null,
                "context": null,
                "message": "error",
                "name": "Error",
                "stack": ["line1", "line2"]
            },
        })
    )]
    #[case(
        None,
        json!({
            "__dagster_pipes_version": "0.1",
            "method": "closed",
            "params": null,
        })
    )]
    fn test_close_pipes_context(
        #[case] exc: Option<PipesException>,
        #[case] expected_message: serde_json::Value,
    ) {
        let file = NamedTempFile::new().unwrap();
        let mut context: PipesContext<DefaultWriter> = PipesContext {
            message_channel: DefaultChannel::File(FileChannel::new(file.path().into())),
            data: PipesContextData {
                asset_keys: Some(vec!["asset1".to_string()]),
                run_id: "012345".to_string(),
                ..Default::default()
            },
        };
        context.close(exc).expect("Failed to close context");
        assert_eq!(
            serde_json::from_str::<serde_json::Value>(&fs::read_to_string(file.path()).unwrap())
                .unwrap(),
            expected_message
        );
    }

    #[test]
    fn test_close_pipes_context_when_out_of_scope() {
        let file = NamedTempFile::new().unwrap();
        {
            let _: PipesContext<DefaultWriter> = PipesContext {
                message_channel: DefaultChannel::File(FileChannel::new(file.path().into())),
                data: PipesContextData {
                    asset_keys: Some(vec!["asset1".to_string()]),
                    run_id: "012345".to_string(),
                    ..Default::default()
                },
            };
        }
        assert_eq!(
            serde_json::from_str::<serde_json::Value>(&fs::read_to_string(file.path()).unwrap())
                .unwrap(),
            json!({
                "__dagster_pipes_version": "0.1",
                "method": "closed",
                "params": null,
            })
        );
    }
}
