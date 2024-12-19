mod context_loader;
mod params_loader;
pub mod types;
mod types_ext;
mod writer;

use std::collections::HashMap;

use serde_json::json;
use serde_json::Map;
use serde_json::Value;
use thiserror::Error;

use crate::context_loader::DefaultLoader as PipesDefaultContextLoader;
pub use crate::context_loader::LoadContext;
use crate::context_loader::PayloadErrorKind;
use crate::params_loader::EnvVarLoader as PipesEnvVarParamsLoader;
pub use crate::params_loader::LoadParams;
use crate::params_loader::ParamsError;
pub use crate::types::{
    AssetCheckSeverity, Method, PipesContextData, PipesMessage, PipesMetadataValue,
};
use crate::writer::message_writer::get_opened_payload;
use crate::writer::message_writer::DefaultWriter as PipesDefaultMessageWriter;
pub use crate::writer::message_writer::{DefaultWriter, MessageWriter};
pub use crate::writer::message_writer_channel::{DefaultChannel, FileChannel};
use crate::writer::message_writer_channel::{MessageWriteError, MessageWriterChannel};

const DAGSTER_PIPES_VERSION: &str = "0.1";

impl PipesMetadataValue {
    pub fn new(raw_value: types::RawValue, pipes_metadata_value_type: types::Type) -> Self {
        Self {
            raw_value: Some(raw_value),
            pipes_metadata_value_type: Some(pipes_metadata_value_type),
        }
    }
}

// partial translation of
// https://github.com/dagster-io/dagster/blob/258d9ca0db/python_modules/dagster-pipes/dagster_pipes/__init__.py#L859-L871
#[derive(Debug)]
pub struct PipesContext<W>
where
    W: MessageWriter,
{
    data: PipesContextData,
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

    pub fn report_asset_materialization(
        &mut self,
        asset_key: &str,
        metadata: HashMap<&str, PipesMetadataValue>,
    ) -> Result<(), MessageWriteError> {
        let params: HashMap<&str, Option<serde_json::Value>> = HashMap::from([
            ("asset_key", Some(json!(asset_key))),
            ("metadata", Some(json!(metadata))),
            ("data_version", None), // TODO - support data versions
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
    use std::collections::HashMap;
    use std::fs;
    use tempfile::NamedTempFile;

    use super::*;

    #[test]
    fn test_write_pipes_metadata() {
        let asset_metadata = HashMap::from([
            (
                "text",
                PipesMetadataValue::new(
                    types::RawValue::String("hello".to_string()),
                    types::Type::Text,
                ),
            ),
            (
                "url",
                PipesMetadataValue::new(
                    types::RawValue::String("http://someurl.com".to_string()),
                    types::Type::Url,
                ),
            ),
            (
                "path",
                PipesMetadataValue::new(
                    types::RawValue::String("file://some/path".to_string()),
                    types::Type::Path,
                ),
            ),
            (
                "notebook",
                PipesMetadataValue::new(
                    types::RawValue::String("notebook".to_string()),
                    types::Type::Notebook,
                ),
            ),
            (
                "json_object",
                PipesMetadataValue::new(
                    types::RawValue::AnythingMap(HashMap::from([(
                        "key".to_string(),
                        Some(json!("value")),
                    )])),
                    types::Type::Json,
                ),
            ),
            (
                "json_array",
                PipesMetadataValue::new(
                    types::RawValue::AnythingArray(vec![Some(json!({"key": "value"}))]),
                    types::Type::Json,
                ),
            ),
            (
                "md",
                PipesMetadataValue::new(
                    types::RawValue::String("## markdown".to_string()),
                    types::Type::Md,
                ),
            ),
            (
                "dagster_run",
                PipesMetadataValue::new(
                    types::RawValue::String("1234".to_string()),
                    types::Type::DagsterRun,
                ),
            ),
            (
                "asset",
                PipesMetadataValue::new(
                    types::RawValue::String("some_asset".to_string()),
                    types::Type::Asset,
                ),
            ),
            (
                "job",
                PipesMetadataValue::new(
                    types::RawValue::String("some_job".to_string()),
                    types::Type::Job,
                ),
            ),
            (
                "timestamp",
                PipesMetadataValue::new(
                    types::RawValue::String("2012-04-23T18:25:43.511Z".to_string()),
                    types::Type::Timestamp,
                ),
            ),
            (
                "int",
                PipesMetadataValue::new(types::RawValue::Integer(100), types::Type::Int),
            ),
            (
                "float",
                PipesMetadataValue::new(types::RawValue::Double(100.0), types::Type::Float),
            ),
            (
                "bool",
                PipesMetadataValue::new(types::RawValue::Bool(true), types::Type::Bool),
            ),
            (
                "none",
                PipesMetadataValue {
                    raw_value: None,
                    pipes_metadata_value_type: None,
                },
            ),
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
            .report_asset_materialization("asset1", asset_metadata)
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
                            },
                            "timestamp": {
                                "raw_value": "2012-04-23T18:25:43.511Z",
                                "type": "timestamp"
                            },
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
                                "type": null
                            }
                        }))
                    ),
                    ("data_version", None),
                ])),
            )
        );
    }
}
