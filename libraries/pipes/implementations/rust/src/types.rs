// Example code that deserializes and serializes the model.
// extern crate serde;
// #[macro_use]
// extern crate serde_derive;
// extern crate serde_json;
//
// use generated_module::AssetCheckSeverity;
//
// fn main() {
//     let json = r#"{"answer": 42}"#;
//     let model: AssetCheckSeverity = serde_json::from_str(&json).unwrap();
// }

use serde::{Deserialize, Serialize};
use std::collections::HashMap;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum AssetCheckSeverity {
    #[serde(rename = "ERROR")]
    Error,

    #[serde(rename = "WARN")]
    Warn,
}

/// The serializable data passed from the orchestration process to the external process. This
/// gets wrapped in a PipesContext.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PipesContextData {
    pub asset_keys: Option<Vec<String>>,

    pub code_version_by_asset_key: Option<HashMap<String, Option<String>>>,

    pub extras: Option<HashMap<String, Option<serde_json::Value>>>,

    pub job_name: Option<String>,

    pub partition_key: Option<String>,

    pub partition_key_range: Option<PartitionKeyRange>,

    pub partition_time_window: Option<PartitionTimeWindow>,

    pub provenance_by_asset_key: Option<HashMap<String, Option<ProvenanceByAssetKey>>>,

    pub retry_number: i64,

    pub run_id: String,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PartitionKeyRange {
    pub end: Option<String>,

    pub start: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PartitionTimeWindow {
    pub end: Option<String>,

    pub start: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ProvenanceByAssetKey {
    pub code_version: Option<String>,

    pub input_data_versions: Option<HashMap<String, String>>,

    pub is_user_provided: Option<bool>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PipesException {
    /// exception that explicitly led to this exception
    pub cause: Box<Option<PipesExceptionClass>>,

    /// exception that being handled when this exception was raised
    pub context: Box<Option<ContextClass>>,

    pub message: Option<String>,

    /// class name of Exception object
    pub name: Option<String>,

    pub stack: Option<Vec<String>>,
}

/// exception that being handled when this exception was raised
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ContextClass {
    /// exception that explicitly led to this exception
    pub cause: Box<Option<PipesExceptionClass>>,

    /// exception that being handled when this exception was raised
    pub context: Box<Option<ContextClass>>,

    pub message: Option<String>,

    /// class name of Exception object
    pub name: Option<String>,

    pub stack: Option<Vec<String>>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PipesExceptionClass {
    /// exception that explicitly led to this exception
    pub cause: Box<Option<PipesExceptionClass>>,

    /// exception that being handled when this exception was raised
    pub context: Box<Option<ContextClass>>,

    pub message: Option<String>,

    /// class name of Exception object
    pub name: Option<String>,

    pub stack: Option<Vec<String>>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum PipesLogLevel {
    #[serde(rename = "CRITICAL")]
    Critical,

    #[serde(rename = "DEBUG")]
    Debug,

    #[serde(rename = "ERROR")]
    Error,

    #[serde(rename = "INFO")]
    Info,

    #[serde(rename = "WARNING")]
    Warning,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PipesMessage {
    /// The version of the Dagster Pipes protocol
    #[serde(rename = "__dagster_pipes_version")]
    pub dagster_pipes_version: String,

    /// Event type
    pub method: Method,

    /// Event parameters
    pub params: Option<HashMap<String, Option<serde_json::Value>>>,
}

/// Event type
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Method {
    Closed,

    Log,

    Opened,

    #[serde(rename = "report_asset_check")]
    ReportAssetCheck,

    #[serde(rename = "report_asset_materialization")]
    ReportAssetMaterialization,

    #[serde(rename = "report_custom_message")]
    ReportCustomMessage,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PipesMetadataValue {
    pub raw_value: Option<RawValue>,

    #[serde(rename = "type")]
    pub pipes_metadata_value_type: Option<Type>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Type {
    Asset,

    Bool,

    #[serde(rename = "dagster_run")]
    DagsterRun,

    Float,

    #[serde(rename = "__infer__")]
    Infer,

    Int,

    Job,

    Json,

    Md,

    Notebook,

    Null,

    Path,

    Text,

    Timestamp,

    Url,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(untagged)]
pub enum RawValue {
    AnythingArray(Vec<Option<serde_json::Value>>),

    AnythingMap(HashMap<String, Option<serde_json::Value>>),

    Bool(bool),

    Double(f64),

    Integer(i64),

    String(String),
}
