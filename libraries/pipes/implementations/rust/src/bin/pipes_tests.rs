use clap::Parser;
use dagster_pipes_rust::{
    open_dagster_pipes, DagsterPipesError, MessageWriter, PipesContext, PipesMetadataValue,
};
use dagster_pipes_rust::{DAGSTER_PIPES_CONTEXT_ENV_VAR, DAGSTER_PIPES_MESSAGES_ENV_VAR};
use serde_json::json;
use std::collections::HashMap;
use std::fs::File;

#[derive(Parser)]
struct Cli {
    #[arg(long)]
    test_name: String,
    #[arg(long)]
    context: Option<String>,
    #[arg(long)]
    messages: Option<String>,
    #[arg(long)]
    job_name: Option<String>,
    #[arg(long)]
    extras: Option<String>,
    #[arg(long)]
    custom_payload: Option<String>,
    #[arg(long)]
    report_asset_check: Option<String>,
    #[arg(long)]
    report_asset_materialization: Option<String>,
    #[arg(long)]
    message_writer: Option<String>,
    #[arg(long)]
    context_loader: Option<String>,
}

pub fn main() -> Result<(), DagsterPipesError> {
    let args = Cli::parse();
    if let Some(context) = args.context {
        std::env::set_var(DAGSTER_PIPES_CONTEXT_ENV_VAR, &context);
    }
    if let Some(messages) = args.messages {
        std::env::set_var(DAGSTER_PIPES_MESSAGES_ENV_VAR, &messages);
    }

    let context = open_dagster_pipes()?;

    if let Some(job_name) = args.job_name {
        assert_eq!(context.data.job_name, Some(job_name));
    }

    if let Some(extras) = args.extras {
        let file = File::open(extras).expect("extras could not be opened");
        let json: HashMap<std::string::String, std::option::Option<serde_json::Value>> =
            serde_json::from_reader(file).expect("extras could not be parsed");
        assert_eq!(context.data.extras, Some(json));
    }

    match args.test_name.as_str() {
        "test_message_log" => test_message_log(context),
        "test_message_report_custom_message" => {
            test_message_report_custom_message(context, args.custom_payload)
        }
        "test_message_report_asset_materialization" => {
            test_message_report_asset_materialization(context, args.report_asset_materialization)
        }
        _ => Ok(()),
    }?;
    Ok(())
}

fn test_message_log<W>(mut context: PipesContext<W>) -> Result<(), DagsterPipesError>
where
    W: MessageWriter,
{
    context.logger.debug("Debug message")?;
    context.logger.info("Info message")?;
    context.logger.warning("Warning message")?;
    context.logger.error("Error message")?;
    context.logger.critical("Critical message")?;
    Ok(())
}

fn test_message_report_custom_message<W>(
    mut context: PipesContext<W>,
    custom_payload: Option<String>,
) -> Result<(), DagsterPipesError>
where
    W: MessageWriter,
{
    let custom_payload = custom_payload.expect("custom_payload is required");
    let file = File::open(custom_payload).expect("custom_payload_path could not be opened");
    let payload = serde_json::from_reader::<File, serde_json::Value>(file)
        .expect("custom_payload_path could not be parsed")
        .as_object()
        .expect("custom payload must be an object")
        .get("payload")
        .expect("custom payload must have a 'payload' key")
        .clone();
    context.report_custom_message(payload)?;
    Ok(())
}

fn test_message_report_asset_materialization<W>(
    mut context: PipesContext<W>,
    report_asset_materialization: Option<String>,
) -> Result<(), DagsterPipesError>
where
    W: MessageWriter,
{
    let report_asset_materialization =
        report_asset_materialization.expect("report_asset_materialization is required");
    let file = File::open(report_asset_materialization)
        .expect("report_asset_materialization could not be opened");
    let json: serde_json::Value =
        serde_json::from_reader(file).expect("report_asset_materialization could not be parsed");
    let data_version = json.get("dataVersion").map(|v| v.as_str().expect("dataVersion must be a string"));
    let asset_key = json.get("assetKey").map(|v| v.as_str().expect("assetKey must be a string"));

    context.report_asset_materialization(asset_key, build_asset_metadata(), data_version)?;
    Ok(())
}

fn build_asset_metadata() -> HashMap<&'static str, PipesMetadataValue> {
    HashMap::from([
        ("float", PipesMetadataValue::from(0.1)),
        ("int", PipesMetadataValue::from(1)),
        ("text", PipesMetadataValue::from("hello".to_string())),
        (
            "notebook",
            PipesMetadataValue::from_notebook("notebook.ipynb".to_string()),
        ),
        (
            "md",
            PipesMetadataValue::from_md("**markdown**".to_string()),
        ),
        ("bool_true", PipesMetadataValue::from(true)),
        ("bool_false", PipesMetadataValue::from(false)),
        (
            "asset",
            PipesMetadataValue::from_asset("foo/bar".to_string()),
        ),
        (
            "dagster_run",
            PipesMetadataValue::from_dagster_run(
                "db892d7f-0031-4747-973d-22e8b9095d9d".to_string(),
            ),
        ),
        ("null", PipesMetadataValue::null()),
        (
            "url",
            PipesMetadataValue::from_url("https://dagster.io".to_string()),
        ),
        (
            "path",
            PipesMetadataValue::from_path("/dev/null".to_string()),
        ),
        (
            "json",
            PipesMetadataValue::from(HashMap::from([
                ("quux".to_string(), Some(json!({"a": 1, "b": 2}))),
                ("baz".to_string(), Some(json!(1))),
                ("foo".to_string(), Some(json!("bar"))),
                ("corge".to_string(), None),
                ("qux".to_string(), Some(json!([1, 2, 3]))),
            ])),
        ),
    ])
}
