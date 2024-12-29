use clap::ArgAction;
use clap::Parser;
use dagster_pipes_rust::{open_dagster_pipes, DagsterPipesError};
use dagster_pipes_rust::{DAGSTER_PIPES_CONTEXT_ENV_VAR, DAGSTER_PIPES_MESSAGES_ENV_VAR};
use std::collections::HashMap;
use std::fs::File;

#[derive(Parser)]
struct Cli {
    #[arg(long)]
    context: Option<String>,
    #[arg(long)]
    messages: Option<String>,
    #[arg(
        long,
        action = ArgAction::Set,
        default_value_t = false,
        default_missing_value = "false",
        num_args=0..=1,
        require_equals = false,
    )]
    env: bool,
    #[arg(long = "job-name")]
    job_name: Option<String>,
    #[arg(long)]
    extras: Option<String>,
    #[arg(
        long,
        action = ArgAction::Set,
        default_value_t = false,
        default_missing_value = "false",
        num_args=0..=1,
        require_equals = false,
    )]
    full: bool,
    #[arg(long)]
    custom_payload_path: Option<String>,
    #[arg(long)]
    report_asset_check: Option<String>,
    #[arg(long)]
    report_asset_materialization: Option<String>,
    #[arg(
        long,
        action = ArgAction::Set,
        default_value_t = false,
        default_missing_value = "false",
        num_args=0..=1,
        require_equals = false,
    )]
    throw_error: bool,
    #[arg(
        long,
        action = ArgAction::Set,
        default_value_t = false,
        default_missing_value = "false",
        num_args=0..=1,
        require_equals = false,
    )]
    logging: bool,
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

    let mut context = open_dagster_pipes()?;

    if let Some(job_name) = args.job_name {
        assert_eq!(context.data.job_name, Some(job_name));
    }

    if let Some(extras) = args.extras {
        let file = File::open(extras).expect("extras could not be opened");
        let json: HashMap<std::string::String, std::option::Option<serde_json::Value>> =
            serde_json::from_reader(file).expect("extras could not be parsed");
        assert_eq!(context.data.extras, Some(json));
    }

    if let Some(custom_payload_path) = args.custom_payload_path {
        let file =
            File::open(custom_payload_path).expect("custom_payload_path could not be opened");
        let payload = serde_json::from_reader::<File, serde_json::Value>(file)
            .expect("custom_payload_path could not be parsed")
            .as_object()
            .expect("custom payload must be an object")
            .get("payload")
            .expect("custom payload must have a 'payload' key")
            .clone();
        context.report_custom_message(payload)?
    }

    Ok(())
}
