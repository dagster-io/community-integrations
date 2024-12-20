use dagster_pipes_rust::types::{PipesMetadataValue, RawValue, Type};
use dagster_pipes_rust::{open_dagster_pipes, AssetCheckSeverity, DagsterPipesError};

use std::collections::HashMap;
use clap::ArgAction;

use clap::Parser;

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
        num_args=(0..=1),
        require_equals = false,
    )]
    env: bool,
    #[arg(long="jobName")]
    job_name: String,
    #[arg(long)]
    extras: Option<String>,
    #[arg(
        long,
        action = ArgAction::Set,
        default_value_t = false,
        default_missing_value = "false",
        num_args=(0..=1),
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
        num_args=(0..=1),
        require_equals = false,
    )]
    throw_error: bool,
    #[arg(
        long,
        action = ArgAction::Set,
        default_value_t = false,
        default_missing_value = "false",
        num_args=(0..=1),
        require_equals = false,
    )]
    logging: bool,
    #[arg(long)]
    message_writer: Option<String>,
    #[arg(long)]
    context_loader: Option<String>,
}

fn main() -> Result<(), DagsterPipesError> {
    let args = Cli::parse();

    let mut context = open_dagster_pipes()?;

    Ok(())
}
