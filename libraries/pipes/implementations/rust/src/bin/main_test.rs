use dagster_pipes_rust::types::{PipesMetadataValue, RawValue, Type};
use dagster_pipes_rust::{open_dagster_pipes, AssetCheckSeverity, DagsterPipesError};

use std::collections::HashMap;

use clap::Parser;

#[derive(Parser)]
struct Cli {
    context: String,
    messages: String,
    #[arg(default_value_t = false)]
    env: bool,
    job_name: String,
    extras: String,
    #[arg(default_value_t = false)]
    full: bool,
    custom_payload_path: String,
    report_asset_check: String,
    report_asset_materialization: String,
    #[arg(default_value_t = false)]
    throw_error: bool,
    #[arg(default_value_t = false)]
    logging: bool,
    message_writer: String,
    context_loader: String,
}

fn main() -> Result<(), DagsterPipesError> {
    let args = Cli::parse();

    let mut context = open_dagster_pipes()?;

    let asset_metadata = HashMap::from([(
        "row_count",
        PipesMetadataValue::new(RawValue::Integer(100), Type::Int),
    )]);
    context.report_asset_materialization("example_rust_subprocess_asset", asset_metadata)?;

    let check_metadata = HashMap::from([(
        "quality",
        PipesMetadataValue::new(RawValue::Integer(100), Type::Int),
    )]);
    context.report_asset_check(
        "example_rust_subprocess_check",
        true,
        "example_rust_subprocess_asset",
        &AssetCheckSeverity::Warn,
        check_metadata,
    )?;
    Ok(())
}
