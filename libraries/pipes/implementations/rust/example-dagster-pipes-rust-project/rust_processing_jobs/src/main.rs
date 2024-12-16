use dagster_pipes_rust::types::{PipesMetadataValue, RawValue, Type};
use dagster_pipes_rust::{open_dagster_pipes, AssetCheckSeverity, DagsterPipesError};

use std::collections::HashMap;

fn main() -> Result<(), DagsterPipesError> {
    let mut context = open_dagster_pipes()?;

    let asset_metadata = HashMap::from([(
        "row_count".to_string(),
        PipesMetadataValue::new(RawValue::Integer(100), Type::Int),
    )]);
    context.report_asset_materialization("example_rust_subprocess_asset", asset_metadata)?;

    let check_metadata = HashMap::from([(
        "quality".to_string(),
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
