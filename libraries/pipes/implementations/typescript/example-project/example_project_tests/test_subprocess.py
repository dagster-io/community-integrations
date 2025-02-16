from pathlib import Path
import subprocess

import dagster as dg

def test_example_typescript_asset():
    @dg.asset(
        check_specs=[
            dg.AssetCheckSpec(
                name="example_typescript_check",
                asset="example_typescript_asset",
            )
        ],
    )
    def example_typescript_asset(
        context: dg.AssetExecutionContext, pipes_subprocess_client: dg.PipesSubprocessClient
    ) -> dg.MaterializeResult:        
        external_script_path = dg.file_relative_path(__file__, "../external_typescript_code/")
        
        subprocess.run(["npm", "install"], 
                    cwd=external_script_path).check_returncode()
        subprocess.run(["npm", "run", "build"],
                    cwd=external_script_path).check_returncode()
        return pipes_subprocess_client.run(
            command=["node", Path(external_script_path)],
            context=context,
        ).get_materialize_result()

    result = dg.materialize_to_memory(
        assets=[example_typescript_asset],
        resources={
            "pipes_subprocess_client": dg.PipesSubprocessClient()
        },
    )
    assert result.success
    materialization = result.get_asset_materialization_events()[0]
    check = result.get_asset_check_evaluations()[0]

    assert materialization.step_materialization_data.materialization.metadata['row_count'] == dg.IntMetadataValue(100)
    assert check.check_name == "example_typescript_check" and check.passed