import subprocess
from pathlib import Path
import dagster as dg

@dg.asset(
    check_specs=[
        dg.AssetCheckSpec(
            name="example_typescript_check",
            asset="example_typescript_asset"
        )
    ]
)
def example_typescript_asset(
    context: dg.AssetExecutionContext, 
    pipes_subprocess_client: dg.PipesSubprocessClient
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


defs = dg.Definitions(
    assets=[example_typescript_asset],
    resources={
        "pipes_subprocess_client": dg.PipesSubprocessClient()
    },
)
