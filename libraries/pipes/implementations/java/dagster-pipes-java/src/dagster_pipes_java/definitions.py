from dagster import (
    AssetExecutionContext,
    Definitions,
    MaterializeResult,
    PipesSubprocessClient,
    asset,
)
import subprocess

from pathlib import Path

ROOT_DIR = Path(__file__).parent.parent.parent

CLASS_PATH = ROOT_DIR / "build/classes/java/main/pipes/PipesMappingParamsLoader.class"


@asset(description="An asset which is computed using an external Java script")
def java_asset(
    context: AssetExecutionContext, pipes_subprocess_client: PipesSubprocessClient
) -> MaterializeResult:
    subprocess.run(["./gradlew", "build"], check=True)

    return pipes_subprocess_client.run(
        context=context,
        command=["java", "-jar", "bbuild/libs/dagster-pipes-java-1.0-SNAPSHOT.jar"],
    ).get_materialize_result()


defs = Definitions(
    assets=[java_asset],
    resources={"pipes_subprocess_client": PipesSubprocessClient()},
)
