import time

from dagster import (
    Definitions,
    EnvVar,
    ScheduleDefinition,
    AssetSelection,
    AssetExecutionContext,
    MaterializeResult,
    asset,
)
from dagster_contrib_notdiamond import NotDiamondResource


@asset
def notdiamond_asset(context: AssetExecutionContext, notdiamond: NotDiamondResource) -> MaterializeResult:
    with notdiamond.get_client(context) as client:
        start = time.time()
        session_id, best_llm = client.model_select(
            model=["openai/gpt-4o", "openai/gpt-4o-mini"],
            messages=[{"role": "user", "content": "Say this is a test"}]
        )
        duration = time.time() - start
    return MaterializeResult(metadata={"session_id": session_id, "best_llm": str(best_llm), "routing_latency": duration})

notdiamond_schedule = ScheduleDefinition(
    name="notdiamond_schedule",
    target=AssetSelection.all(),
    cron_schedule="17 * * * *",
)

defs = Definitions(
    assets=[notdiamond_asset],
    resources={
        "notdiamond": NotDiamondResource(api_key=EnvVar("NOTDIAMOND_API_KEY")),
    },
    schedules=[notdiamond_schedule],
)
