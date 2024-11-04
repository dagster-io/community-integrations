from typing import Tuple

from dagster import (
    AssetExecutionContext,
    Definitions,
    GraphDefinition,
    OpExecutionContext,
    asset,
    define_asset_job,
    EnvVar,
    op,
)
from dagster_contrib_notdiamond import NotDiamondResource

@op
def notdiamond_op(context: OpExecutionContext, notdiamond: NotDiamondResource) -> Tuple[str, str]:
    with notdiamond.get_client(context) as client:
        session_id, best_llm = client.model_select(
            model=["openai/gpt-4o", "openai/gpt-4o-mini"],
            messages=[{"role": "user", "content": "Say this is a test"}]
        )
    return session_id, str(best_llm)

notdiamond_op_job = GraphDefinition(name="notdiamond_op_job", node_defs=[notdiamond_op]).to_job()

@asset(compute_kind="NotDiamond")
def notdiamond_asset(context: AssetExecutionContext, notdiamond: NotDiamondResource) -> Tuple[str, str]:
    with notdiamond.get_client(context) as client:
        session_id, best_llm = client.model_select(
            model=["openai/gpt-4o", "openai/gpt-4o-mini"],
            messages=[{"role": "user", "content": "Say this is a test"}]
        )
    return session_id, str(best_llm)

notdiamond_asset_job = define_asset_job(name="notdiamond_asset_job", selection="notdiamond_asset")

defs = Definitions(
    assets=[notdiamond_asset],
    jobs=[notdiamond_asset_job, notdiamond_op_job],
    resources={
        "notdiamond": NotDiamondResource(api_key=EnvVar("NOTDIAMOND_API_KEY")),
    },
)

if __name__ == "__main__":
    result = notdiamond_op_job.execute_in_process(
        resources={"notdiamond": NotDiamondResource(api_key=EnvVar("NOTDIAMOND_API_KEY"))}
    )
    print(result.output_for_node("notdiamond_op"))