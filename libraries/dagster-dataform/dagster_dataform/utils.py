from datetime import datetime, timezone, timedelta

import dagster as dg
from google.cloud import dataform_v1


def get_epoch_time_ago(minutes: int) -> int:
    current_utc_datetime = datetime.now(timezone.utc)

    # Subtract x seconds from the current time
    time_ago_utc = current_utc_datetime - timedelta(minutes=minutes)

    # Convert the datetime object to an integer Unix timestamp (seconds since epoch)
    return int(time_ago_utc.timestamp())


def handle_asset_check_evaluation(action: dataform_v1.WorkflowInvocationAction):
    asset_key = action.target.name.split("_assertions")[0]
    asset_check_evaluation = dg.AssetCheckEvaluation(
        asset_key=dg.AssetKey(
            asset_key
        ),  # TODO: This is a temporary solution to get the asset key. We should use the asset key from the asset check spec. See if we can search for asset checks by name.
        check_name=action.target.name,
        passed=True if action.state.name == "SUCCEEDED" else False,
        metadata={
            "Outcome": action.state.name,
            "Error Details": action.failure_reason,
            "Assertion SQL Query": dg.MetadataValue.md(
                f"```sql\n{action.bigquery_action.sql_script}\n```"
            ),
            "Assertion Output Destination": dg.MetadataValue.url(
                f"https://console.cloud.google.com/bigquery?project={action.target.database}&p={action.target.database}&d={action.target.schema}&t={action.target.name}&page=table"
            ),
        },
    )
    return asset_check_evaluation


def empty_fn():
    "Placeholder function to satisfy the compute_fn requirement for asset check specifications"
    pass
