import dagster as dg
from dagster import MetadataValue
from dagster_dataform.resources import DataformRepositoryResource
import json
from datetime import datetime
import pytz
import re
from typing import Optional, List


def create_dataform_workflow_invocation_sensor(
    resource: DataformRepositoryResource,
    minutes_ago: int,
    job: Optional[dg.JobDefinition] = None,
    exclusion_patterns: Optional[List[str]] = None,
):
    """
    This function creates a sensor that polls the Dataform API for workflow invocations. It has the following capabilities:
    1. It polls the Dataform API for workflow invocations in a certain interval. This interval is defined by the sensor_minimum_interval_seconds parameter passed to the resource defintion.
    2. Every time it polls for workflow invocations, it is filtering the workflow invocations to look for invocations that have taken place minutes_ago (defined in the sensor definition) backwards in time from the current time.
    3. It iterates through the retrieved workflow invocations, checking the cursor to see if the current workflow invocation has already been processed. If it has, it will not process the workflow invocation again.
    4. Depending on the outcome state of the current workflow invocation, it will either:
        a. Create an asset materialization event if the workflow invocation succeeded.
        b. Create an asset observation event for the asset and a run request for a notification job if the workflow invocation failed.
    5. It will also update the cursor with the latest workflow invocation start time for each asset. This prevents previous workflow invocations from being processed repeatedly.

    Args:
        resource: The DataformRepositoryResource to use for the sensor.
        job: The job to use for the sensor. If not provided, the sensor will not run any jobs.
        exclusion_patterns: A list of regex patterns to use for excluding workflow invocation event targets (by asset name) from being processed by the sensor. If not provided, the sensor will not exclude any workflow invocations actions.
        minutes_ago: The number of minutes to look back for workflow invocations. This should be greater than the average time it takes for a workflow invocation to complete.
    """

    @dg.op
    def dataform_workflow_invocation_failure_notification_op(
        context: dg.OpExecutionContext,
    ) -> str:
        context.log.info("This is the placeholder for the notification job")
        return "Notification sent"

    @dg.job
    def dataform_workflow_invocation_failure_notification_job():
        dataform_workflow_invocation_failure_notification_op()

    @dg.sensor(
        minimum_interval_seconds=resource.sensor_minimum_interval_seconds,
        job=dataform_workflow_invocation_failure_notification_job
        if job is None
        else job,
    )
    def dataform_workflow_invocation_sensor(
        context: dg.SensorEvaluationContext,
    ) -> dg.SensorResult:
        # Poll the external system every 30 seconds
        # for the last time the file was modified

        if context.cursor is not None:
            deserialized_cursor = json.loads(context.cursor)
        else:
            deserialized_cursor = {}

        context.log.info(
            f"Getting latest workflow invocations from dataform repository for the last {minutes_ago} minutes"
        )
        workflow_invocations = resource.get_latest_workflow_invocations(
            minutes_ago=minutes_ago
        )

        context.log.info(f"Found {len(workflow_invocations)} workflow invocations")

        dataform_workflow_invocation_cursors = {}

        asset_events = []
        run_requests = []
        for index, workflow_invocation in enumerate(workflow_invocations):
            workflow_invocation_details = resource.query_workflow_invocation(
                workflow_invocation.name
            )

            context.log.info(
                f"Processing workflow invocation {index+1} of {len(workflow_invocations)}: {workflow_invocation.name}"
            )

            for index, action in enumerate(
                workflow_invocation_details.workflow_invocation_actions
            ):
                context.log.info(
                    f"Target Asset for action {index+1} of {len(workflow_invocation_details.workflow_invocation_actions)}: {action.target.name}, State: {action.state.name}"
                )
                asset_name = action.target.name

                # skip if the asset name matches any of the exclusion patterns
                if exclusion_patterns and not any(
                    re.match(pattern, asset_name) for pattern in exclusion_patterns
                ):
                    context.log.info(
                        f"Skipping asset {asset_name} because it matches an exclusion pattern"
                    )
                    continue

                workflow_invocation_start_time_secs = (
                    action.invocation_timing.start_time.seconds
                )

                # check if asset_name key in context.cursor
                if context.cursor is not None:
                    if asset_name in deserialized_cursor:
                        external_asset_last_updated_at_ms = float(
                            deserialized_cursor[asset_name]
                        )
                    else:
                        external_asset_last_updated_at_ms = 0
                else:
                    external_asset_last_updated_at_ms = 0

                context.log.info(
                    f"Invocation Start Time: {workflow_invocation_start_time_secs}; External asset last updated at: {external_asset_last_updated_at_ms}"
                )

                if (
                    workflow_invocation_start_time_secs
                    > external_asset_last_updated_at_ms
                ):
                    utc_datetime = datetime.fromtimestamp(
                        workflow_invocation_start_time_secs, tz=pytz.utc
                    )

                    # 2. Get the 'US/Eastern' timezone object
                    eastern = pytz.timezone("US/Eastern")

                    # 3. Convert the UTC datetime to the 'US/Eastern' timezone
                    eastern_datetime = utc_datetime.astimezone(eastern)

                    context.log.info(
                        f"Asset {asset_name} has been updated since {external_asset_last_updated_at_ms} : Invocation Start Time: {eastern_datetime}"
                    )

                    if action.state.name == "RUNNING":
                        context.log.info(f"Asset {asset_name} is still running")
                        # Don't update cursor for running workflows - they might not complete
                    elif action.state.name != "SUCCEEDED":
                        context.log.info(
                            f"Creating asset materialization for {asset_name}"
                        )

                        asset_events.append(
                            dg.AssetObservation(
                                asset_key=asset_name,
                                metadata={
                                    "Outcome": action.state.name,
                                    "Error Details": action.failure_reason,
                                    # "Invocation Timing": workflow_invocation_details.workflow_invocation_actions[0].invocation_timing,
                                    "Invocation SQL Query": MetadataValue.md(
                                        f"```sql\n{action.bigquery_action.sql_script}\n```"
                                    ),
                                    "BigQuery JobID": action.bigquery_action.job_id,
                                },
                            )
                        )

                        run_requests.append(
                            dg.RunRequest(
                                run_key=f"materialized-{asset_name}-{action.invocation_timing.start_time.seconds}"
                            )
                        )

                        # Update cursor for failed workflows - they are complete
                        dataform_workflow_invocation_cursors[asset_name] = (
                            workflow_invocation_start_time_secs
                        )
                    else:
                        context.log.info(
                            f"Asset {asset_name} has had a successful workflow invocation"
                        )
                        asset_events.append(
                            dg.AssetMaterialization(
                                asset_key=asset_name,
                                metadata={
                                    # "Invocation Timing": workflow_invocation_details.workflow_invocation_actions[0].invocation_timing,
                                    "Invocation SQL Query": MetadataValue.md(
                                        f"```sql\n{action.bigquery_action.sql_script}\n```"
                                    ),
                                    "BigQuery JobID": action.bigquery_action.job_id,
                                },
                            )
                        )

                        # Update cursor for successful workflows - they are complete
                        dataform_workflow_invocation_cursors[asset_name] = (
                            workflow_invocation_start_time_secs
                        )
                else:
                    context.log.info(
                        f"Asset {asset_name} has not been updated since {external_asset_last_updated_at_ms}"
                    )

        context.log.debug(f"Asset events length: {len(asset_events)}")

        updated_cursor = deserialized_cursor | dataform_workflow_invocation_cursors

        return dg.SensorResult(
            asset_events=asset_events,
            cursor=json.dumps(updated_cursor),
            run_requests=run_requests,
        )

    return dataform_workflow_invocation_sensor
