from dagster import (
    AssetMaterialization,
    Field,
    In,
    MetadataValue,
    Noneable,
    Nothing,
    Out,
    Output,
    op,
)

from .resources import DEFAULT_POLL_INTERVAL
from .types import HexOutput


@op(
    required_resource_keys={"hex"},
    ins={"start_after": In(Nothing)},
    out=Out(
        HexOutput,
        description="Parsed json dictionary representing the details of the Hex"
        "project after the run successfully completes.",
    ),
    config_schema={
        "project_id": Field(
            str, is_required=True, description="The Project ID that this op will run."
        ),
        "inputs": Field(
            Noneable(dict),
            is_required=False,
            default_value=None,
            description="Additional inputs for the Hex Project",
        ),
        "notifications": Field(
            Noneable(list),
            is_required=False,
            default_value=None,
            description="A list of notification details that will be delivered "
            + "once a project run completes",
        ),
        "update_cache": Field(
            bool,
            is_required=False,
            default_value=False,
            description="When true, this run will update the cached state of the "
            "published app with the latest run results.",
        ),
        "kill_on_timeout": Field(
            bool,
            is_required=False,
            default_value=True,
            description="Whether to kill a running project if the poll timeout is "
            "reached before the run completes.",
        ),
        "poll_interval": Field(
            float,
            default_value=DEFAULT_POLL_INTERVAL,
            description="The time (in seconds) that will be waited between successive "
            "polls.",
        ),
        "poll_timeout": Field(
            Noneable(float),
            default_value=None,
            description="The maximum time that will waited before this operation is "
            "timed out. By default, this will never time out.",
        ),
    },
    tags={"kind": "hex"},
)
def hex_project_op(context):
    """
    Executes a Hex project for a given `project_id`, and polls until that run
    completes, raising an error if it is unsuccessful. It outputs a StatusResponse
    which contains the details of the Hex project after the run completes.
    """

    hex_output: HexOutput = context.resources.hex.run_and_poll(
        project_id=context.op_config["project_id"],
        inputs=context.op_config["inputs"],
        notifications=context.op_config["notifications"],
        update_cache=context.op_config["update_cache"],
        kill_on_timeout=context.op_config["kill_on_timeout"],
        poll_interval=context.op_config["poll_interval"],
        poll_timeout=context.op_config["poll_timeout"],
    )
    asset_name = ["hex", hex_output.run_response["projectId"]]

    context.log_event(
        AssetMaterialization(
            asset_name,
            description="Hex Project Details",
            metadata={
                "run_url": MetadataValue.url(hex_output.run_response["runUrl"]),
                "run_status_url": MetadataValue.url(
                    hex_output.run_response["runStatusUrl"]
                ),
                "trace_id": MetadataValue.text(hex_output.run_response["traceId"]),
                "run_id": MetadataValue.text(hex_output.run_response["runId"]),
                "elapsed_time": MetadataValue.int(
                    hex_output.status_response["elapsedTime"]
                ),
            },
        )
    )
    yield Output(hex_output)
