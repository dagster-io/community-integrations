from packaging import version
from dagster import (
    __version__,
    AssetsDefinition,
    asset,
    AssetExecutionContext,
    AssetMaterialization,
    MetadataValue,
    Output,
)
from .resources import HexResource

if version.parse(__version__) >= version.parse("1.10.0"):
    from dagster._annotations import preview  # pyright: ignore[reportAttributeAccessIssue]
else:
    from dagster._annotations import experimental as preview  # pyright: ignore[reportAttributeAccessIssue]


@preview
def build_hex_asset(
    project_id: str,
    resource: HexResource,
    inputs: dict | None = None,
    update_cache: bool = False,
    notifications: list | None = None,
    deps: list | None = None,
    tags: dict | None = None,
) -> AssetsDefinition:
    """
    Returns the AssetDefinition for a Hex Project

    Args:
        project_id: The Hex project id
        resource: The HexResource
        inputs: The inputs for the asset
        update_cache: Whether to update the cache
        notifications: The notifications for the asset
        deps: The dependencies for the asset
        tags: The tags for the asset
    Returns:
        AssetsDefinition: The AssetDefinition for the Hex Project
    """

    project_name = resource.get_project(project_id)["title"]
    metadata = resource.get_project_details(project_id)

    @asset(
        name=f"{project_name}__{project_id.replace('-', '_')}",
        required_resource_keys={"hex"},
        deps=deps,
        metadata=metadata,
        tags=tags,
        kinds={"hex"},
    )
    def hex_asset(context: AssetExecutionContext):
        output = context.resources.hex.run_and_poll(
            project_id=project_id,
            inputs=inputs,
            update_cache=update_cache,
            notifications=notifications,
        )
        asset_key = ["hex", project_name, project_id]
        context.log_event(
            AssetMaterialization(
                asset_key,
                description="Hex Project Details",
                metadata={
                    "run_url": MetadataValue.url(output.run_response["runUrl"]),
                    "run_status_url": MetadataValue.url(
                        output.run_response["runStatusUrl"]
                    ),
                    "trace_id": MetadataValue.text(output.run_response["traceId"]),
                    "run_id": MetadataValue.text(output.run_response["runId"]),
                    "elapsed_time": MetadataValue.int(
                        output.status_response["elapsedTime"]
                    ),
                },
            )
        )
        yield Output(output)

    return hex_asset
