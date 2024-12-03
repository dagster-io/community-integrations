from typing import Any, Callable, Optional


import dagster as dg
from dagster._annotations import experimental


"""

HexWorkspace

Figure out _inputs_

"""


class HexWorkspace(dg.ConfigurableResource):
    api_key: str


@experimental
def hex_project_asset(
    *,
    project_id: str,
    workspace: HexWorkspace,
    name: Optional[str] = None,
    group_name: Optional[str] = None,
) -> Callable[[Callable[..., Any]], dg.AssetsDefinition]:
    """

    Examples:
        Execute and poll a Hex project while providing input arguments.

        .. code-block:: python
            from dagster_hex import HexWorkspace, hex_project_asset

            import dagster as dg


            hex_workspace = HexWorkspace(
                api_key=dg.EnvVar("HEX_API_KEY")
            )

            @hex_project_asset(
                project_id="hex_project_id",
                workspace=hex_workspace,
                group_name="hex"
            )
            def example_hex_project_asset(context: dg.AssetExecutionContext, hex: HexWorkspace):
                yield from hex.run_and_poll(project_id, inputs)

    """

    return dg.asset(name=name, group_name=group_name)
