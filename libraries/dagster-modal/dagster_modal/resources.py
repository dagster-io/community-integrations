# pyright: reportIncompatibleMethodOverride=false

from pathlib import Path
from collections.abc import Mapping

from dagster import AssetExecutionContext, OpExecutionContext, PipesSubprocessClient
from dagster._annotations import public
from dagster._core.pipes.client import PipesClientCompletedInvocation
from dagster_pipes import PipesExtras


class ModalClient(PipesSubprocessClient):
    """Modal client wrapping CLI.

    Examples:

        Registering a Modal project as as Dagster resource:

        .. code-block:: python

            import dagster as dg

            from dagster_modal import ModalClient

            modal_resource = ModalClient(project_directory=Path(__file__).parent.parent)

            defs = dg.Definitions(
                resources={
                        "modal": modal_resource,
                }
            )

    """

    def __init__(
        self,
        project_directory: str | Path | None = None,
        env: Mapping[str, str] | None = None,
    ):
        if isinstance(project_directory, Path):
            project_directory = str(project_directory)
        super().__init__(env=env, cwd=project_directory)

    @public
    def run(
        self,
        *,
        func_ref: str,
        context: AssetExecutionContext | OpExecutionContext,
        extras: PipesExtras | None = None,
        env: Mapping[str, str] | None = None,
    ) -> PipesClientCompletedInvocation:
        """Runs Modal function via the `modal run` command.

        Args:
            func_ref (str): Modal function reference
            context (OpExecutionContext): Dagster execution context
            extras (Optional[PipesExtras]): Extra parameters to pass to Modal
            env (Optional[Mapping[str, str]]): Environment variables to pass to Modal

        Examples:

            Running a modal function from a Dagster asset:

            .. code-block:: python

                @dg.asset(
                    compute_kind="modal",
                )
                def example_modal_asset(modal: ModalClient) -> dg.MaterializeResult:
                    return modal.run(
                        func_ref="example_modal_project.example_modal_function",
                        context=context,
                        env=os.environ,
                        extras={"extra_parameter": "Hello, World!"},
                    ).get_materialize_result()

        """
        return super().run(
            command=["modal", "run", func_ref],
            context=context,
            extras=extras,
            env=env,
        )
