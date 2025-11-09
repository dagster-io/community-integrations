from contextlib import contextmanager
from enum import Enum
from importlib import metadata
from collections.abc import Generator
from weakref import WeakKeyDictionary

from packaging import version
from dagster import (
    __version__,
    AssetExecutionContext,
    AssetKey,
    ConfigurableResource,
    InitResourceContext,
    OpExecutionContext,
)
from dagster._annotations import public
from dagster._core.errors import DagsterInvariantViolationError
from notdiamond import NotDiamond
from pydantic import Field, PrivateAttr

if version.parse(__version__) >= version.parse("1.10.0"):
    from dagster._annotations import preview  # pyright: ignore[reportAttributeAccessIssue]
else:
    from dagster._annotations import experimental as preview  # pyright: ignore[reportAttributeAccessIssue]


class ApiEndpointClassesEnum(Enum):
    """Supported endpoint classes of the Not Diamond API v2."""

    MODEL_SELECT = "model_select"


context_to_counters = WeakKeyDictionary()


@public
@preview
class NotDiamondResource(ConfigurableResource):
    """This resource is wrapper over the
    `notdiamond` library <https://github.com/notdiamond/notdiamond-python>.

    By configuring this resource, you can interact with the Not Diamond API
    and log its usage metadata in the asset metadata.

    Examples:
        .. code-block:: python

        from dagster import (
            Definitions,
            EnvVar,
            ScheduleDefinition,
            AssetSelection,
            AssetExecutionContext,
            MaterializeResult,
            asset,
        )
        from dagster_notdiamond import NotDiamondResource


        @asset
        def notdiamond_asset(context: AssetExecutionContext, notdiamond: NotDiamondResource) -> MaterializeResult:
            with notdiamond.get_client(context) as client:
                session_id, best_llm = client.model_select(
                    model=["openai/gpt-4o", "openai/gpt-4o-mini"],
                    messages=[{"role": "user", "content": "Say this is a test"}]
                )
            return MaterializeResult(metadata={"session_id": session_id, "best_llm": str(best_llm)})

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
    """

    api_key: str = Field(
        description=("NotDiamond API key. See https://app.notdiamond.ai/keys")
    )

    _client: NotDiamond = PrivateAttr()

    @classmethod
    def _is_dagster_maintained(cls) -> bool:
        return False

    def setup_for_execution(self, context: InitResourceContext) -> None:
        # Set up a Not Diamond client based on the API key.
        self._client = NotDiamond(
            api_key=self.api_key,
            user_agent=f"dagster-notdiamond/{metadata.version('dagster-notdiamond')}",
        )

    @public
    @contextmanager
    def get_client(
        self, context: AssetExecutionContext | OpExecutionContext
    ) -> Generator[NotDiamond, None, None]:
        """Yields an ``notdiamond.NotDiamond`` client for interacting with the Not DiamondAPI.

        By default, in an asset context, the client comes with wrapped endpoints
        for the model_select resource in the Not Diamond API.

        Note that the endpoints are not and cannot be wrapped
        to automatically capture the API usage metadata in an op context.

        :param context: The ``context`` object for computing the op or asset in which ``get_client`` is called.

        Examples:
            .. code-block:: python

                from dagster import AssetExecutionContext, asset
                from dagster_notdiamond import NotDiamondResource

                @asset(compute_kind="NotDiamond")
                def notdiamond_asset(context: AssetExecutionContext, notdiamond: NotDiamondResource) -> Tuple[str, str]:
                    with notdiamond.get_client(context) as client:
                        session_id, best_llm = client.model_select(
                            model=["openai/gpt-4o", "openai/gpt-4o-mini"],
                            messages=[{"role": "user", "content": "Say this is a test"}]
                        )
                    return session_id, str(best_llm)
        """
        yield from self._get_client(context=context, asset_key=None)

    @public
    @contextmanager
    def get_client_for_asset(
        self, context: AssetExecutionContext, asset_key: AssetKey
    ) -> Generator[NotDiamond, None, None]:
        """Yields an ``notdiamond.NotDiamond`` for interacting with the Not Diamond.

        When using this method, the Not Diamond API usage metadata is automatically
        logged in the asset materializations associated with the provided ``asset_key``.

        This method can only be called when working with assets,
        i.e. the provided ``context`` must be of type ``AssetExecutionContext``.

        :param context: The ``context`` object for computing the asset in which ``get_client`` is called.
        :param asset_key: the ``asset_key`` of the asset for which a materialization should include the metadata.
        """
        yield from self._get_client(context=context, asset_key=asset_key)

    def _get_client(
        self,
        context: AssetExecutionContext | OpExecutionContext,
        asset_key: AssetKey | None = None,
    ) -> Generator[NotDiamond, None, None]:
        if isinstance(context, AssetExecutionContext):
            if asset_key is None:
                if len(context.assets_def.keys_by_output_name.keys()) > 1:
                    raise DagsterInvariantViolationError(
                        "The argument `asset_key` must be specified for multi_asset with more than one asset."
                    )
                asset_key = context.asset_key

        yield self._client
