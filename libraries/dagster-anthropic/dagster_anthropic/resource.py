from collections import defaultdict
from contextlib import contextmanager
from functools import wraps
from packaging import version
from collections.abc import Generator
from weakref import WeakKeyDictionary

from pydantic import Field, PrivateAttr

from dagster._annotations import public
from dagster import (
    __version__,
    ConfigurableResource,
    DagsterInvariantViolationError,
    InitResourceContext,
    AssetExecutionContext,
    OpExecutionContext,
    AssetKey,
)

if version.parse(__version__) >= version.parse("1.10.0"):
    from dagster._annotations import preview  # pyright: ignore[reportAttributeAccessIssue]
else:
    from dagster._annotations import experimental as preview  # pyright: ignore[reportAttributeAccessIssue]

from anthropic import Anthropic

context_to_counters = WeakKeyDictionary()


def _add_to_asset_metadata(
    context: AssetExecutionContext, usage_metadata: dict, output_name: str | None
):
    if context not in context_to_counters:
        context_to_counters[context] = defaultdict(int)
    counters = context_to_counters[context]

    for metadata_key, delta in usage_metadata.items():
        counters[metadata_key] += delta
    context.add_output_metadata(dict(counters), output_name)


# Unlike in OpenAI - we shouldn't export this wrapper to the user.
# While OpenAI has a lot of endpoints that can be wrapped to log usage,
# Anthropic is a much smaller library, and only has one interface (Anthropic.messages.create).
def with_usage_metadata(
    context: AssetExecutionContext | OpExecutionContext,
    output_name: str | None,
    func,
):
    if not isinstance(context, AssetExecutionContext):
        raise DagsterInvariantViolationError(
            "The `with_usage_metadata` can only be used when context is of type `AssetExecutionContext`."
        )

    @wraps(func)
    def wrapper(*args, **kwargs):
        response = func(*args, **kwargs)
        usage = response.usage
        usage_metadata = {
            "anthropic.calls": 1,
            "anthropic.input_tokens": usage.input_tokens,
            # The "or 0" pattern is needed since these are Optional in the anthropic Usage struct
            "anthropic.cache_creation_input_tokens": usage.cache_creation_input_tokens
            or 0,
            "anthropic.cache_read_input_tokens": usage.cache_read_input_tokens or 0,
            "anthropic.output_tokens": usage.output_tokens,
        }

        _add_to_asset_metadata(context, usage_metadata, output_name)

        return response

    return wrapper


@public
@preview
class AnthropicResource(ConfigurableResource):
    """This resource is a wrapper over the `Anthropic library<https://github.com/anthropics/anthropic-sdk-python>`.

    In addition to wrapping the Anthropic client (get_client/get_client_for_asset methods),
    this resource logs the usage of the Anthropic API to to the asset metadata (both number of calls, and tokens).
    This is achieved by wrapping the Anthropic.messages.create method.

    Note that the usage will only be logged to the asset metadata from an Asset context -
    not from an Op context.
    Also note that only the synchronous API usage metadata will be automatically logged -
    not the streaming or batching API.

    Examples:
        .. code-block:: python

            from dagster import AssetExecutionContext, Definitions, EnvVar, asset, define_asset_job
            from dagster_anthropic import AnthropicResource


            @asset(compute_kind="anthropic")
            def anthropic_asset(context: AssetExecutionContext, anthropic: AnthropicResource):
                with anthropic.get_client(context) as client:
                    response = client.messages.create(
                        model="claude-3-5-sonnet-20241022",
                        max_tokens=1024,
                        messages=[
                            {"role": "user", "content": "Please write a short sentence on cats"}
                        ]
                    )

            defs = Definitions(
                assets=[anthropic_asset],
                resources={
                    "anthropic": AnthropicResource(api_key=EnvVar("ANTHROPIC_API_KEY")),
                },
            )

    """

    api_key: str = Field(
        description=(
            "Anthropic API key. See https://docs.anthropic.com/en/api/getting-started#authentication"
        )
    )
    base_url: str = Field(default=None)

    _client: Anthropic = PrivateAttr()

    @classmethod
    def _is_dagster_maintained(cls) -> bool:
        return False

    def setup_for_execution(self, context: InitResourceContext) -> None:
        self._client = Anthropic(api_key=self.api_key, base_url=self.base_url)

    @public
    @contextmanager
    def get_client(
        self, context: AssetExecutionContext | OpExecutionContext
    ) -> Generator[Anthropic, None, None]:
        """Yields an ``anthropic.Anthropic`` client for interacting with the Anthropic API.

        When used in an asset context, the client wraps the ``Anthropic.messages.create`` method
        to automatically log their usage to the asset metadata.

        Note that the usage will only be logged to the asset metadata from an Asset context -
        not from an Op context.
        Also note that only the synchronous API usage metadata will be automatically logged -
        not the streaming or batching API.

        :param context: The ``context`` object for computing the op or asset in which ``get_client`` is called.

        Examples:
            .. code-block:: python

                from dagster import (
                    AssetExecutionContext,
                    Definitions,
                    EnvVar,
                    GraphDefinition,
                    OpExecutionContext,
                    asset,
                    define_asset_job,
                    op,
                )
                from dagster_anthropic import AnthropicResource

                # This is an asset using the Anthropic resource - usage stats will be logged to
                #  the asset metadata.
                @asset(compute_kind="anthropic")
                def anthropic_asset(context: AssetExecutionContext, anthropic: AnthropicResource):
                    with anthropic.get_client(context) as client:
                        response = client.messages.create(
                            model="claude-3-5-sonnet-20241022",
                            max_tokens=1024,
                            messages=[{"role": "user", "content": "Write a sentence on hats"}]
                        )

                anthropic_asset_job = define_asset_job(name="anthropic_asset_job", selection="anthropic_asset")

                # This is an op using the Anthropic resource - usage stats will not be automatically logged.
                @op
                def anthropic_op(context: OpExecutionContext, anthropic: AnthropicResource):
                    with anthropic.get_client(context) as client:
                        response = client.messages.create(
                            model="claude-3-5-sonnet-20241022",
                            max_tokens=1024,
                            messages=[{"role": "user", "content": "Write a sentence on cats"}]
                        )

                anthropic_op_job = GraphDefinition(name="anthropic_op_job", node_defs=[anthropic_op]).to_job()

                defs = Definitions(
                    assets=[anthropic_asset],
                    jobs=[anthropic_asset_job, anthropic_op_job],
                    resources={
                        "anthropic": AnthropicResource(api_key=EnvVar("ANTHROPIC_API_KEY")),
                    },
                )
        """
        yield from self._get_client(context=context, asset_key=None)

    def _wrap_with_usage_metadata(
        self, context: AssetExecutionContext, output_name: str | None
    ):
        self._client.messages.create = with_usage_metadata(
            context, output_name, func=self._client.messages.create
        )  # pyright: ignore
        # We skip pyright on this line - it complains about the fact that the Anthropic.message.create
        # (just like OpenAI.chat.copmletions.create), has multiple possible return values
        #  (depending on the boolean stream parameter)

    @public
    @contextmanager
    def get_client_for_asset(
        self, context: AssetExecutionContext, asset_key: AssetKey
    ) -> Generator[Anthropic, None, None]:
        """Yields an ``anthropic.Anthropic`` client for interacting with the Anthropic API.

        When using this method, the anthropic usage metadata is automatically logged in the
        asset materializations associated with the provided ``asset_key``.
        This is achieved by wrapping the Anthropic.messages.create method.

        This method can only be called when working with assets,
        i.e. the provided ``context`` must be of type ``AssetExecutionContext``.
        Also note that only the synchronous API usage metadata will be automatically logged -
        not the streaming or batching API.

        :param context: the ``context`` object for computing the asset in which ``get_client`` is called.
        :param asset_key: the ``asset_key`` of the asset for which a materialization should include the metadata.

        Examples:
            .. code-block:: python

                from dagster import (
                    AssetExecutionContext,
                    AssetKey,
                    AssetSpec,
                    Definitions,
                    EnvVar,
                    MaterializeResult,
                    asset,
                    define_asset_job,
                    multi_asset,
                )
                from dagster_anthropic import AnthropicResource


                @asset(compute_kind="anthropic")
                def anthropic_asset(context: AssetExecutionContext, anthropic: AnthropicResource):
                    with anthropic.get_client_for_asset(context, context.asset_key) as client:
                        response = client.messages.create(
                            model="claude-3-5-sonnet-20241022",
                            max_tokens=1024,
                            messages=[{"role": "user", "content": "Write a sentence on cats"}]
                        )


                @multi_asset(specs=[AssetSpec("my_asset1"), AssetSpec("my_asset2")], compute_kind="anthropic")
                def anthropic_multi_asset(context: AssetExecutionContext, anthropic: AnthropicResource):
                    with anthropic.get_client_for_asset(context, asset_key=AssetKey("my_asset1")) as client:
                        response = client.messages.create(
                            model="claude-3-5-sonnet-20241022",
                            max_tokens=1024,
                            messages=[{"role": "user", "content": "Write a sentence on hats"}]
                        )
                        return (
                            MaterializeResult(asset_key="my_asset1", metadata={"some_key": "some_value1"}),
                            MaterializeResult(asset_key="my_asset2", metadata={"some_key": "some_value2"}),
                        )


                defs = Definitions(
                    assets=[anthropic_asset, anthropic_multi_asset],
                    resources={
                        "anthropic": AnthropicResource(api_key=EnvVar("ANTHROPIC_API_KEY")),
                    },
                )

        """
        yield from self._get_client(context=context, asset_key=asset_key)

    def _get_client(
        self,
        context: AssetExecutionContext | OpExecutionContext,
        asset_key: AssetKey | None = None,
    ) -> Generator[Anthropic, None, None]:
        if isinstance(context, AssetExecutionContext):
            if asset_key is None:
                if len(context.assets_def.keys_by_output_name.keys()) > 1:
                    raise DagsterInvariantViolationError(
                        "The argument `asset_key` must be specified for multi_asset with more than one asset."
                    )
                asset_key = context.asset_key

            output_name = context.output_for_asset_key(asset_key)
            self._wrap_with_usage_metadata(context=context, output_name=output_name)

        yield self._client

    def teardown_after_execution(self, context: InitResourceContext) -> None:
        self._client.close()
