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

import google.generativeai as genai
from google.generativeai import GenerativeModel

if version.parse(__version__) >= version.parse("1.10.0"):
    from dagster._annotations import preview  # pyright: ignore[reportAttributeAccessIssue]
else:
    from dagster._annotations import experimental as preview  # pyright: ignore[reportAttributeAccessIssue]

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
#  Gemini only has one endpoint that provides usage stats (generate_content).
#  Gemini's "chat conversation" feature is just a thin wrapper around the same generate_content
#  method https://ai.google.dev/gemini-api/docs/text-generation?lang=python#chat
# likewise, the embedding-endpoint also doens't provide any usage stats.
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
        usage = response.usage_metadata
        usage_metadata = {
            "gemini.calls": 1,
            "gemini.cached_content_token_count": usage.cached_content_token_count,
            "gemini.candidates_token_count": usage.candidates_token_count,
            "gemini.prompt_token_count": usage.prompt_token_count,
            "gemini.total_token_count": usage.total_token_count,
        }

        _add_to_asset_metadata(context, usage_metadata, output_name)

        return response

    return wrapper


@public
@preview
class GeminiResource(ConfigurableResource):
    """This resource is a wrapper over the `Gemini library<https://github.com/google-gemini/generative-ai-python>`.

    In addition to wrapping the Gemini GenerativeModel class (get_model/get_model_for_asset methods),
    this resource logs the usage of the Gemini API to to the asset metadata (both number of calls, and tokens).
    This is achieved by wrapping the model.generate_content method.

    Note that the usage will only be logged to the asset metadata from an Asset context -
    not from an Op context.
    Also note that only the synchronous API usage metadata will be automatically logged -
    not the streaming or batching API.

    Examples:
        .. code-block:: python

            from dagster import AssetExecutionContext, Definitions, EnvVar, asset, define_asset_job
            from dagster_gemini import GeminiResource


            @asset(compute_kind="gemini")
            def gemini_asset(context: AssetExecutionContext, gemini: GeminiResource):
                with gemini.get_model(context) as model:
                    response = model.generate_content(
                        "Generate a short sentence on tests"
                    )

            defs = Definitions(
                assets=[gemini_asset],
                resources={
                    "gemini": GeminiResource(
                        api_key=EnvVar("GEMINI_API_KEY"),
                        generative_model_name="gemini-1.5-flash"
                    ),
                },
            )

    """

    api_key: str = Field(
        description=(
            "Gemini API key - See https://ai.google.dev/gemini-api/docs/api-key"
        )
    )

    # We call this "generative_model_name" and not "model_name" to avoid pydantic warnings about
    #  model_* being a protected namespace
    generative_model_name: str = Field(
        description=(
            "A gemini model (e.g. gemini-1.5-flash). select one from https://ai.google.dev/gemini-api/docs/models/gemini"
        )
    )

    _generative_model: GenerativeModel = PrivateAttr()

    @classmethod
    def _is_dagster_maintained(cls) -> bool:
        return False

    def setup_for_execution(self, context: InitResourceContext) -> None:
        # See documentation of the gemini SDK: https://ai.google.dev/api?lang=python
        genai.configure(api_key=self.api_key)
        self._generative_model = GenerativeModel(model_name=self.generative_model_name)

        # The Gemini library is strange: instead of passing an API-key to the model, you need
        #  to call the global "configure" function -
        #  the model will take the API key from this global configuration - and save it under
        #  the model's _client field.
        # But also - this doesn't happen in the GenerativeModel constructor!
        # It happens lazily - on the first real API call to Gemini.
        # So - in order for the model to actually use the provided API-key (and not with whichever
        #  api-key was last configurd via the global "configure" function),
        #    - we need to make an API call after creating the model.
        # We use the countTokens API - since it is cost-free and not billable by Google.
        #   https://cloud.google.com/vertex-ai/generative-ai/docs/multimodal/get-token-count#pricing_and_quota
        # Code link here:
        #  https://github.com/google-gemini/generative-ai-python/blob/5a3ba73b363f08b7feb319791f2db6c1d6b00c6f/google/generativeai/generative_models.py#L412-L413

        self._generative_model.count_tokens("how many tokens in this sentence?")

    @public
    @contextmanager
    def get_model(
        self, context: AssetExecutionContext | OpExecutionContext
    ) -> Generator[GenerativeModel, None, None]:
        """Yields a ``gemini.GenerativeModel`` model for interacting with the Gemini API.

        When used in an asset context, the resource wraps the ``model.generate_content`` method
        to automatically log its usage to the asset metadata.

        Note that the usage will only be logged to the asset metadata from an Asset context -
        not from an Op context.
        Also note that only the synchronous API usage metadata will be automatically logged -
        not the streaming or batching API.

        :param context: The ``context`` object for computing the op or asset in which ``get_model`` is called.

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
                from dagster_gemini import GeminiResource

                # This is an asset using the Gemini resource - usage stats will be logged to
                #  the asset metadata.
                @asset(compute_kind="gemini")
                def gemini_asset(context: AssetExecutionContext, gemini: GeminiResource):
                    with gemini.get_model(context) as model:
                        response = model.generate_content(
                            "Generate a short sentence on tests"
                        )

                gemini_asset_job = define_asset_job(name="gemini_asset_job", selection="gemini_asset")

                # This is an op using the Gemini resource - usage stats will not be logged.
                @op
                def gemini_op(context: OpExecutionContext, gemini: GeminiResource):
                    with gemini.get_model(context) as model:
                        response = model.generate_content(
                            "Generate a short sentence on tests"
                        )

                gemini_op_job = GraphDefinition(name="gemini_op_job", node_defs=[gemini_op]).to_job()

                defs = Definitions(
                    assets=[gemini_asset],
                    jobs=[gemini_asset_job, gemini_op_job],
                    resources={
                        "gemini": GeminiResource(
                            api_key=EnvVar("GEMINI_API_KEY"),
                            generative_model_name="gemini-1.5-flash"
                        ),
                    },
                )
        """
        yield from self._get_model(context=context, asset_key=None)

    def _wrap_with_usage_metadata(
        self, context: AssetExecutionContext, output_name: str | None
    ):
        self._generative_model.generate_content = with_usage_metadata(
            context, output_name, func=self._generative_model.generate_content
        )

    @public
    @contextmanager
    def get_model_for_asset(
        self, context: AssetExecutionContext, asset_key: AssetKey
    ) -> Generator[GenerativeModel, None, None]:
        """Yields a ``gemini.GenerativeModel`` model for interacting with the Gemini API.

        When using this method, the gemini usage metadata is automatically logged in the
        asset materializations associated with the provided ``asset_key``.
        This is achieved by wrapping the GenerativeModel.generate_content method.

        This method can only be called when working with assets,
        i.e. the provided ``context`` must be of type ``AssetExecutionContext``.
        Also note that only the synchronous API usage metadata will be automatically logged -
        not the streaming or batching API.

        :param context: the ``context`` object for computing the asset in which ``get_model_for_asset`` is called.
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
                from dagster_gemini import GeminiResource


                @asset(compute_kind="gemini")
                def gemini_asset(context: AssetExecutionContext, gemini: GeminiResource):
                    with gemini.get_model_for_asset(context, context.asset_key) as model:
                        response = model.generate_content(
                            "Generate a short sentence on tests"
                        )


                @multi_asset(specs=[AssetSpec("my_asset1"), AssetSpec("my_asset2")], compute_kind="gemini")
                def gemini_multi_asset(context: AssetExecutionContext, gemini: GeminiResource):
                    with gemini.get_model_for_asset(context, asset_key=AssetKey("my_asset1")) as model:
                        response = model.generate_content(
                            "Generate a short sentence on hats"
                        )
                        return (
                            MaterializeResult(asset_key="my_asset1", metadata={"some_key": "some_value1"}),
                            MaterializeResult(asset_key="my_asset2", metadata={"some_key": "some_value2"}),
                        )


                defs = Definitions(
                    assets=[gemini_asset, gemini_multi_asset],
                    resources={
                        "gemini": GeminiResource(
                            api_key=EnvVar("GEMINI_API_KEY"),
                            generative_model_name="gemini-1.5-flash"
                        ),
                    },
                )

        """
        yield from self._get_model(context=context, asset_key=asset_key)

    def _get_model(
        self,
        context: AssetExecutionContext | OpExecutionContext,
        asset_key: AssetKey | None = None,
    ) -> Generator[GenerativeModel, None, None]:
        if isinstance(context, AssetExecutionContext):
            if asset_key is None:
                if len(context.assets_def.keys_by_output_name.keys()) > 1:
                    raise DagsterInvariantViolationError(
                        "The argument `asset_key` must be specified for multi_asset with more than one asset."
                    )
                asset_key = context.asset_key

            output_name = context.output_for_asset_key(asset_key)
            self._wrap_with_usage_metadata(context=context, output_name=output_name)

        yield self._generative_model

    def teardown_after_execution(self, context: InitResourceContext) -> None:
        # The google-generativeai package has no 'close' method.
        pass
