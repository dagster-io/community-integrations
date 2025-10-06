import json
from collections import defaultdict
from contextlib import contextmanager
from functools import wraps
from typing import Generator, Optional, Union, Callable, Iterable
from weakref import WeakKeyDictionary

import google.auth
import vertexai
from dagster import (
    AssetExecutionContext,
    AssetKey,
    ConfigurableResource,
    DagsterInvariantViolationError,
    InitResourceContext,
    OpExecutionContext,
)
from dagster._annotations import public
from google.auth.credentials import Credentials as GoogleCredentials
from pydantic import Field, PrivateAttr
from vertexai.generative_models import GenerationResponse, GenerativeModel

from dagster._annotations import preview


# A WeakKeyDictionary is used to track usage counters for each execution context.
# This ensures that counters are automatically garbage-collected when the context is no longer in use,
# preventing memory leaks in long-running Dagster processes.
context_to_counters = WeakKeyDictionary()


def _add_to_asset_metadata(
    context: AssetExecutionContext, usage_metadata: dict, output_name: Optional[str]
) -> None:
    """Adds and aggregates usage metadata to the current asset materialization."""
    if context not in context_to_counters:
        context_to_counters[context] = defaultdict(lambda: 0)
    counters = context_to_counters[context]

    for metadata_key, delta in usage_metadata.items():
        counters[metadata_key] += delta
    context.add_output_metadata(dict(counters), output_name)


def _with_usage_metadata(
    context: AssetExecutionContext, output_name: Optional[str], func: Callable
) -> Callable:
    """A wrapper for the `generate_content` method to handle both streaming and non-streaming
    responses, extracting and logging usage metadata correctly for each case.
    """

    @wraps(func)
    def wrapper(
        *args, **kwargs
    ) -> Union[GenerationResponse, Iterable[GenerationResponse]]:
        is_streaming = kwargs.get("stream", False)

        if not is_streaming:
            # Standard non-streaming request
            response = func(*args, **kwargs)
            if hasattr(response, "usage_metadata"):
                usage = response.usage_metadata
                usage_metadata = {
                    "vertexai.calls": 1,
                    "vertexai.candidates_token_count": usage.candidates_token_count,
                    "vertexai.prompt_token_count": usage.prompt_token_count,
                    "vertexai.total_token_count": usage.total_token_count,  # Fixed: Use correct attribute
                }
                _add_to_asset_metadata(context, usage_metadata, output_name)
            return response

        # Streaming request: must return a generator
        def streaming_generator() -> Iterable[GenerationResponse]:
            # The original function call returns an iterator
            response_iterator = func(*args, **kwargs)
            yield from response_iterator

            # After the stream is exhausted, the iterator itself has the usage_metadata
            if hasattr(response_iterator, "usage_metadata"):
                usage = response_iterator.usage_metadata
                usage_metadata = {
                    "vertexai.calls": 1,
                    "vertexai.candidates_token_count": usage.candidates_token_count,
                    "vertexai.prompt_token_count": usage.prompt_token_count,
                    "vertexai.total_token_count": usage.total_token_count,  # Fixed: Use correct attribute
                }
                _add_to_asset_metadata(context, usage_metadata, output_name)

        return streaming_generator()

    return wrapper


@public
@preview
class VertexAIResource(ConfigurableResource):
    """A Dagster resource for interacting with Google Cloud Vertex AI's generative models.

    This resource provides a `vertexai.generative_models.GenerativeModel` instance, configured
    for use in ops and assets. It automatically handles the initialization of the Vertex AI SDK
    and logs API usage (token counts and call counts) as asset metadata.

    **Authentication:**
    Authentication is handled via `Application Default Credentials (ADC) <https://cloud.google.com/docs/authentication/application-default-credentials>`_
    by default. To use a service account, provide the service account key as a JSON string via
    the ``google_credentials`` configuration field.

    **Dependencies:**
    This resource requires the ``google-cloud-aiplatform`` library to be installed.
    ``pip install google-cloud-aiplatform``

    Examples:
        .. code-block:: python

            from dagster import AssetExecutionContext, Definitions, EnvVar, asset
            from dagster_vertexai import VertexAIResource

            @asset(compute_kind="vertexai")
            def my_report(context: AssetExecutionContext, vertex_ai: VertexAIResource):
                with vertex_ai.get_model(context) as model:
                    response = model.generate_content(
                        "Generate a business summary for a new marketing campaign."
                    )
                    return response.text

            defs = Definitions(
                assets=[my_report],
                resources={
                    "vertex_ai": VertexAIResource(
                        project_id="my-gcp-project",
                        location="us-central1",
                        generative_model_name="gemini-1.5-flash-001"
                    ),
                },
            )
    """

    project_id: str = Field(description="The Google Cloud project ID.")
    location: str = Field(
        description="The Google Cloud location (e.g., 'us-central1')."
    )

    google_credentials: Optional[str] = Field(
        default=None,
        description=(
            "A JSON string of Google Cloud service account credentials. If not provided, "
            "Application Default Credentials (ADC) will be used."
        ),
    )
    api_endpoint: Optional[str] = Field(
        default=None,
        description=(
            "The regional API endpoint for Vertex AI. If not set, the SDK will determine it "
            "based on the `location`. Example: 'us-central1-aiplatform.googleapis.com'"
        ),
    )
    generative_model_name: str = Field(
        description="The name of the generative model on Vertex AI (e.g., 'gemini-1.5-flash-001')."
    )

    _generative_model: GenerativeModel = PrivateAttr()

    @classmethod
    def _is_dagster_maintained(cls) -> bool:
        return False

    def setup_for_execution(self, context: InitResourceContext) -> None:
        """Initializes the Vertex AI SDK and the generative model instance."""
        creds: Optional[GoogleCredentials] = None
        if self.google_credentials:
            try:
                creds_dict = json.loads(self.google_credentials)
                creds, _ = google.auth.load_credentials_from_dict(creds_dict)
            except (json.JSONDecodeError, TypeError) as e:
                raise DagsterInvariantViolationError(
                    "Failed to load Google credentials from 'google_credentials'. "
                    f"Please ensure it is a valid JSON string. Error: {e}"
                )

        vertexai.init(
            project=self.project_id,
            location=self.location,
            credentials=creds,
            api_endpoint=self.api_endpoint,
        )
        self._generative_model = GenerativeModel(model_name=self.generative_model_name)

    @public
    @contextmanager
    def get_model(
        self, context: Union[AssetExecutionContext, OpExecutionContext]
    ) -> Generator[GenerativeModel, None, None]:
        """Yields a ``vertexai.generative_models.GenerativeModel`` for interacting with Vertex AI.

        When used within an asset's compute function, this method wraps the ``model.generate_content``
        method to automatically log its usage to the asset's materialization metadata.

        Args:
            context (Union[AssetExecutionContext, OpExecutionContext]): The Dagster execution context.
        """
        with self._get_model(context=context, asset_key=None) as model:
            yield model

    def _wrap_for_usage_tracking(
        self, context: AssetExecutionContext, output_name: Optional[str]
    ):
        """Patches the `generate_content` method on the model instance for the current context."""
        original_generate_content = self._generative_model.generate_content
        self._generative_model.generate_content = _with_usage_metadata(
            context, output_name, func=original_generate_content
        )

    @public
    @contextmanager
    def get_model_for_asset(
        self, context: AssetExecutionContext, asset_key: AssetKey
    ) -> Generator[GenerativeModel, None, None]:
        """Yields a ``vertexai.generative_models.GenerativeModel`` for a specific asset.

        This method is for use in ``@multi_asset``s. It ensures that any usage metadata
        is logged to the materialization of the asset specified by ``asset_key``.

        Args:
            context (AssetExecutionContext): The Dagster asset execution context.
            asset_key (AssetKey): The key of the asset to associate the metadata with.
        """
        with self._get_model(context=context, asset_key=asset_key) as model:
            yield model

    @contextmanager
    def _get_model(
        self,
        context: Union[AssetExecutionContext, OpExecutionContext],
        asset_key: Optional[AssetKey] = None,
    ) -> Generator[GenerativeModel, None, None]:
        """Internal method to provide the model, applying usage tracking if in an asset context."""
        # Store original method to restore later
        original_generate_content = None

        try:
            if isinstance(context, AssetExecutionContext):
                if asset_key is None:
                    # For single assets or multi-assets with one output, determine the asset key from context.
                    if len(context.assets_def.keys_by_output_name) > 1:
                        raise DagsterInvariantViolationError(
                            "The `asset_key` argument must be specified when calling `get_model` "
                            "from a @multi_asset with more than one output. To specify the asset, "
                            "use `get_model_for_asset` instead."
                        )
                    asset_key = context.asset_key

                output_name = context.output_for_asset_key(asset_key)
                # Store original method before wrapping
                original_generate_content = self._generative_model.generate_content
                self._wrap_for_usage_tracking(context=context, output_name=output_name)

            # Yield the model for both Asset and Op contexts
            yield self._generative_model

        finally:
            # Restore original method if we wrapped it
            if original_generate_content is not None:
                self._generative_model.generate_content = original_generate_content

    def teardown_after_execution(self, context: InitResourceContext) -> None:
        """No explicit teardown is required for the Vertex AI SDK."""
        pass
