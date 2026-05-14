from __future__ import annotations

from collections.abc import Callable
from typing import Any

from dagster import AssetExecutionContext, MaterializeResult, asset

from dagster_hf_datasets._metadata import build_dataset_metadata
from dagster_hf_datasets.resources import HuggingFaceResource


def hf_dataset_asset(
    *,
    path: str,
    config: str | None = None,
    split: str | None = None,
    revision: str | None = None,
    streaming: bool = False,
    name: str | None = None,
    group_name: str | None = None,
    key_prefix: str | list[str] | None = None,
    metadata: dict[str, Any] | None = None,
    tags: dict[str, str] | None = None,
) -> Callable[[Callable[..., Any]], Any]:
    """
    Dagster asset decorator for Hugging Face datasets.

    This decorator preserves Hugging Face dataset loading semantics
    while exposing datasets as first-class Dagster assets.

    Example:
        @hf_dataset_asset(
            path="glue",
            config="qqp",
            split="train",
        )
        def glue_qqp():
            ...

    Args:
        path:
            Hugging Face dataset repository path.
        config:
            Dataset configuration name.
        split:
            Dataset split.
        revision:
            Dataset revision/tag/commit hash.
        streaming:
            Enable streaming mode.
        name:
            Dagster asset name override.
        group_name:
            Dagster asset group.
        key_prefix:
            Dagster asset key prefix.
        metadata:
            Additional Dagster metadata.
        tags:
            Dagster asset tags.
    """

    def decorator(fn: Callable[..., Any]) -> Any:
        asset_name = name or _default_asset_name(
            path=path,
            config=config,
            split=split,
        )

        @asset(
            name=asset_name,
            group_name=group_name,
            key_prefix=key_prefix,
            metadata=metadata,
            tags=tags,
        )
        def _asset(
            context: AssetExecutionContext,
            huggingface: HuggingFaceResource,
        ) -> MaterializeResult:
            dataset = huggingface.load_dataset(
                path=path,
                config=config,
                split=split,
                revision=revision,
                streaming=streaming,
            )

            dataset_metadata = build_dataset_metadata(dataset)

            context.log.info(
                f"Loaded Hugging Face dataset: {path}"
            )

            return MaterializeResult(
                metadata={
                    "path": path,
                    "config": config,
                    "split": split,
                    "streaming": streaming,
                    **dataset_metadata,
                },
                value=dataset,
            )

        return _asset

    return decorator


def _default_asset_name(
    *,
    path: str,
    config: str | None,
    split: str | None,
) -> str:
    """
    Generate deterministic default Dagster asset names.
    """
    parts = [path.replace("/", "_")]

    if config:
        parts.append(config)

    if split:
        parts.append(split)

    return "_".join(parts)