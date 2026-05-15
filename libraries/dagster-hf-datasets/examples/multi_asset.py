from __future__ import annotations

from dagster import (
    Definitions,
    materialize,
)

from dagster_hf_datasets.assets import (
    hf_multi_asset,
)
from dagster_hf_datasets.io_manager import (
    HFParquetIOManager,
)
from dagster_hf_datasets.resources import (
    HuggingFaceResource,
)


resources = {
    "huggingface": HuggingFaceResource(
        cache_dir=".hf_cache",
        offline=False,
    ),
    "hf_parquet_io_manager": (
        HFParquetIOManager(
            base_dir=".dagster_hf_storage",
        )
    ),
}


@hf_multi_asset(
    path="allenai/c4",
    config="en",
    streaming=True,
    group_name="huggingface_streaming",
    io_manager_key="hf_parquet_io_manager",
)
def c4_streaming_assets():
    """
    Orchestrate streaming Hugging Face
    dataset splits as Dagster assets.

    Demonstrates:
    - Hugging Face streaming datasets
    - lazy IterableDataset execution
    - split-aware orchestration
    - scalable ingestion workflows

    Notes:
    - streaming assets are runtime-only
    - IterableDataset objects are not
      persisted by HFParquetIOManager
    """
    ...


defs = Definitions(
    assets=[
        c4_streaming_assets,
    ],
    resources=resources,
)


if __name__ == "__main__":
    result = materialize(
        assets=[
            c4_streaming_assets,
        ],
        resources=resources,
    )

    if result.success:
        print(
            "Successfully orchestrated "
            "C4 streaming dataset assets."
        )
    else:
        raise RuntimeError(
            "Dagster streaming asset "
            "execution failed."
        )