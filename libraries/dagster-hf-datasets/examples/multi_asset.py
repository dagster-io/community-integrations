from dagster import Definitions

from dagster_hf_datasets.assets import (
    hf_multi_asset,
)
from dagster_hf_datasets.io_manager import (
    HFParquetIOManager,
)
from dagster_hf_datasets.resources import (
    HuggingFaceResource,
)


@hf_multi_asset(
    path="allenai/c4",
    streaming=True,
    group_name="huggingface_streaming",
    io_manager_key="hf_parquet_io_manager",
)
def c4_streaming_assets():
    """
    Materialize streaming dataset splits
    as separate Dagster assets.

    Demonstrates:
    - Hugging Face streaming
    - DatasetDict orchestration
    - scalable ingestion
    """
    ...


defs = Definitions(
    assets=[
        c4_streaming_assets,
    ],
    resources={
        "huggingface": HuggingFaceResource(
            cache_dir=".hf_cache",
            offline=False,
        ),
        "hf_parquet_io_manager": (
            HFParquetIOManager(
                base_dir=".dagster_hf_storage",
            )
        ),
    },
)
