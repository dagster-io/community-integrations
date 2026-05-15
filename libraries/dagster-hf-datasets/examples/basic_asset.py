from __future__ import annotations

from dagster import (
    Definitions,
    materialize,
)

from dagster_hf_datasets.assets import (
    hf_dataset_asset,
)
from dagster_hf_datasets.io_manager import (
    HFParquetIOManager,
)
from dagster_hf_datasets.resources import (
    HuggingFaceResource,
)


@hf_dataset_asset(
    path="nyu-mll/glue",
    config="qqp",
    split="train",
    group_name="huggingface_datasets",
    io_manager_key="hf_parquet_io_manager",
)
def glue_qqp_train():
    """
    Materialize the GLUE QQP training split
    as a Dagster asset backed by a
    Hugging Face Dataset.
    """
    pass


defs = Definitions(
    assets=[
        glue_qqp_train,
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


if __name__ == "__main__":
    result = materialize(
        assets=[
            glue_qqp_train,
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

    if result.success:
        print(
            "Successfully materialized "
            "GLUE QQP training dataset asset."
        )
    else:
        raise RuntimeError(
            "Dagster asset materialization failed."
        )