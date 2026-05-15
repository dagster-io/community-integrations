from __future__ import annotations

from pathlib import Path

import pandas as pd
from dagster import (
    ConfigurableIOManager,
    InputContext,
    MetadataValue,
    OutputContext,
)
from datasets import (
    Dataset,
    IterableDataset,
)


class HFParquetIOManager(ConfigurableIOManager):
    """
    Dagster IO manager for Hugging Face datasets.

    Supported object types:
    - datasets.Dataset
    - datasets.IterableDataset
    - pandas.DataFrame

    Persistence semantics:
    - Dataset:
        persisted via save_to_disk()

    - IterableDataset:
        runtime-only streaming object
        (not persisted)

    - pandas.DataFrame:
        persisted as parquet
    """

    base_dir: str = ".dagster_hf_storage"

    def handle_output(
        self,
        context: OutputContext,
        obj: (
            Dataset
            | IterableDataset
            | pd.DataFrame
        ),
    ) -> None:
        output_path = self._get_output_path(
            context
        )

        output_path.parent.mkdir(
            parents=True,
            exist_ok=True,
        )

        if isinstance(obj, Dataset):
            obj.save_to_disk(
                str(output_path)
            )

            metadata = {
                "path": MetadataValue.path(
                    str(output_path)
                ),
                "format": MetadataValue.text(
                    "huggingface_dataset"
                ),
                "streaming": MetadataValue.bool(
                    False
                ),
                "rows": MetadataValue.int(
                    obj.num_rows
                ),
                "columns": MetadataValue.int(
                    len(obj.column_names)
                ),
                "column_names": MetadataValue.json(
                    obj.column_names
                ),
                "fingerprint": (
                    MetadataValue.text(
                        obj._fingerprint
                    )
                ),
            }

        elif isinstance(obj, IterableDataset):
            context.log.warning(
                "IterableDataset detected. "
                "Streaming datasets are not "
                "persisted by "
                "HFParquetIOManager."
            )

            metadata = {
                "format": MetadataValue.text(
                    "huggingface_iterable_dataset"
                ),
                "streaming": MetadataValue.bool(
                    True
                ),
            }

        elif isinstance(obj, pd.DataFrame):
            parquet_path = (
                output_path.with_suffix(
                    ".parquet"
                )
            )

            obj.to_parquet(parquet_path)

            metadata = {
                "path": MetadataValue.path(
                    str(parquet_path)
                ),
                "format": MetadataValue.text(
                    "pandas_parquet"
                ),
                "streaming": MetadataValue.bool(
                    False
                ),
                "rows": MetadataValue.int(
                    len(obj)
                ),
                "columns": MetadataValue.int(
                    len(obj.columns)
                ),
                "column_names": MetadataValue.json(
                    list(obj.columns)
                ),
            }

        else:
            raise TypeError(
                "Unsupported object type: "
                f"{type(obj)}"
            )

        context.add_output_metadata(
            metadata
        )

    def load_input(
        self,
        context: InputContext,
    ):
        input_path = self._get_input_path(
            context
        )

        parquet_path = (
            input_path.with_suffix(
                ".parquet"
            )
        )

        if input_path.exists():
            return Dataset.load_from_disk(
                str(input_path)
            )

        if parquet_path.exists():
            return pd.read_parquet(
                parquet_path
            )

        raise FileNotFoundError(
            "No persisted artifact found. "
            "This may be a streaming "
            "IterableDataset asset, which "
            "is runtime-only and not "
            "persisted by "
            "HFParquetIOManager."
        )

    def _get_output_path(
        self,
        context: OutputContext,
    ) -> Path:
        asset_path = "/".join(
            context.asset_key.path
        )

        return (
            Path(self.base_dir)
            / asset_path
        )

    def _get_input_path(
        self,
        context: InputContext,
    ) -> Path:
        upstream_output = (
            context.upstream_output
        )

        if upstream_output is None:
            raise ValueError(
                "Upstream output context "
                "is required."
            )

        asset_path = "/".join(
            upstream_output.asset_key.path
        )

        return (
            Path(self.base_dir)
            / asset_path
        )