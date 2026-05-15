from __future__ import annotations

from pathlib import Path

import pandas as pd
from dagster import (
    ConfigurableIOManager,
    InputContext,
    MetadataValue,
    OutputContext,
)
from datasets import Dataset


class HFParquetIOManager(ConfigurableIOManager):
    """
    Lightweight parquet-backed Dagster IO manager for
    Hugging Face datasets.

    Responsibilities:
    - Persist Hugging Face Datasets as parquet
    - Persist pandas DataFrames as parquet
    - Load parquet into pandas DataFrames
    - Emit lightweight Dagster metadata

    This IO manager intentionally avoids:
    - automatic Hub synchronization
    - dataset card generation
    - orchestration automation
    - multimodal serialization handling
    - schema evolution management

    Canonical storage format:
        parquet

    Supported input types:
        - datasets.Dataset
        - pandas.DataFrame
    """

    base_dir: str = ".dagster_hf_storage"

    def handle_output(
        self,
        context: OutputContext,
        obj: Dataset | pd.DataFrame,
    ) -> None:
        """
        Persist supported objects as parquet.
        """
        output_path = self._get_output_path(context)

        output_path.parent.mkdir(
            parents=True,
            exist_ok=True,
        )

        if isinstance(obj, Dataset):
            self._handle_dataset_output(
                output_path=output_path,
                dataset=obj,
            )

            metadata = self._build_dataset_metadata(
                output_path=output_path,
                dataset=obj,
            )

        elif isinstance(obj, pd.DataFrame):
            self._handle_dataframe_output(
                output_path=output_path,
                dataframe=obj,
            )

            metadata = self._build_dataframe_metadata(
                output_path=output_path,
                dataframe=obj,
            )

        else:
            raise TypeError(
                "HFParquetIOManager only supports "
                "datasets.Dataset and pandas.DataFrame."
            )

        context.add_output_metadata(metadata)

    def load_input(
        self,
        context: InputContext,
    ) -> pd.DataFrame:
        """
        Load parquet into pandas DataFrame.

        Parquet is treated as the canonical storage layer.
        """
        input_path = self._get_input_path(context)

        if not input_path.exists():
            raise FileNotFoundError(f"Input parquet file not found: " f"{input_path}")

        return pd.read_parquet(input_path)

    def _handle_dataset_output(
        self,
        *,
        output_path: Path,
        dataset: Dataset,
    ) -> None:
        """
        Persist Hugging Face Dataset directly as parquet.
        """
        dataset.to_parquet(str(output_path))

    def _handle_dataframe_output(
        self,
        *,
        output_path: Path,
        dataframe: pd.DataFrame,
    ) -> None:
        """
        Persist pandas DataFrame as parquet.
        """
        dataframe.to_parquet(output_path)

    def _build_dataset_metadata(
        self,
        *,
        output_path: Path,
        dataset: Dataset,
    ) -> dict[str, MetadataValue]:
        """
        Build Dagster metadata for Dataset outputs.
        """
        return {
            "path": MetadataValue.path(str(output_path)),
            "rows": MetadataValue.int(dataset.num_rows),
            "columns": MetadataValue.int(len(dataset.column_names)),
            "column_names": MetadataValue.json(dataset.column_names),
            "fingerprint": MetadataValue.text(dataset._fingerprint),
        }

    def _build_dataframe_metadata(
        self,
        *,
        output_path: Path,
        dataframe: pd.DataFrame,
    ) -> dict[str, MetadataValue]:
        """
        Build Dagster metadata for DataFrame outputs.
        """
        return {
            "path": MetadataValue.path(str(output_path)),
            "rows": MetadataValue.int(len(dataframe)),
            "columns": MetadataValue.int(len(dataframe.columns)),
            "column_names": MetadataValue.json(list(dataframe.columns)),
        }

    def _get_output_path(
        self,
        context: OutputContext,
    ) -> Path:
        """
        Generate deterministic parquet output path.

        Path format:
            <base_dir>/<asset_key>.parquet
        """
        asset_path = "/".join(context.asset_key.path)

        return Path(self.base_dir) / f"{asset_path}.parquet"

    def _get_input_path(
        self,
        context: InputContext,
    ) -> Path:
        """
        Resolve parquet input path from upstream asset.
        """
        upstream_output = context.upstream_output

        if upstream_output is None:
            raise ValueError("Upstream output context is required.")

        asset_path = "/".join(upstream_output.asset_key.path)

        return Path(self.base_dir) / f"{asset_path}.parquet"

    @staticmethod
    def dataset_to_dataframe(
        dataset: Dataset,
    ) -> pd.DataFrame:
        """
        Convert Hugging Face Dataset into pandas DataFrame.
        """
        return dataset.to_pandas()

    @staticmethod
    def dataframe_to_dataset(
        dataframe: pd.DataFrame,
    ) -> Dataset:
        """
        Convert pandas DataFrame into Hugging Face Dataset.
        """
        return Dataset.from_pandas(
            dataframe,
            preserve_index=False,
        )
