from __future__ import annotations

import os
from pathlib import Path
from typing import Any

from dagster import ConfigurableResource
from datasets import (
    Dataset,
    DatasetDict,
    Features,
    IterableDataset,
    IterableDatasetDict,
    load_dataset,
)
from pydantic import Field

type HFDatasetLike = Dataset | DatasetDict | IterableDataset | IterableDatasetDict


class HuggingFaceResource(ConfigurableResource):
    """
    Dagster resource for interacting with Hugging Face Datasets.

    This resource intentionally preserves Hugging Face dataset
    semantics and delegates directly to
    `datasets.load_dataset(...)`.

    Responsibilities:
    - authentication configuration
    - dataset loading
    - cache configuration
    - offline mode support
    - lightweight metadata extraction
    """

    token: str | None = Field(
        default=None,
        description="Optional Hugging Face access token.",
    )

    token_path: str | None = Field(
        default=None,
        description=("Optional path to a file containing " "a Hugging Face token."),
    )

    cache_dir: str | None = Field(
        default=None,
        description=("Optional Hugging Face datasets " "cache directory."),
    )

    offline: bool = Field(
        default=False,
        description="Enable Hugging Face offline mode.",
    )

    def setup_for_execution(
        self,
        context: Any,
    ) -> None:
        """
        Configure Hugging Face execution environment.
        """
        del context

        self._configure_offline_mode()

    def load_dataset(
        self,
        path: str,
        config: str | None = None,
        split: str | None = None,
        revision: str | None = None,
        streaming: bool = False,
        **kwargs: Any,
    ) -> HFDatasetLike:
        """
        Load dataset using Hugging Face Datasets.

        This method intentionally mirrors the
        semantics of `datasets.load_dataset(...)`.

        Args:
            path:
                Dataset repository path or local
                dataset script.

            config:
                Dataset configuration name.

            split:
                Dataset split.

            revision:
                Dataset revision, tag, branch,
                or commit hash.

            streaming:
                Enable streaming mode.

            **kwargs:
                Additional keyword arguments
                forwarded directly to
                `datasets.load_dataset(...)`.

        Returns:
            Hugging Face dataset object.
        """
        resolved_token = self._resolve_token()

        common_kwargs = {
            "path": path,
            "name": config,
            "revision": revision,
            "token": resolved_token,
            "cache_dir": self.cache_dir,
            **kwargs,
        }

        if streaming:
            if split is None:
                return load_dataset(
                    streaming=True,
                    **common_kwargs,
                )

            return load_dataset(
                split=split,
                streaming=True,
                **common_kwargs,
            )

        if split is None:
            return load_dataset(
                **common_kwargs,
            )

        return load_dataset(
            split=split,
            **common_kwargs,
        )

    @staticmethod
    def get_num_rows(
        dataset: HFDatasetLike,
    ) -> int | dict[str, int] | None:
        """
        Extract row count metadata.

        Streaming datasets may not expose
        deterministic row counts.
        """
        if isinstance(dataset, Dataset):
            return dataset.num_rows

        if isinstance(dataset, DatasetDict):
            return {str(split): ds.num_rows for split, ds in dataset.items()}

        return None

    @staticmethod
    def get_features(
        dataset: HFDatasetLike,
    ) -> Features | dict[str, Features] | None:
        """
        Extract dataset feature/schema metadata.
        """
        if isinstance(
            dataset,
            (
                Dataset,
                IterableDataset,
            ),
        ):
            return dataset.features

        if isinstance(
            dataset,
            (
                DatasetDict,
                IterableDatasetDict,
            ),
        ):
            feature_mapping: dict[str, Features] = {}

            for split, ds in dataset.items():
                features = ds.features

                if features is not None:
                    feature_mapping[str(split)] = features

            return feature_mapping

        return None

    @staticmethod
    def get_fingerprint(
        dataset: HFDatasetLike,
    ) -> str | dict[str, str] | None:
        """
        Extract dataset fingerprint metadata.

        Streaming datasets may not expose
        fingerprints.
        """
        if isinstance(dataset, Dataset):
            fingerprint = getattr(
                dataset,
                "_fingerprint",
                None,
            )

            return str(fingerprint) if fingerprint is not None else None

        if isinstance(dataset, DatasetDict):
            fingerprint_mapping: dict[str, str] = {}

            for split, ds in dataset.items():
                fingerprint = getattr(
                    ds,
                    "_fingerprint",
                    None,
                )

                if fingerprint is not None:
                    fingerprint_mapping[str(split)] = str(fingerprint)

            return fingerprint_mapping

        return None

    @staticmethod
    def get_revision(
        dataset: HFDatasetLike,
    ) -> str | None:
        """
        Attempt to extract dataset revision/version.
        """
        info = getattr(
            dataset,
            "info",
            None,
        )

        if info is None:
            return None

        version = getattr(
            info,
            "version",
            None,
        )

        if version is None:
            return None

        return str(version)

    def _resolve_token(
        self,
    ) -> str | None:
        """
        Resolve Hugging Face token.

        Precedence order:
        1. explicit token field
        2. token_path file
        3. HF_TOKEN environment variable
        """
        if self.token:
            return self.token

        if self.token_path:
            token_file = Path(self.token_path)

            if token_file.exists():
                return token_file.read_text(
                    encoding="utf-8",
                ).strip()

        return os.getenv("HF_TOKEN")

    def _configure_offline_mode(
        self,
    ) -> None:
        """
        Configure Hugging Face offline mode.
        """
        if self.offline:
            os.environ["HF_HUB_OFFLINE"] = "1"
        else:
            os.environ.pop(
                "HF_HUB_OFFLINE",
                None,
            )
