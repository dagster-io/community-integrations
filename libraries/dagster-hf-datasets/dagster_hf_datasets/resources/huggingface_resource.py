from __future__ import annotations

import os
from pathlib import Path
from typing import Any

from dagster import ConfigurableResource
from datasets import (
    Dataset,
    DatasetDict,
    IterableDataset,
    IterableDatasetDict,
    load_dataset,
)
from huggingface_hub import login
from pydantic import Field

HFDatasetLike = (
    Dataset
    | DatasetDict
    | IterableDataset
    | IterableDatasetDict
)


class HuggingFaceResource(ConfigurableResource):
    """
    Dagster resource for interacting with Hugging Face Datasets.

    This resource intentionally preserves Hugging Face dataset semantics
    and delegates directly to `datasets.load_dataset(...)` rather than
    introducing custom loading abstractions.

    Responsibilities:
    - Authentication
    - Dataset loading
    - Cache configuration
    - Offline mode support
    - Lightweight metadata helpers
    """

    token: str | None = Field(
        default=None,
        description="Optional Hugging Face access token.",
    )

    token_path: str | None = Field(
        default=None,
        description="Optional path to a file containing a Hugging Face token.",
    )

    cache_dir: str | None = Field(
        default=None,
        description="Optional Hugging Face datasets cache directory.",
    )

    offline: bool = Field(
        default=False,
        description="Enable Hugging Face offline mode.",
    )

    def setup_for_execution(self, _: Any) -> None:
        """
        Configure authentication and environment before execution.
        """
        self._configure_offline_mode()

        resolved_token = self._resolve_token()

        if resolved_token:
            login(token=resolved_token, add_to_git_credential=False)

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
        Load a dataset using Hugging Face Datasets.

        This method intentionally mirrors the semantics of
        `datasets.load_dataset(...)`.

        Args:
            path:
                Dataset repository path or local dataset script.
            config:
                Dataset configuration name.
            split:
                Dataset split to load.
            revision:
                Dataset revision, tag, branch, or commit hash.
            streaming:
                Enable streaming mode.
            **kwargs:
                Additional keyword arguments forwarded directly to
                `datasets.load_dataset(...)`.

        Returns:
            A Hugging Face Dataset-compatible object.
        """
        return load_dataset(
            path=path,
            name=config,
            split=split,
            revision=revision,
            streaming=streaming,
            cache_dir=self.cache_dir,
            **kwargs,
        )

    @staticmethod
    def get_num_rows(dataset: HFDatasetLike) -> int | dict[str, int] | None:
        """
        Extract row count metadata from a dataset.

        Returns:
            Integer row count, mapping of split -> row count,
            or None if unavailable (e.g. streaming datasets).
        """
        if isinstance(dataset, Dataset):
            return dataset.num_rows

        if isinstance(dataset, DatasetDict):
            return {
                split: ds.num_rows
                for split, ds in dataset.items()
            }

        return None

    @staticmethod
    def get_features(
        dataset: HFDatasetLike,
    ) -> Any:
        """
        Extract feature/schema metadata from a dataset.

        Returns:
            Dataset features object or mapping of split -> features.
        """
        if isinstance(dataset, (Dataset, IterableDataset)):
            return dataset.features

        if isinstance(dataset, (DatasetDict, IterableDatasetDict)):
            return {
                split: ds.features
                for split, ds in dataset.items()
            }

        return None

    @staticmethod
    def get_fingerprint(
        dataset: HFDatasetLike,
    ) -> str | dict[str, str] | None:
        """
        Extract dataset fingerprint metadata.

        Returns:
            Dataset fingerprint or mapping of split -> fingerprint.
        """
        if isinstance(dataset, Dataset):
            return dataset._fingerprint

        if isinstance(dataset, DatasetDict):
            return {
                split: ds._fingerprint
                for split, ds in dataset.items()
            }

        return None

    @staticmethod
    def get_revision(dataset: HFDatasetLike) -> str | None:
        """
        Attempt to extract dataset revision metadata.

        Returns:
            Dataset revision string if available.
        """
        try:
            return dataset.info.version.version
        except AttributeError:
            return None

    def _resolve_token(self) -> str | None:
        """
        Resolve Hugging Face token using precedence order:

        1. Explicit token field
        2. token_path file
        3. HF_TOKEN environment variable
        """
        if self.token:
            return self.token

        if self.token_path:
            token_file = Path(self.token_path)

            if token_file.exists():
                return token_file.read_text().strip()

        return os.getenv("HF_TOKEN")

    def _configure_offline_mode(self) -> None:
        """
        Configure Hugging Face offline mode environment.
        """
        if self.offline:
            os.environ["HF_HUB_OFFLINE"] = "1"
        else:
            os.environ.pop("HF_HUB_OFFLINE", None)
