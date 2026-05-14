from __future__ import annotations

from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Any

from datasets import Dataset
from huggingface_hub import (
    create_repo,
    upload_file,
)

from dagster_hf_datasets._export._dataset_card import (
    DatasetCardBuilder,
)


class HFDatasetPublisher:
    """
    Lightweight Hugging Face dataset publisher.

    Responsibilities:
    - create dataset repositories
    - push processed datasets to HF Hub
    - optionally generate lightweight dataset cards

    Explicit non-goals:
    - synchronization orchestration
    - advanced governance
    - streaming dataset persistence
    - dataset lifecycle automation
    """

    def __init__(
        self,
        *,
        repo_id: str,
        token: str | None = None,
        private: bool = False,
        exist_ok: bool = True,
    ) -> None:
        self.repo_id = repo_id
        self.token = token
        self.private = private
        self.exist_ok = exist_ok

    def publish(
        self,
        *,
        dataset: Dataset,
        source_dataset: str,
        source_revision: str | None = None,
        description: str | None = None,
        processing_steps: list[str] | None = None,
        metadata: dict[str, Any] | None = None,
        create_dataset_card: bool = True,
        commit_message: str = (
            "Upload dataset via dagster-hf-datasets"
        ),
    ) -> str:
        """
        Publish dataset to Hugging Face Hub.

        Returns:
            URL to the dataset repository.
        """
        create_repo(
            repo_id=self.repo_id,
            repo_type="dataset",
            token=self.token,
            private=self.private,
            exist_ok=self.exist_ok,
        )

        if create_dataset_card:
            self._upload_dataset_card(
                source_dataset=source_dataset,
                source_revision=source_revision,
                description=description,
                processing_steps=processing_steps,
                metadata=metadata,
                commit_message=commit_message,
            )

        dataset.push_to_hub(
            repo_id=self.repo_id,
            token=self.token,
            commit_message=commit_message,
        )

        return (
            f"https://huggingface.co/datasets/"
            f"{self.repo_id}"
        )

    def _upload_dataset_card(
        self,
        *,
        source_dataset: str,
        source_revision: str | None,
        description: str | None,
        processing_steps: list[str] | None,
        metadata: dict[str, Any] | None,
        commit_message: str,
    ) -> None:
        """
        Generate and upload lightweight README.md
        dataset card.
        """
        card_builder = DatasetCardBuilder(
            dataset_name=self.repo_id,
            source_dataset=source_dataset,
            source_revision=source_revision,
            description=description,
            processing_steps=processing_steps,
            metadata=metadata,
        )

        card_content = card_builder.build()

        with TemporaryDirectory() as temp_dir:
            card_path = (
                Path(temp_dir) / "README.md"
            )

            card_path.write_text(
                card_content,
                encoding="utf-8",
            )

            upload_file(
                path_or_fileobj=str(card_path),
                path_in_repo="README.md",
                repo_id=self.repo_id,
                repo_type="dataset",
                token=self.token,
                commit_message=(
                    f"{commit_message} "
                    "(dataset card)"
                ),
            )