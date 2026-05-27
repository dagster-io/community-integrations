from __future__ import annotations

from dataclasses import dataclass
from typing import Final


DEFAULT_REVISION_PARTITION_KEY: Final[str] = "revision"
DEFAULT_CONFIG_PARTITION_KEY: Final[str] = "config"
DEFAULT_DATE_PARTITION_KEY: Final[str] = "date"


@dataclass(frozen=True, slots=True)
class HFPartitionMapping:
    """
    Lightweight partition mapping helper for Hugging Face datasets.

    This class intentionally avoids introducing Dagster-specific
    partition orchestration abstractions prematurely.

    It acts as a thin semantic mapping layer between:
    - Dagster partition keys
    - Hugging Face dataset concepts

    Supported mappings:
    - dataset revisions
    - dataset configs
    - date-based partitions

    Examples:
        HFPartitionMapping.from_revision("main")
        HFPartitionMapping.from_config("qqp")
        HFPartitionMapping.from_date("2025-01-01")
    """

    partition_type: str
    value: str

    @classmethod
    def from_revision(
        cls,
        revision: str,
    ) -> "HFPartitionMapping":
        """
        Create a revision-based partition mapping.

        Example:
            partition_key -> dataset revision/tag/commit hash
        """
        return cls(
            partition_type=DEFAULT_REVISION_PARTITION_KEY,
            value=revision,
        )

    @classmethod
    def from_config(
        cls,
        config: str,
    ) -> "HFPartitionMapping":
        """
        Create a config-based partition mapping.

        Example:
            partition_key -> dataset configuration
        """
        return cls(
            partition_type=DEFAULT_CONFIG_PARTITION_KEY,
            value=config,
        )

    @classmethod
    def from_date(
        cls,
        date: str,
    ) -> "HFPartitionMapping":
        """
        Create a date-based partition mapping.

        Useful for:
        - streaming datasets
        - rolling corpora
        - synthetic dataset snapshots
        """
        return cls(
            partition_type=DEFAULT_DATE_PARTITION_KEY,
            value=date,
        )

    def to_partition_key(self) -> str:
        """
        Convert mapping into a deterministic Dagster partition key.

        Format:
            <partition_type>:<value>

        Examples:
            revision:main
            config:qqp
            date:2025-01-01
        """
        return f"{self.partition_type}:{self.value}"

    @classmethod
    def from_partition_key(
        cls,
        partition_key: str,
    ) -> "HFPartitionMapping":
        """
        Parse a Dagster partition key into a mapping object.

        Expected format:
            <partition_type>:<value>
        """
        if ":" not in partition_key:
            raise ValueError(
                "Invalid partition key format. " "Expected '<partition_type>:<value>'."
            )

        partition_type, value = partition_key.split(
            ":",
            maxsplit=1,
        )

        return cls(
            partition_type=partition_type,
            value=value,
        )

    @property
    def is_revision(self) -> bool:
        return self.partition_type == DEFAULT_REVISION_PARTITION_KEY

    @property
    def is_config(self) -> bool:
        return self.partition_type == DEFAULT_CONFIG_PARTITION_KEY

    @property
    def is_date(self) -> bool:
        return self.partition_type == DEFAULT_DATE_PARTITION_KEY
