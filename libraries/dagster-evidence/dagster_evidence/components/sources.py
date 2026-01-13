"""Source classes for Evidence projects."""

from abc import abstractmethod
from dataclasses import dataclass
from typing import Any

import dagster as dg
from dagster._serdes import whitelist_for_serdes


@whitelist_for_serdes
@dataclass
class SourceQuery:
    """Represents a single SQL query in an Evidence source."""

    name: str
    content: str


@whitelist_for_serdes
@dataclass
class SourceConnection:
    """Represents connection configuration for an Evidence source."""

    type: str
    extra: dict[str, Any]  # Additional connection-specific fields


@whitelist_for_serdes
@dataclass
class SourceContent:
    """Represents the full content of an Evidence source."""

    connection: SourceConnection
    queries: list[SourceQuery]

    @staticmethod
    def from_dict(data: dict[str, Any]) -> "SourceContent":
        """Create SourceContent from a raw dictionary."""
        connection_data = data.get("connection", {})
        connection = SourceConnection(
            type=connection_data.get("type", ""),
            extra={k: v for k, v in connection_data.items() if k != "type"},
        )
        queries = [
            SourceQuery(name=q.get("name", ""), content=q.get("content", ""))
            for q in data.get("queries", [])
        ]
        return SourceContent(connection=connection, queries=queries)


@dataclass
class BaseEvidenceProjectSource:
    source_content: SourceContent

    @staticmethod
    @abstractmethod
    def get_source_type() -> str:
        raise NotImplementedError()

    @abstractmethod
    def get_source_asset_specs(self, source_group: str) -> list[dg.AssetSpec]:
        raise NotImplementedError()

    @staticmethod
    def resolve_source_type(
        source_content: SourceContent,
    ) -> "BaseEvidenceProjectSource":
        """Resolve source content to the appropriate source class instance."""
        source_type = source_content.connection.type

        if source_type == DuckdbEvidenceProjectSource.get_source_type():
            return DuckdbEvidenceProjectSource(source_content)

        if source_type == MotherDuckEvidenceProjectSource.get_source_type():
            return MotherDuckEvidenceProjectSource(source_content)

        if source_type == BigQueryEvidenceProjectSource.get_source_type():
            return BigQueryEvidenceProjectSource(source_content)

        raise NotImplementedError(f"Unknown source type: {source_type}")


class DuckdbEvidenceProjectSource(BaseEvidenceProjectSource):
    @staticmethod
    def get_source_type() -> str:
        return "duckdb"

    def get_source_asset_specs(self, source_group: str) -> list[dg.AssetSpec]:
        source_asset_specs: list[dg.AssetSpec] = []
        for query in self.source_content.queries:
            source_asset_specs.append(
                dg.AssetSpec(
                    query.name,
                    group_name=source_group,
                    kinds={"evidence", "source", self.get_source_type()},
                )
            )
        return source_asset_specs


class MotherDuckEvidenceProjectSource(BaseEvidenceProjectSource):
    @staticmethod
    def get_source_type() -> str:
        return "motherduck"

    def get_source_asset_specs(self, source_group: str) -> list[dg.AssetSpec]:
        source_asset_specs: list[dg.AssetSpec] = []
        for query in self.source_content.queries:
            source_asset_specs.append(
                dg.AssetSpec(
                    query.name,
                    group_name=source_group,
                    kinds={"evidence", "source", self.get_source_type()},
                )
            )
        return source_asset_specs


class BigQueryEvidenceProjectSource(BaseEvidenceProjectSource):
    @staticmethod
    def get_source_type() -> str:
        return "bigquery"

    def get_source_asset_specs(self, source_group: str) -> list[dg.AssetSpec]:
        source_asset_specs: list[dg.AssetSpec] = []
        for query in self.source_content.queries:
            source_asset_specs.append(
                dg.AssetSpec(
                    query.name,
                    group_name=source_group,
                    kinds={"evidence", "source", self.get_source_type()},
                )
            )
        return source_asset_specs
