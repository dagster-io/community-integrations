"""Source classes for Evidence projects."""

from abc import abstractmethod
from dataclasses import dataclass

import dagster as dg


@dataclass
class BaseEvidenceProjectSource:
    source_content: dict

    @staticmethod
    @abstractmethod
    def get_source_type():
        raise NotImplementedError()

    @abstractmethod
    def get_source_asset_specs(self, source_group: str):
        raise NotImplementedError()

    @staticmethod
    def resolve_source_type(source_content) -> "BaseEvidenceProjectSource":
        """Resolve source content to the appropriate source class instance."""
        source_type = source_content.get("connection").get("type")

        if source_type == DuckdbEvidenceProjectSource.get_source_type():
            return DuckdbEvidenceProjectSource(source_content)

        if source_type == MotherDuckEvidenceProjectSource.get_source_type():
            return MotherDuckEvidenceProjectSource(source_content)

        if source_type == BigQueryEvidenceProjectSource.get_source_type():
            return BigQueryEvidenceProjectSource(source_content)

        raise NotImplementedError(f"Unknown source type: {source_type}")


class DuckdbEvidenceProjectSource(BaseEvidenceProjectSource):
    @staticmethod
    def get_source_type():
        return "duckdb"

    def get_source_asset_specs(self, source_group: str):
        source_asset_specs = []
        for each_query in self.source_content.get("queries", []):
            source_asset_specs.append(
                dg.AssetSpec(
                    each_query.get("name"),
                    group_name=source_group,
                    kinds={"evidence", "source", self.get_source_type()},
                )
            )
        return source_asset_specs


class MotherDuckEvidenceProjectSource(BaseEvidenceProjectSource):
    @staticmethod
    def get_source_type():
        return "motherduck"

    def get_source_asset_specs(self, source_group: str):
        source_asset_specs = []
        for each_query in self.source_content.get("queries", []):
            source_asset_specs.append(
                dg.AssetSpec(
                    each_query.get("name"),
                    group_name=source_group,
                    kinds={"evidence", "source", self.get_source_type()},
                )
            )
        return source_asset_specs


class BigQueryEvidenceProjectSource(BaseEvidenceProjectSource):
    @staticmethod
    def get_source_type():
        return "bigquery"

    def get_source_asset_specs(self, source_group: str):
        source_asset_specs = []
        for each_query in self.source_content.get("queries", []):
            source_asset_specs.append(
                dg.AssetSpec(
                    each_query.get("name"),
                    group_name=source_group,
                    kinds={"evidence", "source", self.get_source_type()},
                )
            )
        return source_asset_specs
