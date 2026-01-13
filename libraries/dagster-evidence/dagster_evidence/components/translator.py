"""Translator for Evidence project assets.

This module provides the translator class for converting Evidence project data
into Dagster AssetSpecs. Subclass DagsterEvidenceTranslator to customize
how Evidence sources and projects are represented in Dagster.
"""

from typing import Union

import dagster as dg
from dagster._annotations import beta, public

from .sources import (
    BaseEvidenceProjectSource,
    BigQueryEvidenceProjectSource,
    DuckdbEvidenceProjectSource,
    EvidenceProjectTranslatorData,
    EvidenceSourceTranslatorData,
    MotherDuckEvidenceProjectSource,
)


@beta
@public
class DagsterEvidenceTranslator:
    """Translator class which converts Evidence project data into AssetSpecs.

    Subclass this class to provide custom translation logic.

    Examples:
        Extend the source type registry via class attribute:

            class MyTranslator(DagsterEvidenceTranslator):
                SOURCE_TYPE_REGISTRY = {
                    **DagsterEvidenceTranslator.SOURCE_TYPE_REGISTRY,
                    "postgres": PostgresEvidenceProjectSource,
                }

        Or override get_source_type_registry() for dynamic configuration:

            class MyTranslator(DagsterEvidenceTranslator):
                def get_source_type_registry(self):
                    return {
                        **super().get_source_type_registry(),
                        "postgres": PostgresEvidenceProjectSource,
                    }

        Customize asset spec generation:

            class MyTranslator(DagsterEvidenceTranslator):
                def get_asset_spec(self, data):
                    spec = super().get_asset_spec(data)
                    if isinstance(data, EvidenceSourceTranslatorData):
                        return spec.replace_attributes(
                            key=spec.key.with_prefix("my_prefix"),
                        )
                    return spec
    """

    # Source type registry - maps source type string to source class
    # Subclasses can extend via class attribute or override get_source_type_registry()
    SOURCE_TYPE_REGISTRY: dict[str, type[BaseEvidenceProjectSource]] = {
        "duckdb": DuckdbEvidenceProjectSource,
        "motherduck": MotherDuckEvidenceProjectSource,
        "bigquery": BigQueryEvidenceProjectSource,
    }

    @public
    def get_source_type_registry(
        self,
    ) -> dict[str, type[BaseEvidenceProjectSource]]:
        """Get the source type registry mapping source types to source classes.

        Override this method to dynamically configure the source type registry.
        By default, returns the SOURCE_TYPE_REGISTRY class attribute.

        Returns:
            Dictionary mapping source type strings to source class types.

        Example:

            .. code-block:: python

                class CustomTranslator(DagsterEvidenceTranslator):
                    def get_source_type_registry(self):
                        return {
                            **super().get_source_type_registry(),
                            "postgres": PostgresEvidenceProjectSource,
                            "mysql": MySQLEvidenceProjectSource,
                        }
        """
        return self.SOURCE_TYPE_REGISTRY

    @public
    def get_source_class(self, source_type: str) -> type[BaseEvidenceProjectSource]:
        """Get the source class for a given source type.

        Args:
            source_type: The source type identifier (e.g., "duckdb", "bigquery").

        Returns:
            The source class for the given type.

        Raises:
            NotImplementedError: If the source type is not in the registry.
        """
        registry = self.get_source_type_registry()
        if source_type not in registry:
            raise NotImplementedError(f"Unknown source type: {source_type}")
        return registry[source_type]

    @public
    def get_asset_spec(
        self, data: Union[EvidenceSourceTranslatorData, EvidenceProjectTranslatorData]
    ) -> dg.AssetSpec:
        """Get the AssetSpec for an Evidence object (source query or project).

        Override this method to customize asset spec generation.

        Args:
            data: Either EvidenceSourceTranslatorData for source queries
                  or EvidenceProjectTranslatorData for the main project asset.

        Returns:
            The AssetSpec for the Evidence object.

        Example:

            .. code-block:: python

                class CustomTranslator(DagsterEvidenceTranslator):
                    def get_asset_spec(self, data):
                        spec = super().get_asset_spec(data)
                        if isinstance(data, EvidenceSourceTranslatorData):
                            return spec.replace_attributes(
                                key=spec.key.with_prefix("evidence"),
                                metadata={"source_type": data.source_content.connection.type},
                            )
                        return spec
        """
        if isinstance(data, EvidenceSourceTranslatorData):
            return self._get_source_asset_spec(data)
        elif isinstance(data, EvidenceProjectTranslatorData):
            return self._get_project_asset_spec(data)
        else:
            raise TypeError(f"Unknown data type: {type(data)}")

    def _get_source_asset_spec(
        self, data: EvidenceSourceTranslatorData
    ) -> dg.AssetSpec:
        """Default translation for source query assets."""
        source_type = data.source_content.connection.type
        return dg.AssetSpec(
            key=dg.AssetKey([data.query.name]),
            group_name=data.source_group,
            kinds={"evidence", "source", source_type},
        )

    def _get_project_asset_spec(
        self, data: EvidenceProjectTranslatorData
    ) -> dg.AssetSpec:
        """Default translation for main project asset."""
        return dg.AssetSpec(
            key=dg.AssetKey([data.project_name]),
            kinds={"evidence"},
            deps=data.source_deps,
        )
