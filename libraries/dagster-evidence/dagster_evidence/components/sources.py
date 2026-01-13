"""Source classes for Evidence projects.

This module defines the data structures used to represent Evidence project sources,
including queries, connections, and the translator data classes.
"""

from abc import abstractmethod
from collections.abc import Sequence
from dataclasses import dataclass
from typing import Any

from dagster import AssetKey
from dagster._annotations import beta, public
from dagster._record import record
from dagster._serdes import whitelist_for_serdes


@beta
@public
@whitelist_for_serdes
@dataclass
class SourceQuery:
    """Represents a single SQL query in an Evidence source.

    Attributes:
        name: The query name (derived from filename without .sql extension).
        content: The SQL query content.

    Example:

        A query file ``sources/orders_db/daily_orders.sql`` would be parsed as:

        .. code-block:: python

            SourceQuery(
                name="daily_orders",
                content="SELECT * FROM orders WHERE date = current_date"
            )
    """

    name: str
    content: str


@beta
@public
@whitelist_for_serdes
@dataclass
class SourceConnection:
    """Represents connection configuration for an Evidence source.

    This is parsed from the ``connection.yaml`` file in each source directory.

    Attributes:
        type: The source type identifier (e.g., "duckdb", "bigquery", "motherduck").
        extra: Additional connection-specific fields from the YAML file.

    Example:

        A ``connection.yaml`` file:

        .. code-block:: yaml

            type: duckdb
            filename: ./data/analytics.duckdb

        Would be parsed as:

        .. code-block:: python

            SourceConnection(
                type="duckdb",
                extra={"filename": "./data/analytics.duckdb"}
            )
    """

    type: str
    extra: dict[str, Any]  # Additional connection-specific fields


@beta
@public
@whitelist_for_serdes
@dataclass
class SourceContent:
    """Represents the full content of an Evidence source directory.

    A source directory contains a connection.yaml and one or more .sql query files.

    Attributes:
        connection: The connection configuration parsed from connection.yaml.
        queries: List of SQL queries parsed from .sql files.

    Example:

        Source directory structure:

        .. code-block:: text

            sources/orders_db/
            ├── connection.yaml
            ├── orders.sql
            └── customers.sql

        Would be parsed as:

        .. code-block:: python

            SourceContent(
                connection=SourceConnection(type="duckdb", extra={...}),
                queries=[
                    SourceQuery(name="orders", content="SELECT ..."),
                    SourceQuery(name="customers", content="SELECT ..."),
                ]
            )
    """

    connection: SourceConnection
    queries: list[SourceQuery]

    @public
    @staticmethod
    def from_dict(data: dict[str, Any]) -> "SourceContent":
        """Create SourceContent from a raw dictionary.

        Args:
            data: Dictionary containing "connection" and "queries" keys.

        Returns:
            A SourceContent instance.

        Example:

            .. code-block:: python

                data = {
                    "connection": {"type": "duckdb", "filename": "data.db"},
                    "queries": [
                        {"name": "orders", "content": "SELECT * FROM orders"}
                    ]
                }
                source = SourceContent.from_dict(data)
        """
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


@beta
@public
@record
class EvidenceSourceTranslatorData:
    """Data passed to the translator for generating source asset specs.

    This record contains all information needed to generate an AssetSpec
    for a single source query.

    Attributes:
        source_content: The full source content including connection and queries.
        source_group: The source folder name (e.g., "orders_db").
        query: The specific query being translated.

    Example:

        Used in custom translator implementations:

        .. code-block:: python

            from dagster_evidence import (
                DagsterEvidenceTranslator,
                EvidenceSourceTranslatorData,
            )
            import dagster as dg

            class CustomTranslator(DagsterEvidenceTranslator):
                def get_asset_spec(self, data):
                    if isinstance(data, EvidenceSourceTranslatorData):
                        # Access source information
                        source_type = data.source_content.connection.type
                        query_name = data.query.name
                        group = data.source_group
                        # Generate custom AssetSpec
                        return dg.AssetSpec(
                            key=dg.AssetKey([group, query_name]),
                            kinds={"evidence", source_type},
                        )
                    return super().get_asset_spec(data)
    """

    source_content: SourceContent
    source_group: str  # The source folder name (e.g., "orders_db")
    query: SourceQuery  # The specific query being translated


@beta
@public
@record
class EvidenceProjectTranslatorData:
    """Data passed to the translator for generating the main project asset spec.

    This record contains all information needed to generate an AssetSpec
    for the Evidence project build-and-deploy asset.

    Attributes:
        project_name: The name of the Evidence project.
        sources_by_id: Dictionary mapping source folder names to their content.
        source_deps: List of AssetKeys for source assets this project depends on.

    Example:

        Used in custom translator implementations:

        .. code-block:: python

            from dagster_evidence import (
                DagsterEvidenceTranslator,
                EvidenceProjectTranslatorData,
            )
            import dagster as dg

            class CustomTranslator(DagsterEvidenceTranslator):
                def get_asset_spec(self, data):
                    if isinstance(data, EvidenceProjectTranslatorData):
                        return dg.AssetSpec(
                            key=dg.AssetKey(["dashboards", data.project_name]),
                            kinds={"evidence", "dashboard"},
                            deps=data.source_deps,
                            metadata={"source_count": len(data.sources_by_id)},
                        )
                    return super().get_asset_spec(data)
    """

    project_name: str
    sources_by_id: dict[str, SourceContent]
    source_deps: Sequence[AssetKey]  # Dependencies on source assets


@beta
@public
@dataclass
class BaseEvidenceProjectSource:
    """Base class for Evidence project data sources.

    Subclass this to implement custom source types that can be registered
    with the translator's SOURCE_TYPE_REGISTRY.

    Attributes:
        source_content: The parsed source content from the Evidence project.

    Example:

        Implementing a custom PostgreSQL source:

        .. code-block:: python

            from dagster_evidence.components.sources import BaseEvidenceProjectSource

            class PostgresEvidenceProjectSource(BaseEvidenceProjectSource):
                @staticmethod
                def get_source_type() -> str:
                    return "postgres"

            # Register with translator
            from dagster_evidence import DagsterEvidenceTranslator

            class CustomTranslator(DagsterEvidenceTranslator):
                SOURCE_TYPE_REGISTRY = {
                    **DagsterEvidenceTranslator.SOURCE_TYPE_REGISTRY,
                    "postgres": PostgresEvidenceProjectSource,
                }
    """

    source_content: SourceContent

    @public
    @staticmethod
    @abstractmethod
    def get_source_type() -> str:
        """Return the source type identifier (e.g., 'duckdb').

        Returns:
            The source type string that matches the 'type' field in connection.yaml.
        """
        raise NotImplementedError()


@beta
@public
class DuckdbEvidenceProjectSource(BaseEvidenceProjectSource):
    """DuckDB source for Evidence projects.

    Handles Evidence sources configured with ``type: duckdb`` in connection.yaml.

    Example:

        connection.yaml for a DuckDB source:

        .. code-block:: yaml

            type: duckdb
            filename: ./data/analytics.duckdb
    """

    @staticmethod
    def get_source_type() -> str:
        return "duckdb"


@beta
@public
class MotherDuckEvidenceProjectSource(BaseEvidenceProjectSource):
    """MotherDuck source for Evidence projects.

    Handles Evidence sources configured with ``type: motherduck`` in connection.yaml.

    Example:

        connection.yaml for a MotherDuck source:

        .. code-block:: yaml

            type: motherduck
            token: ${MOTHERDUCK_TOKEN}
            database: my_database
    """

    @staticmethod
    def get_source_type() -> str:
        return "motherduck"


@beta
@public
class BigQueryEvidenceProjectSource(BaseEvidenceProjectSource):
    """BigQuery source for Evidence projects.

    Handles Evidence sources configured with ``type: bigquery`` in connection.yaml.

    Example:

        connection.yaml for a BigQuery source:

        .. code-block:: yaml

            type: bigquery
            project_id: my-gcp-project
            credentials: ${GOOGLE_APPLICATION_CREDENTIALS}
    """

    @staticmethod
    def get_source_type() -> str:
        return "bigquery"
