"""Source classes for Evidence projects.

This module defines the data structures used to represent Evidence project sources,
including queries, connections, and the translator data classes.
"""

from abc import abstractmethod
from collections.abc import Sequence
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

import dagster as dg
from dagster import AssetKey
from dagster._annotations import beta, public
from dagster._record import record
from dagster._serdes import whitelist_for_serdes

if TYPE_CHECKING:
    from .sources import EvidenceSourceTranslatorData


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
        extracted_data: Additional data extracted from the source (e.g., table dependencies).

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
                        # Access extracted table dependencies
                        table_deps = data.extracted_data.get("table_deps", [])
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
    extracted_data: dict[str, Any] = {}  # Additional extracted data (e.g., table_deps)


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

    @public
    @classmethod
    @abstractmethod
    def extract_data_from_source(
        cls, data: "EvidenceSourceTranslatorData"
    ) -> dict[str, Any]:
        """Extract additional data from the source query.

        This method is called before get_asset_spec to extract information
        from the SQL query and connection configuration. The extracted data
        is stored in data.extracted_data and can be used in get_asset_spec.

        Common extracted data includes table dependencies parsed from the SQL query.

        Args:
            data: The translator data containing source and query information.

        Returns:
            Dictionary of extracted data. Common keys include:
            - table_deps: List of table references extracted from the SQL query.

        Example:

            .. code-block:: python

                class PostgresEvidenceProjectSource(BaseEvidenceProjectSource):
                    @classmethod
                    def extract_data_from_source(cls, data):
                        from dagster_evidence.utils import extract_table_references
                        table_refs = extract_table_references(
                            data.query.content,
                            default_schema="public",
                        )
                        return {"table_deps": table_refs}
        """
        raise NotImplementedError()

    @public
    @classmethod
    @abstractmethod
    def get_source_asset(cls, data: "EvidenceSourceTranslatorData") -> dg.AssetsDefinition:
        """Get the AssetsDefinition for a source query.

        Each source type must implement this method to define how its
        assets are represented in Dagster. The returned asset includes
        an automation condition that triggers when upstream dependencies
        are updated.

        Args:
            data: The translator data containing source and query information.
                  The extracted_data field contains data from extract_data_from_source.

        Returns:
            The AssetsDefinition for the source query with automation condition.

        Example:

            .. code-block:: python

                class PostgresEvidenceProjectSource(BaseEvidenceProjectSource):
                    @staticmethod
                    def get_source_type() -> str:
                        return "postgres"

                    @classmethod
                    def get_source_asset(cls, data):
                        # Use extracted table dependencies
                        deps = []
                        for ref in data.extracted_data.get("table_deps", []):
                            if ref.get("table"):
                                deps.append(dg.AssetKey([ref["table"]]))

                        key = dg.AssetKey(["postgres", data.query.name])
                        has_deps = bool(deps)

                        @dg.asset(
                            key=key,
                            group_name=data.source_group,
                            kinds={"evidence", "postgres"},
                            deps=deps,
                            automation_condition=dg.AutomationCondition.any_deps_match(
                                dg.AutomationCondition.newly_updated()
                            ) if has_deps else None,
                        )
                        def _source_asset():
                            return dg.MaterializeResult()

                        return _source_asset
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

    @classmethod
    def extract_data_from_source(
        cls, data: "EvidenceSourceTranslatorData"
    ) -> dict[str, Any]:
        """Extract table references from DuckDB source query."""
        from dagster_evidence.utils import extract_table_references

        options = data.source_content.connection.extra.get("options", {})
        # For DuckDB, database can be inferred from filename (without .duckdb extension)
        filename = options.get("filename", "")
        default_database = filename.replace(".duckdb", "") if filename else None
        default_schema = "main"  # DuckDB default schema

        table_refs = extract_table_references(
            data.query.content,
            default_database=default_database,
            default_schema=default_schema,
        )
        return {"table_deps": table_refs}

    @classmethod
    def get_source_asset(cls, data: "EvidenceSourceTranslatorData") -> dg.AssetsDefinition:
        """Get the AssetsDefinition for a DuckDB source query."""
        deps = []
        for ref in data.extracted_data.get("table_deps", []):
            if ref.get("table"):
                deps.append(dg.AssetKey([ref["table"]]))

        key = dg.AssetKey([data.source_group, data.query.name])
        group_name = data.source_group
        has_deps = bool(deps)

        @dg.asset(
            key=key,
            group_name=group_name,
            kinds={"evidence", "source", "duckdb"},
            deps=deps,
            automation_condition=dg.AutomationCondition.any_deps_match(
                dg.AutomationCondition.newly_updated()
            ) if has_deps else None,
        )
        def _source_asset():
            return dg.MaterializeResult()

        return _source_asset


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

    @classmethod
    def extract_data_from_source(
        cls, data: "EvidenceSourceTranslatorData"
    ) -> dict[str, Any]:
        """Extract table references from MotherDuck source query."""
        from dagster_evidence.utils import extract_table_references

        # Get database from connection config options
        options = data.source_content.connection.extra.get("options", {})
        default_database = options.get("database")
        default_schema = "main"  # MotherDuck default schema

        table_refs = extract_table_references(
            data.query.content,
            default_database=default_database,
            default_schema=default_schema,
        )
        return {"table_deps": table_refs}

    @classmethod
    def get_source_asset(cls, data: "EvidenceSourceTranslatorData") -> dg.AssetsDefinition:
        """Get the AssetsDefinition for a MotherDuck source query."""
        deps = []
        for ref in data.extracted_data.get("table_deps", []):
            if ref.get("table"):
                deps.append(dg.AssetKey([ref["table"]]))

        key = dg.AssetKey([data.source_group, data.query.name])
        group_name = data.source_group
        has_deps = bool(deps)

        @dg.asset(
            key=key,
            group_name=group_name,
            kinds={"evidence", "source", "motherduck"},
            deps=deps,
            automation_condition=dg.AutomationCondition.any_deps_match(
                dg.AutomationCondition.newly_updated()
            ) if has_deps else None,
        )
        def _source_asset():
            return dg.MaterializeResult()

        return _source_asset


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

    @classmethod
    def extract_data_from_source(
        cls, data: "EvidenceSourceTranslatorData"
    ) -> dict[str, Any]:
        """Extract table references from BigQuery source query."""
        from dagster_evidence.utils import extract_table_references

        # Get project and dataset from connection config options
        options = data.source_content.connection.extra.get("options", {})
        default_database = options.get("project_id")
        default_schema = options.get("dataset")

        table_refs = extract_table_references(
            data.query.content,
            default_database=default_database,
            default_schema=default_schema,
        )
        return {"table_deps": table_refs}

    @classmethod
    def get_source_asset(cls, data: "EvidenceSourceTranslatorData") -> dg.AssetsDefinition:
        """Get the AssetsDefinition for a BigQuery source query."""
        deps = []
        for ref in data.extracted_data.get("table_deps", []):
            if ref.get("table"):
                deps.append(dg.AssetKey([ref["table"]]))

        key = dg.AssetKey([data.source_group, data.query.name])
        group_name = data.source_group
        has_deps = bool(deps)

        @dg.asset(
            key=key,
            group_name=group_name,
            kinds={"evidence", "source", "bigquery"},
            deps=deps,
            automation_condition=dg.AutomationCondition.any_deps_match(
                dg.AutomationCondition.newly_updated()
            ) if has_deps else None,
        )
        def _source_asset():
            return dg.MaterializeResult()

        return _source_asset


@beta
@public
class GSheetsEvidenceProjectSource(BaseEvidenceProjectSource):
    """Google Sheets source for Evidence projects.

    Handles Evidence sources configured with ``type: gsheets`` in connection.yaml.
    Unlike SQL-based sources, gsheets sources define sheets and pages declaratively
    rather than using SQL queries.

    Example:

        connection.yaml for a Google Sheets source:

        .. code-block:: yaml

            name: my_sheets
            type: gsheets
            options:
              ratelimitms: 2500
            sheets:
              sales_data:
                id: 1Sc4nyLSSNETSIEpNKzheh5AFJJ-YA-wQeubFgeeEw9g
                pages:
                  - q1_sales
                  - q2_sales
              inventory:
                id: kj235Bo3wRFG9kj3tp98grnPB-P97iu87lv877gliuId

        This generates assets:
        - ``[source_group, "sales_data", "q1_sales"]``
        - ``[source_group, "sales_data", "q2_sales"]``
        - ``[source_group, "inventory"]``
    """

    @staticmethod
    def get_source_type() -> str:
        return "gsheets"

    @classmethod
    def extract_data_from_source(
        cls, data: "EvidenceSourceTranslatorData"
    ) -> dict[str, Any]:
        """Extract sheet configuration from Google Sheets source.

        Google Sheets sources don't have SQL to parse, so this returns
        the sheets configuration for use in get_asset_spec.
        """
        sheets_config = data.source_content.connection.extra.get("sheets", {})
        return {"sheets_config": sheets_config}

    @classmethod
    def get_source_asset(cls, data: "EvidenceSourceTranslatorData") -> dg.AssetsDefinition:
        """Get the AssetsDefinition for a Google Sheets source.

        Parses the query name to extract sheet_name and optional page_name,
        then builds a 3-part asset key: [source_group, sheet_name, page_name].
        """
        # Parse query.name to get sheet_name and optional page_name
        # Format: "sheet_name" or "sheet_name/page_name"
        parts = data.query.name.split("/", 1)
        sheet_name = parts[0]
        page_name = parts[1] if len(parts) > 1 else None

        # Build asset key: [source_group, sheet_name, page_name] or [source_group, sheet_name]
        if page_name:
            key = dg.AssetKey([data.source_group, sheet_name, page_name])
        else:
            key = dg.AssetKey([data.source_group, sheet_name])

        group_name = data.source_group

        @dg.asset(
            key=key,
            group_name=group_name,
            kinds={"evidence", "source", "gsheets"},
            deps=[],  # No upstream deps for gsheets - they are source of truth
        )
        def _source_asset():
            return dg.MaterializeResult()

        return _source_asset

    @classmethod
    def build_queries_from_sheets_config(
        cls, connection: dict[str, Any]
    ) -> list[dict[str, str]]:
        """Build virtual queries from sheets configuration.

        This method synthesizes SourceQuery-compatible dictionaries from
        the sheets configuration in connection.yaml. Each sheet/page
        combination becomes a "virtual query" with an empty content field.

        Args:
            connection: The full connection configuration dictionary.

        Returns:
            List of query dictionaries with "name" and "content" keys.
        """
        queries: list[dict[str, str]] = []
        sheets = connection.get("sheets", {})
        for sheet_name, sheet_config in sheets.items():
            if not isinstance(sheet_config, dict):
                continue
            pages = sheet_config.get("pages", [])
            if pages:
                for page in pages:
                    queries.append({"name": f"{sheet_name}/{page}", "content": ""})
            else:
                # No pages specified - create single asset for the sheet
                queries.append({"name": sheet_name, "content": ""})
        return queries
