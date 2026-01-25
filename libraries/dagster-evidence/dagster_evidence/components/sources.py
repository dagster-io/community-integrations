"""Source classes for Evidence projects.

This module defines the data structures used to represent Evidence project sources,
including queries, connections, and the translator data classes.
"""

import os
from abc import abstractmethod
from collections.abc import Sequence
from dataclasses import dataclass, field
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
class SourceDagsterMetadata:
    """Dagster-specific metadata for Evidence sources.

    Parsed from the ``meta.dagster`` section of connection.yaml.

    Attributes:
        create_source_sensor: Override whether sensors are created for this source.
            If None, uses the source type's default (get_source_sensor_enabled_default).
        hide_source_asset: Override whether this source's assets are hidden.
            If None, uses the source type's default (get_hide_source_asset_default).
        group_name: Override the asset group name for this source.
            If None, uses the source folder name.

    Example:

        A ``connection.yaml`` file with Dagster metadata:

        .. code-block:: yaml

            name: motherduck_source
            type: motherduck
            options:
              database: analytics
            meta:
              dagster:
                create_source_sensor: false
                hide_source_asset: false
                group_name: analytics_sources

        Would be parsed as:

        .. code-block:: python

            SourceDagsterMetadata(
                create_source_sensor=False,
                hide_source_asset=False,
                group_name="analytics_sources"
            )
    """

    create_source_sensor: bool | None = None
    hide_source_asset: bool | None = None
    group_name: str | None = None


@beta
@public
@whitelist_for_serdes
@dataclass
class ProjectDagsterMetadata:
    """Dagster-specific metadata for Evidence projects.

    Parsed from the ``meta.dagster`` section of evidence.config.yaml.

    Attributes:
        group_name: Override the asset group name for the project asset.
            If None, uses Dagster's default grouping.

    Example:

        An ``evidence.config.yaml`` file with Dagster metadata:

        .. code-block:: yaml

            deployment:
              basePath: /sales-dashboard

            meta:
              dagster:
                group_name: dashboards

        Would be parsed as:

        .. code-block:: python

            ProjectDagsterMetadata(
                group_name="dashboards"
            )
    """

    group_name: str | None = None


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
        dagster_metadata: Dagster-specific metadata parsed from meta.dagster section.

    Example:

        A ``connection.yaml`` file:

        .. code-block:: yaml

            type: duckdb
            filename: ./data/analytics.duckdb

        Would be parsed as:

        .. code-block:: python

            SourceConnection(
                type="duckdb",
                extra={"filename": "./data/analytics.duckdb"},
                dagster_metadata=SourceDagsterMetadata()
            )
    """

    type: str
    extra: dict[str, Any]  # Additional connection-specific fields
    dagster_metadata: SourceDagsterMetadata = field(
        default_factory=lambda: SourceDagsterMetadata()
    )


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

            With Dagster metadata:

            .. code-block:: python

                data = {
                    "connection": {
                        "type": "duckdb",
                        "filename": "data.db",
                        "meta": {
                            "dagster": {
                                "create_source_sensor": False,
                                "hide_source_asset": False,
                                "group_name": "custom_group"
                            }
                        }
                    },
                    "queries": [...]
                }
        """
        connection_data = data.get("connection", {})
        # Parse dagster metadata from meta.dagster section
        meta = connection_data.get("meta", {})
        dagster_meta = meta.get("dagster", {})
        dagster_metadata = SourceDagsterMetadata(
            create_source_sensor=dagster_meta.get("create_source_sensor"),
            hide_source_asset=dagster_meta.get("hide_source_asset"),
            group_name=dagster_meta.get("group_name"),
        )
        connection = SourceConnection(
            type=connection_data.get("type", ""),
            extra={
                k: v for k, v in connection_data.items() if k not in ("type", "meta")
            },
            dagster_metadata=dagster_metadata,
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
    source_path: str | None = (
        None  # Absolute path to source directory (for resolving relative paths)
    )

    @public
    @property
    def effective_group_name(self) -> str:
        """Get the effective group name, considering metadata override.

        Returns the group_name from dagster metadata if set, otherwise
        returns the source_group (folder name).

        Returns:
            The effective group name to use for asset grouping.
        """
        meta = self.source_content.connection.dagster_metadata
        return meta.group_name if meta.group_name else self.source_group


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
        dagster_metadata: Dagster-specific metadata parsed from evidence.config.yaml.

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
    dagster_metadata: ProjectDagsterMetadata = ProjectDagsterMetadata()

    @public
    @property
    def effective_group_name(self) -> str | None:
        """Get the effective group name from metadata, or None for default.

        Returns the group_name from dagster metadata if set, otherwise
        returns None to use Dagster's default grouping.

        Returns:
            The effective group name to use for asset grouping, or None.
        """
        return self.dagster_metadata.group_name


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
    @classmethod
    def get_hide_source_asset_default(cls) -> bool:
        """Return whether this source type should hide its assets by default.

        When enabled via ``enable_source_assets_hiding`` on the project, sources
        that return True here will not create intermediate source assets. Instead,
        their table dependencies (extracted from SQL) are linked directly to the
        project asset.

        Override in subclasses to change the default behavior.

        Returns:
            True to hide source assets by default, False to show them.
        """
        return False

    @public
    @classmethod
    def get_source_sensor_enabled_default(cls) -> bool:
        """Return whether sensors are enabled by default for this source type.

        When enabled via ``enable_source_sensors`` on the project, sources
        that return True here will have sensors created to detect changes
        in the underlying data.

        Override in subclasses to enable sensor support.

        Returns:
            True to enable sensors by default, False to disable them.
        """
        return False

    @public
    def get_hide_source_asset(self) -> bool:
        """Return whether this source should hide its assets.

        Checks per-source metadata override first (meta.dagster.hide_source_asset),
        then falls back to the class default (get_hide_source_asset_default).

        Returns:
            True to hide source assets, False to show them.
        """
        meta = self.source_content.connection.dagster_metadata
        if meta.hide_source_asset is not None:
            return meta.hide_source_asset
        return self.get_hide_source_asset_default()

    @public
    def get_source_sensor_enabled(self) -> bool:
        """Return whether sensors are enabled for this source.

        Checks per-source metadata override first (meta.dagster.create_source_sensor),
        then falls back to the class default (get_source_sensor_enabled_default).

        Returns:
            True to enable sensors, False to disable them.
        """
        meta = self.source_content.connection.dagster_metadata
        if meta.create_source_sensor is not None:
            return meta.create_source_sensor
        return self.get_source_sensor_enabled_default()

    @public
    @classmethod
    def get_source_sensor(
        cls,
        data: "EvidenceSourceTranslatorData",
        asset_key: dg.AssetKey,
    ) -> dg.SensorDefinition | None:
        """Get a sensor for this source to detect data changes.

        Override in subclasses to implement source-specific change detection.
        The sensor should detect changes in the underlying data and trigger
        the source asset materialization when changes are detected.

        Args:
            data: The translator data containing source and query information.
            asset_key: The asset key of the source asset to trigger.

        Returns:
            A SensorDefinition that monitors for changes, or None if not supported.
        """
        return None

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
    def get_source_asset(
        cls, data: "EvidenceSourceTranslatorData"
    ) -> dg.AssetsDefinition:
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

    @classmethod
    def get_hide_source_asset_default(cls) -> bool:
        return False

    @classmethod
    def get_source_sensor_enabled_default(cls) -> bool:
        return False

    @staticmethod
    def get_source_type() -> str:
        return "duckdb"

    @classmethod
    def get_source_sensor(
        cls,
        data: "EvidenceSourceTranslatorData",
        asset_key: dg.AssetKey,
    ) -> dg.SensorDefinition | None:
        """Get a sensor that monitors DuckDB tables for changes.

        Uses information_schema queries with read-only connection to detect
        changes in table row counts.
        """
        import json

        options = data.source_content.connection.extra.get("options", {})
        db_path = options.get("filename")
        if not db_path:
            return None

        # Resolve relative path against source_path
        if data.source_path and not os.path.isabs(db_path):
            db_path = os.path.join(data.source_path, db_path)

        table_deps = data.extracted_data.get("table_deps", [])
        if not table_deps:
            return None

        source_group = data.source_group
        query_name = data.query.name
        sensor_name = f"{source_group}_{query_name}_sensor"

        @dg.sensor(name=sensor_name, asset_selection=[asset_key])
        def duckdb_sensor(context: dg.SensorEvaluationContext):
            try:
                import duckdb
            except ImportError:
                raise ImportError(
                    "duckdb is required for DuckDB sensors. "
                    "Install it with: pip install dagster-evidence[duckdb]"
                ) from None

            try:
                conn = duckdb.connect(db_path, read_only=True)
            except Exception as e:
                raise Exception(f"Could not connect to DuckDB: {e}") from e

            try:
                table_counts: dict[str, int] = {}
                for ref in table_deps:
                    table_name = ref.get("table")
                    schema = ref.get("schema", "main")
                    if table_name:
                        try:
                            result = conn.execute(
                                """
                                SELECT estimated_size
                                FROM duckdb_tables()
                                WHERE table_name = ? AND schema_name = ?
                            """,
                                [table_name, schema],
                            ).fetchone()
                            table_counts[f"{schema}.{table_name}"] = (
                                result[0] if result else 0
                            )
                        except Exception:
                            table_counts[f"{schema}.{table_name}"] = 0
            finally:
                conn.close()

            cursor = json.loads(context.cursor) if context.cursor else {}
            last_counts = cursor.get("counts", {})

            if table_counts != last_counts:
                context.update_cursor(json.dumps({"counts": table_counts}))
                yield dg.RunRequest(asset_selection=[asset_key])

        return duckdb_sensor

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
    def get_source_asset(
        cls, data: "EvidenceSourceTranslatorData"
    ) -> dg.AssetsDefinition:
        """Get the AssetsDefinition for a DuckDB source query."""
        deps = []
        for ref in data.extracted_data.get("table_deps", []):
            if ref.get("table"):
                deps.append(dg.AssetKey([ref["table"]]))

        key = dg.AssetKey([data.source_group, data.query.name])
        group_name = data.effective_group_name
        has_deps = bool(deps)

        @dg.asset(
            key=key,
            group_name=group_name,
            kinds={"evidence", "source", "duckdb"},
            deps=deps,
            automation_condition=dg.AutomationCondition.any_deps_match(
                dg.AutomationCondition.newly_updated()
            )
            if has_deps
            else None,
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

    @classmethod
    def get_hide_source_asset_default(cls) -> bool:
        return False

    @classmethod
    def get_source_sensor_enabled_default(cls) -> bool:
        return False

    @staticmethod
    def get_source_type() -> str:
        return "motherduck"

    @classmethod
    def get_source_sensor(
        cls,
        data: "EvidenceSourceTranslatorData",
        asset_key: dg.AssetKey,
    ) -> dg.SensorDefinition | None:
        """Get a sensor that monitors MotherDuck tables for changes.

        Uses information_schema queries with read-only connection to detect
        changes in table row counts.
        """
        import json
        import os

        options = data.source_content.connection.extra.get("options", {})
        database = options.get("database")
        token = options.get("token") or os.environ.get("MOTHERDUCK_TOKEN")

        if not database or not token:
            return None

        table_deps = data.extracted_data.get("table_deps", [])
        if not table_deps:
            return None

        source_group = data.source_group
        query_name = data.query.name
        sensor_name = f"{source_group}_{query_name}_sensor"

        @dg.sensor(name=sensor_name, asset_selection=[asset_key])
        def motherduck_sensor(context: dg.SensorEvaluationContext):
            try:
                import duckdb
            except ImportError:
                raise ImportError(
                    "duckdb is required for MotherDuck sensors. "
                    "Install it with: pip install dagster-evidence[duckdb]"
                ) from None

            md_token = os.environ.get("MOTHERDUCK_TOKEN", token)
            connection_string = f"md:{database}?motherduck_token={md_token}"

            try:
                conn = duckdb.connect(connection_string, read_only=True)
            except Exception as e:
                raise Exception(f"Could not connect to MotherDuck: {e}") from e

            try:
                table_counts: dict[str, int] = {}
                for ref in table_deps:
                    table_name = ref.get("table")
                    schema = ref.get("schema", "main")
                    if table_name:
                        try:
                            result = conn.execute(
                                """
                                SELECT estimated_size
                                FROM duckdb_tables()
                                WHERE table_name = ? AND schema_name = ?
                            """,
                                [table_name, schema],
                            ).fetchone()
                            table_counts[f"{schema}.{table_name}"] = (
                                result[0] if result else 0
                            )
                        except Exception:
                            table_counts[f"{schema}.{table_name}"] = 0
            finally:
                conn.close()

            cursor = json.loads(context.cursor) if context.cursor else {}
            last_counts = cursor.get("counts", {})

            if table_counts != last_counts:
                context.update_cursor(json.dumps({"counts": table_counts}))
                yield dg.RunRequest(asset_selection=[asset_key])

        return motherduck_sensor

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
    def get_source_asset(
        cls, data: "EvidenceSourceTranslatorData"
    ) -> dg.AssetsDefinition:
        """Get the AssetsDefinition for a MotherDuck source query."""
        deps = []
        for ref in data.extracted_data.get("table_deps", []):
            if ref.get("table"):
                deps.append(dg.AssetKey([ref["table"]]))

        key = dg.AssetKey([data.source_group, data.query.name])
        group_name = data.effective_group_name
        has_deps = bool(deps)

        @dg.asset(
            key=key,
            group_name=group_name,
            kinds={"evidence", "source", "motherduck"},
            deps=deps,
            automation_condition=dg.AutomationCondition.any_deps_match(
                dg.AutomationCondition.newly_updated()
            )
            if has_deps
            else None,
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

    @classmethod
    def get_hide_source_asset_default(cls) -> bool:
        return True

    @classmethod
    def get_source_sensor_enabled_default(cls) -> bool:
        return False

    @staticmethod
    def get_source_type() -> str:
        return "bigquery"

    @classmethod
    def get_source_sensor(
        cls,
        data: "EvidenceSourceTranslatorData",
        asset_key: dg.AssetKey,
    ) -> dg.SensorDefinition | None:
        """Get a sensor that monitors BigQuery tables for changes.

        Uses BigQuery API to check table.modified timestamps.
        """
        import json

        options = data.source_content.connection.extra.get("options", {})
        project_id = options.get("project_id")

        if not project_id:
            return None

        table_deps = data.extracted_data.get("table_deps", [])
        if not table_deps:
            return None

        source_group = data.source_group
        query_name = data.query.name
        sensor_name = f"{source_group}_{query_name}_sensor"

        @dg.sensor(name=sensor_name, asset_selection=[asset_key])
        def bigquery_sensor(context: dg.SensorEvaluationContext):
            try:
                from google.cloud import bigquery
            except ImportError:
                raise ImportError(
                    "google-cloud-bigquery is required for BigQuery sensors. "
                    "Install it with: pip install dagster-evidence[bigquery]"
                ) from None

            try:
                client = bigquery.Client(project=project_id)
            except Exception as e:
                raise Exception(f"Could not connect to BigQuery: {e}") from e

            mod_times: dict[str, str] = {}
            for ref in table_deps:
                table_name = ref.get("table")
                dataset = ref.get("schema")
                if table_name and dataset:
                    try:
                        table_ref = f"{project_id}.{dataset}.{table_name}"
                        table = client.get_table(table_ref)
                        if table.modified:
                            mod_times[table_ref] = table.modified.isoformat()
                    except Exception:
                        pass

            cursor = json.loads(context.cursor) if context.cursor else {}
            last_mod_times = cursor.get("mod_times", {})

            if mod_times != last_mod_times:
                context.update_cursor(json.dumps({"mod_times": mod_times}))
                yield dg.RunRequest(asset_selection=[asset_key])

        return bigquery_sensor

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
    def get_source_asset(
        cls, data: "EvidenceSourceTranslatorData"
    ) -> dg.AssetsDefinition:
        """Get the AssetsDefinition for a BigQuery source query."""
        deps = []
        for ref in data.extracted_data.get("table_deps", []):
            if ref.get("table"):
                deps.append(dg.AssetKey([ref["table"]]))

        key = dg.AssetKey([data.source_group, data.query.name])
        group_name = data.effective_group_name
        has_deps = bool(deps)

        @dg.asset(
            key=key,
            group_name=group_name,
            kinds={"evidence", "source", "bigquery"},
            deps=deps,
            automation_condition=dg.AutomationCondition.any_deps_match(
                dg.AutomationCondition.newly_updated()
            )
            if has_deps
            else None,
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

    @classmethod
    def get_source_sensor_enabled_default(cls) -> bool:
        return False

    @staticmethod
    def get_source_type() -> str:
        return "gsheets"

    @classmethod
    def get_source_sensor(
        cls,
        data: "EvidenceSourceTranslatorData",
        asset_key: dg.AssetKey,
    ) -> dg.SensorDefinition | None:
        """Get a sensor that monitors Google Sheets for changes.

        Uses Google Drive API to check modifiedTime and version.
        """
        import json

        options = data.source_content.connection.extra.get("options", {})
        service_account_path = options.get("service_account_path")
        sheets_config = data.source_content.connection.extra.get("sheets", {})

        # Parse query.name to get sheet_name
        parts = data.query.name.split("/", 1)
        sheet_name = parts[0]

        sheet_config = sheets_config.get(sheet_name, {})
        sheet_id = sheet_config.get("id") if isinstance(sheet_config, dict) else None

        if not sheet_id:
            return None

        source_group = data.source_group
        query_name = data.query.name.replace("/", "_")
        sensor_name = f"{source_group}_{query_name}_sensor"

        @dg.sensor(name=sensor_name, asset_selection=[asset_key])
        def gsheets_sensor(context: dg.SensorEvaluationContext):
            try:
                from google.oauth2 import service_account
                from googleapiclient.discovery import build
            except ImportError:
                raise ImportError(
                    "google-api-python-client is required for Google Sheets sensors. "
                    "Install it with: uv pip install 'dagster-evidence[gsheets]'"
                ) from None

            try:
                if service_account_path:
                    credentials = service_account.Credentials.from_service_account_file(
                        service_account_path,
                        scopes=["https://www.googleapis.com/auth/drive.readonly"],
                    )
                else:
                    # Use default credentials
                    import google.auth

                    credentials, _ = google.auth.default(
                        scopes=["https://www.googleapis.com/auth/drive.readonly"]
                    )

                service = build("drive", "v3", credentials=credentials)
                file_metadata = (
                    service.files()
                    .get(fileId=sheet_id, fields="modifiedTime,version")
                    .execute()
                )

                current_state = {
                    "modified_time": file_metadata.get("modifiedTime"),
                    "version": file_metadata.get("version"),
                }
            except Exception as e:
                raise Exception(f"Could not fetch Google Sheet metadata: {e}") from e

            cursor = json.loads(context.cursor) if context.cursor else {}

            if current_state.get("modified_time") != cursor.get(
                "modified_time"
            ) or current_state.get("version") != cursor.get("version"):
                context.update_cursor(json.dumps(current_state))
                yield dg.RunRequest(asset_selection=[asset_key])

        return gsheets_sensor

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
    def get_source_asset(
        cls, data: "EvidenceSourceTranslatorData"
    ) -> dg.AssetsDefinition:
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

        group_name = data.effective_group_name

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
