import os
from unittest.mock import MagicMock, patch

import pytest

from dagster_ducklake.ducklake import (
    DuckDBConfig,
    DuckLakeLocalDirectory,
    DuckLakeResource,
    PostgresConfig,
    S3Config,
    SqliteConfig,
)


class TestDuckDBConfig:
    """Tests for DuckDB metadata backend configuration."""

    def test_duckdb_config_creation(self):
        """Test basic DuckDB config instantiation."""
        config = DuckDBConfig(path="/tmp/metadata.db")
        assert config.path == "/tmp/metadata.db"

    def test_duckdb_sql_parts_generation(self):
        """Test SQL generation for DuckDB metadata backend."""
        config = DuckDBConfig(path="/tmp/metadata.db")
        secret_sql, metadata_sql = config.get_ducklake_sql_parts("test_alias")

        assert secret_sql == ""
        assert metadata_sql == "METADATA_PATH '/tmp/metadata.db'"


class TestSqliteConfig:
    """Tests for SQLite metadata backend configuration."""

    def test_sqlite_config_creation(self):
        """Test basic SQLite config instantiation."""
        config = SqliteConfig(path="/tmp/metadata.sqlite")
        assert config.path == "/tmp/metadata.sqlite"

    def test_sqlite_sql_parts_generation(self):
        """Test SQL generation for SQLite metadata backend."""
        config = SqliteConfig(path="/tmp/metadata.sqlite")
        secret_sql, metadata_sql = config.get_ducklake_sql_parts("test_alias")

        assert secret_sql == ""
        assert metadata_sql == "METADATA_PATH '/tmp/metadata.sqlite'"


class TestPostgresConfig:
    """Tests for Postgres metadata backend configuration."""

    def test_postgres_config_creation(self):
        """Test basic Postgres config instantiation."""
        config = PostgresConfig(
            host="localhost",
            port=5432,
            database="testdb",
            user="testuser",
            password="testpass",
        )
        assert config.host == "localhost"
        assert config.port == 5432

    def test_postgres_config_env_defaults(self):
        """Test Postgres config with environment variable defaults."""
        with patch.dict(os.environ, {"DUCKLAKE_PG_HOST": "pghost.example.com"}):
            config = PostgresConfig(
                database="testdb",
                user="testuser",
                password="testpass",
            )
            assert config.host == "pghost.example.com"

    def test_postgres_sql_parts_generation(self):
        """Test SQL generation for Postgres metadata backend."""
        config = PostgresConfig(
            host="localhost",
            port=5432,
            database="testdb",
            user="testuser",
            password="testpass",
        )
        secret_sql, metadata_sql = config.get_ducklake_sql_parts("test_alias")

        assert "CREATE OR REPLACE SECRET secret_catalog_test_alias" in secret_sql
        assert "TYPE postgres" in secret_sql
        assert "HOST 'localhost'" in secret_sql
        assert "PORT 5432" in secret_sql
        assert "DATABASE 'testdb'" in secret_sql
        assert "USER 'testuser'" in secret_sql
        assert "PASSWORD 'testpass'" in secret_sql

        assert "METADATA_PATH ''" in metadata_sql
        assert "METADATA_PARAMETERS MAP" in metadata_sql
        assert "'TYPE': 'postgres'" in metadata_sql
        assert "'SECRET': 'secret_catalog_test_alias'" in metadata_sql


class TestS3Config:
    """Tests for S3 storage backend configuration."""

    def test_s3_config_creation(self):
        """Test basic S3 config instantiation."""
        config = S3Config(
            endpoint_url="https://s3.amazonaws.com",
            bucket="test-bucket",
            aws_access_key_id="AKIAIOSFODNN7EXAMPLE",
            aws_secret_access_key="wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
        )
        assert config.bucket == "test-bucket"
        assert config.region == "us-east-1"
        assert config.use_ssl is True

    def test_s3_full_data_path_without_prefix(self):
        """Test S3 data path generation without prefix."""
        config = S3Config(
            endpoint_url="https://s3.amazonaws.com",
            bucket="test-bucket",
            aws_access_key_id="key",
            aws_secret_access_key="secret",
        )
        assert config.full_data_path == "s3://test-bucket/"

    def test_s3_full_data_path_with_prefix(self):
        """Test S3 data path generation with prefix."""
        config = S3Config(
            endpoint_url="https://s3.amazonaws.com",
            bucket="test-bucket",
            prefix="data/lake",
            aws_access_key_id="key",
            aws_secret_access_key="secret",
        )
        assert config.full_data_path == "s3://test-bucket/data/lake/"

    def test_s3_full_data_path_with_leading_trailing_slashes(self):
        """Test S3 data path handles prefix slashes correctly."""
        config = S3Config(
            endpoint_url="https://s3.amazonaws.com",
            bucket="test-bucket",
            prefix="/data/lake/",
            aws_access_key_id="key",
            aws_secret_access_key="secret",
        )
        assert config.full_data_path == "s3://test-bucket/data/lake/"

    def test_s3_sql_parts_generation(self):
        """Test SQL generation for S3 storage backend."""
        config = S3Config(
            endpoint_url="https://s3.amazonaws.com",
            bucket="test-bucket",
            prefix="data",
            aws_access_key_id="AKIAIOSFODNN7EXAMPLE",
            aws_secret_access_key="wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
            region="us-west-2",
        )
        secret_sql, data_path_sql = config.get_ducklake_sql_parts("test_alias")

        assert "CREATE OR REPLACE SECRET secret_storage_test_alias" in secret_sql
        assert "TYPE S3" in secret_sql
        assert "KEY_ID 'AKIAIOSFODNN7EXAMPLE'" in secret_sql
        assert "SECRET 'wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY'" in secret_sql
        assert "ENDPOINT 'https://s3.amazonaws.com'" in secret_sql
        assert "REGION 'us-west-2'" in secret_sql
        assert "USE_SSL true" in secret_sql
        assert "SCOPE 's3://test-bucket'" in secret_sql

        assert data_path_sql == "DATA_PATH 's3://test-bucket/data/'"


class TestDuckLakeLocalDirectory:
    """Tests for local directory storage backend configuration."""

    def test_local_directory_config_creation(self):
        """Test basic local directory config instantiation."""
        config = DuckLakeLocalDirectory(path="/tmp/data")
        assert config.path == "/tmp/data"

    def test_local_directory_sql_parts_generation(self):
        """Test SQL generation for local directory storage backend."""
        config = DuckLakeLocalDirectory(path="/tmp/data")
        secret_sql, data_path_sql = config.get_ducklake_sql_parts("test_alias")

        assert secret_sql == ""
        assert data_path_sql == "DATA_PATH '/tmp/data'"


class TestDuckLakeResource:
    """Tests for the main DuckLake resource."""

    def test_ducklake_resource_creation_duckdb_local(self):
        """Test DuckLake resource with DuckDB metadata and local storage."""
        resource = DuckLakeResource(
            metadata_backend=DuckDBConfig(path="/tmp/metadata.db"),
            storage_backend=DuckLakeLocalDirectory(path="/tmp/data"),
            alias="test_lake",
        )
        assert resource.alias == "test_lake"
        assert "ducklake" in resource.plugins

    def test_ducklake_resource_custom_plugins(self):
        """Test DuckLake resource with custom plugins."""
        resource = DuckLakeResource(
            metadata_backend=DuckDBConfig(path="/tmp/metadata.db"),
            storage_backend=DuckLakeLocalDirectory(path="/tmp/data"),
            plugins=["ducklake", "spatial"],
        )
        assert resource.plugins == ["ducklake", "spatial"]

    def test_build_attach_options_clause_empty(self):
        """Test attach options clause building with no options."""
        resource = DuckLakeResource(
            metadata_backend=DuckDBConfig(path="/tmp/metadata.db"),
            storage_backend=DuckLakeLocalDirectory(path="/tmp/data"),
        )
        assert resource._build_attach_options_clause() == ""

    def test_build_attach_options_clause_with_booleans(self):
        """Test attach options clause with boolean values."""
        resource = DuckLakeResource(
            metadata_backend=DuckDBConfig(path="/tmp/metadata.db"),
            storage_backend=DuckLakeLocalDirectory(path="/tmp/data"),
            attach_options={"override_data_path": True, "read_only": False},
        )
        result = resource._build_attach_options_clause()
        assert "OVERRIDE_DATA_PATH true" in result
        assert "READ_ONLY false" in result

    def test_build_attach_options_clause_with_mixed_types(self):
        """Test attach options clause with mixed boolean types."""
        resource = DuckLakeResource(
            metadata_backend=DuckDBConfig(path="/tmp/metadata.db"),
            storage_backend=DuckLakeLocalDirectory(path="/tmp/data"),
            attach_options={"override_data_path": True, "read_only": False},
        )
        result = resource._build_attach_options_clause()
        # The method should handle the boolean values
        assert "(" in result and ")" in result

    @patch("duckdb.connect")
    def test_setup_ducklake_connection_installs_plugins(self, mock_connect):
        """Test that plugins are installed during connection setup."""
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value = mock_cursor

        resource = DuckLakeResource(
            metadata_backend=DuckDBConfig(path="/tmp/metadata.db"),
            storage_backend=DuckLakeLocalDirectory(path="/tmp/data"),
            plugins=["ducklake", "spatial"],
        )

        resource._setup_ducklake_connection(mock_conn, None)

        calls = [str(call) for call in mock_cursor.execute.call_args_list]
        assert any("INSTALL ducklake" in str(call) for call in calls)
        assert any("INSTALL spatial" in str(call) for call in calls)

    @patch("duckdb.connect")
    def test_setup_ducklake_connection_creates_secrets(self, mock_connect):
        """Test that secrets are created during connection setup."""
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value = mock_cursor

        resource = DuckLakeResource(
            metadata_backend=DuckDBConfig(path="/tmp/metadata.db"),
            storage_backend=DuckLakeLocalDirectory(path="/tmp/data"),
            alias="test_lake",
        )

        resource._setup_ducklake_connection(mock_conn, None)

        calls = [str(call) for call in mock_cursor.execute.call_args_list]
        # Should create the main ducklake secret
        assert any("secret_test_lake" in str(call) for call in calls)
        assert any("TYPE DUCKLAKE" in str(call) for call in calls)

    @patch("duckdb.connect")
    def test_setup_ducklake_connection_attaches_database(self, mock_connect):
        """Test that database is attached during connection setup."""
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value = mock_cursor

        resource = DuckLakeResource(
            metadata_backend=DuckDBConfig(path="/tmp/metadata.db"),
            storage_backend=DuckLakeLocalDirectory(path="/tmp/data"),
            alias="test_lake",
        )

        resource._setup_ducklake_connection(mock_conn, None)

        calls = [str(call) for call in mock_cursor.execute.call_args_list]
        assert any("ATTACH" in str(call) and "test_lake" in str(call) for call in calls)
        assert any("USE test_lake" in str(call) for call in calls)

    @patch("duckdb.connect")
    def test_get_duckdb_connection(self, mock_connect):
        """Test getting a native DuckDB connection."""
        mock_conn = MagicMock()
        mock_connect.return_value = mock_conn

        resource = DuckLakeResource(
            metadata_backend=DuckDBConfig(path="/tmp/metadata.db"),
            storage_backend=DuckLakeLocalDirectory(path="/tmp/data"),
        )

        conn = resource.get_duckdb_connection()

        mock_connect.assert_called_once_with(database=":memory:")
        assert conn == mock_conn

    @patch("duckdb.connect")
    def test_duckdb_connect_context_manager(self, mock_connect):
        """Test the duckdb_connect context manager."""
        mock_conn = MagicMock()
        mock_connect.return_value = mock_conn

        resource = DuckLakeResource(
            metadata_backend=DuckDBConfig(path="/tmp/metadata.db"),
            storage_backend=DuckLakeLocalDirectory(path="/tmp/data"),
        )

        with resource.duckdb_connect() as conn:
            assert conn == mock_conn

        mock_conn.close.assert_called_once()

    def test_ducklake_resource_with_postgres_s3(self):
        """Test DuckLake resource with Postgres metadata and S3 storage."""
        resource = DuckLakeResource(
            metadata_backend=PostgresConfig(
                host="localhost",
                database="testdb",
                user="testuser",
                password="testpass",
            ),
            storage_backend=S3Config(
                endpoint_url="https://s3.amazonaws.com",
                bucket="test-bucket",
                aws_access_key_id="key",
                aws_secret_access_key="secret",
            ),
            alias="prod_lake",
        )
        assert resource.alias == "prod_lake"


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
