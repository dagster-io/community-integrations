"""Unit tests for PolarsIcebergIOManager.reader_override configuration."""

from unittest.mock import MagicMock, patch

import pytest

from dagster_iceberg.config import IcebergCatalogConfig

# ---------------------------------------------------------------------------
# PolarsIcebergIOManager field validation
# ---------------------------------------------------------------------------


def test_reader_override_defaults_to_none() -> None:
    """reader_override should be None when not set."""
    pytest.importorskip("polars")
    from dagster_iceberg.io_manager.polars import PolarsIcebergIOManager

    mgr = PolarsIcebergIOManager(
        name="test",
        config=IcebergCatalogConfig(
            properties={"uri": "sqlite:///test.db", "warehouse": "file:///tmp/wh"}
        ),
        namespace="default",
    )
    assert mgr.reader_override is None


def test_reader_override_accepts_pyiceberg() -> None:
    pytest.importorskip("polars")
    from dagster_iceberg.io_manager.polars import PolarsIcebergIOManager

    mgr = PolarsIcebergIOManager(
        name="test",
        config=IcebergCatalogConfig(
            properties={"uri": "sqlite:///test.db", "warehouse": "file:///tmp/wh"}
        ),
        namespace="default",
        reader_override="pyiceberg",
    )
    assert mgr.reader_override == "pyiceberg"


def test_reader_override_accepts_native() -> None:
    pytest.importorskip("polars")
    from dagster_iceberg.io_manager.polars import PolarsIcebergIOManager

    mgr = PolarsIcebergIOManager(
        name="test",
        config=IcebergCatalogConfig(
            properties={"uri": "sqlite:///test.db", "warehouse": "file:///tmp/wh"}
        ),
        namespace="default",
        reader_override="native",
    )
    assert mgr.reader_override == "native"


def test_reader_override_rejects_invalid_value() -> None:
    pytest.importorskip("polars")
    from pydantic import ValidationError

    from dagster_iceberg.io_manager.polars import PolarsIcebergIOManager

    with pytest.raises(ValidationError):
        PolarsIcebergIOManager(
            name="test",
            config=IcebergCatalogConfig(
                properties={"uri": "sqlite:///test.db", "warehouse": "file:///tmp/wh"}
            ),
            namespace="default",
            reader_override="rust",  # type: ignore[arg-type]
        )


# ---------------------------------------------------------------------------
# _PolarsIcebergTypeHandler.to_data_frame passes reader_override through
# ---------------------------------------------------------------------------


def test_to_data_frame_passes_reader_override_to_scan_iceberg() -> None:
    """reader_override kwarg must be forwarded to pl.scan_iceberg."""
    pl = pytest.importorskip("polars")
    from dagster._core.storage.db_io_manager import TableSlice

    from dagster_iceberg.io_manager.polars import _PolarsIcebergTypeHandler

    handler = _PolarsIcebergTypeHandler()

    mock_table = MagicMock()
    mock_table.schema.return_value = MagicMock()
    mock_table.spec.return_value = MagicMock(fields=[])

    mock_lazy = MagicMock(spec=pl.LazyFrame)
    mock_lazy.sql.return_value = mock_lazy  # sql() returns LazyFrame

    table_slice = TableSlice(
        schema="ns",
        table="tbl",
        columns=None,
        partition_dimensions=None,
        database="cat",
    )

    with patch("polars.scan_iceberg", return_value=mock_lazy) as mock_scan:
        handler.to_data_frame(
            table=mock_table,
            table_slice=table_slice,
            target_type=pl.LazyFrame,
            reader_override="pyiceberg",
        )

    mock_scan.assert_called_once()
    _, kwargs = mock_scan.call_args
    assert kwargs.get("reader_override") == "pyiceberg"


def test_to_data_frame_passes_none_reader_override_by_default() -> None:
    """When reader_override is not supplied it should still be passed as None."""
    pl = pytest.importorskip("polars")
    from dagster._core.storage.db_io_manager import TableSlice

    from dagster_iceberg.io_manager.polars import _PolarsIcebergTypeHandler

    handler = _PolarsIcebergTypeHandler()

    mock_table = MagicMock()
    mock_table.schema.return_value = MagicMock()
    mock_table.spec.return_value = MagicMock(fields=[])

    mock_lazy = MagicMock(spec=pl.LazyFrame)
    mock_lazy.sql.return_value = mock_lazy

    table_slice = TableSlice(
        schema="ns",
        table="tbl",
        columns=None,
        partition_dimensions=None,
        database="cat",
    )

    with patch("polars.scan_iceberg", return_value=mock_lazy) as mock_scan:
        handler.to_data_frame(
            table=mock_table,
            table_slice=table_slice,
            target_type=pl.LazyFrame,
        )

    _, kwargs = mock_scan.call_args
    assert kwargs.get("reader_override") is None


# ---------------------------------------------------------------------------
# load_input reads reader_override from resource_config
# ---------------------------------------------------------------------------


def test_load_input_reads_reader_override_from_resource_config() -> None:
    """load_input must pull reader_override from context.resource_config."""
    pl = pytest.importorskip("polars")
    from dagster._core.storage.db_io_manager import TableSlice

    from dagster_iceberg.io_manager.polars import _PolarsIcebergTypeHandler

    handler = _PolarsIcebergTypeHandler()

    mock_table = MagicMock()
    mock_table.schema.return_value = MagicMock()
    mock_table.spec.return_value = MagicMock(fields=[])

    mock_catalog = MagicMock()
    mock_catalog.load_table.return_value = mock_table

    mock_lazy = MagicMock(spec=pl.LazyFrame)
    mock_lazy.sql.return_value = mock_lazy

    context = MagicMock()
    context.resource_config = {"reader_override": "pyiceberg"}
    context.dagster_type.typing_type = pl.LazyFrame

    table_slice = TableSlice(
        schema="ns",
        table="tbl",
        columns=None,
        partition_dimensions=None,
        database="cat",
    )

    with patch("polars.scan_iceberg", return_value=mock_lazy) as mock_scan:
        handler.load_input(
            context=context, table_slice=table_slice, connection=mock_catalog
        )

    _, kwargs = mock_scan.call_args
    assert kwargs.get("reader_override") == "pyiceberg"


def test_load_input_uses_none_when_resource_config_absent() -> None:
    """When resource_config is None, reader_override must default to None."""
    pl = pytest.importorskip("polars")
    from dagster._core.storage.db_io_manager import TableSlice

    from dagster_iceberg.io_manager.polars import _PolarsIcebergTypeHandler

    handler = _PolarsIcebergTypeHandler()

    mock_table = MagicMock()
    mock_table.schema.return_value = MagicMock()
    mock_table.spec.return_value = MagicMock(fields=[])

    mock_catalog = MagicMock()
    mock_catalog.load_table.return_value = mock_table

    mock_lazy = MagicMock(spec=pl.LazyFrame)
    mock_lazy.sql.return_value = mock_lazy

    context = MagicMock()
    context.resource_config = None
    context.dagster_type.typing_type = pl.LazyFrame

    table_slice = TableSlice(
        schema="ns",
        table="tbl",
        columns=None,
        partition_dimensions=None,
        database="cat",
    )

    with patch("polars.scan_iceberg", return_value=mock_lazy) as mock_scan:
        handler.load_input(
            context=context, table_slice=table_slice, connection=mock_catalog
        )

    _, kwargs = mock_scan.call_args
    assert kwargs.get("reader_override") is None
