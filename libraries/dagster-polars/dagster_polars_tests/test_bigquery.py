import datetime as dt
import io
from decimal import Decimal
from unittest.mock import MagicMock, patch

import polars as pl
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

pytest.importorskip("dagster_gcp")

from dagster._core.storage.db_io_manager import TableSlice  # noqa: E402
from google.cloud import bigquery  # noqa: E402

from dagster_polars.io_managers.bigquery import PolarsBigQueryTypeHandler  # noqa: E402


def _handle_output(
    df: pl.DataFrame, definition_metadata: dict | None = None
) -> tuple[MagicMock, pa.Schema]:
    """Runs handle_output and returns the mocked client and the schema of the uploaded Parquet."""
    connection = MagicMock(spec=bigquery.Client)
    uploaded: list[bytes] = []
    connection.load_table_from_file.side_effect = lambda stream, **kwargs: (
        uploaded.append(stream.read()) or MagicMock()
    )
    context = MagicMock()
    context.definition_metadata = definition_metadata or {}
    context.resource_config = {"location": "EU", "timeout": 30}
    table_slice = TableSlice(
        table="my_table", schema="my_dataset", database="my_project", columns=None
    )

    with patch(
        "dagster_polars.io_managers.bigquery.get_polars_metadata", return_value={}
    ):
        PolarsBigQueryTypeHandler().handle_output(context, table_slice, df, connection)

    connection.load_table_from_file.assert_called_once()
    connection.load_table_from_dataframe.assert_not_called()
    return connection, pq.read_schema(io.BytesIO(uploaded[0]))


def test_dataframe_is_loaded_as_parquet():
    df = pl.DataFrame({"a": [1, 2]})

    connection, _ = _handle_output(df, {"write_disposition": "WRITE_TRUNCATE"})

    kwargs = connection.load_table_from_file.call_args.kwargs
    assert kwargs["destination"] == "my_dataset.my_table"
    assert kwargs["project"] == "my_project"
    assert kwargs["location"] == "EU"
    assert kwargs["timeout"] == 30
    job_config = kwargs["job_config"]
    assert job_config.source_format == bigquery.SourceFormat.PARQUET
    assert job_config.parquet_options.enable_list_inference is True
    assert job_config.write_disposition == "WRITE_TRUNCATE"


def test_polars_types_are_kept_in_the_uploaded_parquet():
    """pl.Date columns must reach BigQuery as DATE, not TIMESTAMP (#253)."""
    df = pl.DataFrame(
        {
            "day": [dt.date(2024, 1, 1), dt.date(2024, 1, 2)],
            "moment": [dt.datetime(2024, 1, 1, 12), dt.datetime(2024, 1, 2, 12)],
            "moment_utc": pl.Series(
                [dt.datetime(2024, 1, 1, 12), dt.datetime(2024, 1, 2, 12)]
            ).dt.replace_time_zone("UTC"),
            "label": ["a", "b"],
            "amount": pl.Series(
                [Decimal("1.50"), Decimal("2.25")], dtype=pl.Decimal(10, 2)
            ),
            "tags": [["x"], ["y", "z"]],
            "point": [{"x": 1, "y": 2}, {"x": 3, "y": 4}],
        }
    )

    _, schema = _handle_output(df)

    assert schema.field("day").type == pa.date32()
    assert schema.field("moment").type == pa.timestamp("us")
    assert schema.field("moment_utc").type == pa.timestamp("us", tz="UTC")
    assert schema.field("label").type in (pa.string(), pa.large_string())
    assert schema.field("amount").type == pa.decimal128(10, 2)
    assert pa.types.is_list(schema.field("tags").type) or pa.types.is_large_list(
        schema.field("tags").type
    )
    assert pa.types.is_struct(schema.field("point").type)


def test_nanosecond_times_and_timestamps_are_written_in_microseconds():
    """BigQuery reads nanosecond Parquet times and timestamps as INTEGER."""
    df = pl.DataFrame(
        {
            "time": [dt.time(12, 30, 0, 123456)],
            "moment_ns": pl.Series(
                [dt.datetime(2024, 1, 1, 12, 0, 0, 123456)], dtype=pl.Datetime("ns")
            ),
            "moment_ns_utc": pl.Series(
                [dt.datetime(2024, 1, 1, 12)], dtype=pl.Datetime("ns", "UTC")
            ),
            "nested": [{"time": dt.time(1, 2), "times": [dt.time(3, 4)]}],
            "array": pl.Series([[dt.time(5, 6)]], dtype=pl.Array(pl.Time, 1)),
            "map": pl.from_arrow(
                pa.array(
                    [[("k", dt.time(7, 8))]],
                    type=pa.map_(pa.string(), pa.time64("ns")),
                )
            ),
        }
    )

    _, schema = _handle_output(df)

    assert schema.field("time").type == pa.time64("us")
    assert schema.field("moment_ns").type == pa.timestamp("us")
    assert schema.field("moment_ns_utc").type == pa.timestamp("us", tz="UTC")
    nested = schema.field("nested").type
    assert nested.field("time").type == pa.time64("us")
    assert nested.field("times").type.value_type == pa.time64("us")
    assert schema.field("array").type.value_type == pa.time64("us")
    assert schema.field("map").type.item_type == pa.time64("us")


def test_empty_dataframe_is_skipped():
    context = MagicMock()
    connection = MagicMock(spec=bigquery.Client)
    table_slice = TableSlice(
        table="my_table", schema="my_dataset", database="my_project", columns=None
    )

    PolarsBigQueryTypeHandler().handle_output(
        context, table_slice, pl.DataFrame(), connection
    )

    connection.load_table_from_file.assert_not_called()
    connection.load_table_from_dataframe.assert_not_called()
