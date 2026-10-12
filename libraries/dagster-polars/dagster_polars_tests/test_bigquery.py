import datetime as dt
from unittest.mock import MagicMock, patch

import polars as pl
import pytest

pytest.importorskip("dagster_gcp")

from dagster._core.storage.db_io_manager import TableSlice  # noqa: E402
from google.cloud import bigquery  # noqa: E402
from google.cloud.bigquery import _pandas_helpers  # noqa: E402

from dagster_polars.io_managers.bigquery import PolarsBigQueryTypeHandler  # noqa: E402


def _handle_output(df: pl.DataFrame) -> MagicMock:
    connection = MagicMock(spec=bigquery.Client)
    context = MagicMock()
    context.definition_metadata = {}
    context.resource_config = {}
    table_slice = TableSlice(
        table="my_table", schema="my_dataset", database="my_project", columns=None
    )

    with patch(
        "dagster_polars.io_managers.bigquery.get_polars_metadata", return_value={}
    ):
        PolarsBigQueryTypeHandler().handle_output(context, table_slice, df, connection)

    connection.load_table_from_dataframe.assert_called_once()
    return connection


def test_date_columns_are_loaded_as_bigquery_date():
    """pl.Date columns must reach BigQuery as DATE, not TIMESTAMP (#253)."""
    df = pl.DataFrame(
        {
            "day": [dt.date(2024, 1, 1), dt.date(2024, 1, 2)],
            "moment": [dt.datetime(2024, 1, 1, 12), dt.datetime(2024, 1, 2, 12)],
            "label": ["a", "b"],
        }
    )

    connection = _handle_output(df)

    pandas_df = connection.load_table_from_dataframe.call_args.kwargs["dataframe"]
    schema = {
        field.name: field.field_type
        for field in _pandas_helpers.dataframe_to_bq_schema(pandas_df, [])
    }

    assert schema["day"] == "DATE"
    assert schema["moment"] == "TIMESTAMP"
    assert schema["label"] == "STRING"


def test_empty_dataframe_is_skipped():
    context = MagicMock()
    connection = MagicMock(spec=bigquery.Client)
    table_slice = TableSlice(
        table="my_table", schema="my_dataset", database="my_project", columns=None
    )

    PolarsBigQueryTypeHandler().handle_output(
        context, table_slice, pl.DataFrame(), connection
    )

    connection.load_table_from_dataframe.assert_not_called()
