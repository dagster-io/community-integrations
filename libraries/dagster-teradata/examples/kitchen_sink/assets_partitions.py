"""Partitioned I/O manager scenarios.

Covers:
  * static partitions (exercises the escaped ``IN (...)`` cleanup clause)
  * time-window (daily) partitions (exercises the
    ``CAST(... AS TIMESTAMP(6))`` WHERE clause, including sub-second
    boundaries via ``_format_datetime``)
  * multi-dimensional partitions (static x daily), which requires the
    ``partition_expr`` metadata to be a mapping, not a single column name
"""

from datetime import datetime

import pandas as pd
from dagster import (
    AssetExecutionContext,
    DailyPartitionsDefinition,
    MultiPartitionsDefinition,
    StaticPartitionsDefinition,
    asset,
)

region_partitions = StaticPartitionsDefinition(["us", "eu", "apac"])
daily_partitions = DailyPartitionsDefinition(start_date="2024-01-01")


@asset(
    name="orders_by_region",
    partitions_def=region_partitions,
    metadata={"partition_expr": "region"},
)
def orders_by_region(context: AssetExecutionContext) -> pd.DataFrame:
    """Materialize once per partition_key ("us", "eu", "apac", ...).

    Re-materializing a single partition replaces only the rows for that
    region - other regions' rows are untouched.
    """
    region = context.partition_key
    return pd.DataFrame({"region": [region] * 3, "amount": [10, 20, 30]})


@asset(
    name="events_by_day",
    partitions_def=daily_partitions,
    metadata={"partition_expr": "event_ts"},
)
def events_by_day(context: AssetExecutionContext) -> pd.DataFrame:
    """Materialize once per daily partition_key ("2024-01-01", ...)."""
    day = datetime.fromisoformat(context.partition_key)
    return pd.DataFrame({"event_ts": [day, day], "count": [1, 2]})


@asset(
    name="sales_by_region_and_day",
    partitions_def=MultiPartitionsDefinition(
        {"region": region_partitions, "date": daily_partitions}
    ),
    metadata={"partition_expr": {"region": "region", "date": "sale_ts"}},
)
def sales_by_region_and_day(context: AssetExecutionContext) -> pd.DataFrame:
    """Multi-partitioned asset: partition_expr must map each dimension to a
    column, e.g. {"region": "region", "date": "sale_ts"}."""
    keys = context.partition_key.keys_by_dimension
    region = keys["region"]
    day = datetime.fromisoformat(keys["date"])
    return pd.DataFrame({"region": [region], "sale_ts": [day], "amount": [99]})
