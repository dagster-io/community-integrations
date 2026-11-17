"""Type-handling scenarios for TeradataPandasTypeHandler.

Covers:
  * the shipped dtype mapping (ints incl. widened unsigned types, floats,
    bools, Decimal, date/datetime, plain strings)
  * categorical columns sized from the full category domain, not just the
    values present in one materialization
  * VARCHAR/CLOB fallback columns holding unsupported objects (a dict here),
    which are stringified before being sent to the driver
  * chunked writes (paired with the small ``chunk_size=250`` configured on
    the io manager in resources.py) across more rows than one chunk
  * an intentionally-broken asset demonstrating the clear ``ValueError`` for
    duplicate column names (case-insensitively, since Teradata identifiers
    are case-insensitive) - this asset is expected to fail if materialized
"""

from datetime import date
from decimal import Decimal

import numpy as np
import pandas as pd
from dagster import asset


@asset(name="dtype_showcase")
def dtype_showcase() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "small_int": pd.Series([1, 2], dtype="int16"),
            "big_uint": pd.Series([2**40, 1], dtype="uint64"),
            "flag": pd.Series([True, False]),
            "amount": pd.Series([Decimal("12.345"), Decimal("0.001")], dtype=object),
            "day": pd.Series([date(2024, 1, 1), date(2024, 1, 2)], dtype=object),
            "moment": pd.to_datetime(["2024-01-01 10:00:00", None]),
            "label": pd.Series(["hello", None]),
        }
    )


@asset(name="categorical_showcase")
def categorical_showcase() -> pd.DataFrame:
    """The category domain includes "extra-long-category-value", which is not
    present in the materialized rows below. The created VARCHAR column is
    still sized to fit it, so a later materialization using that category
    value won't overflow the column."""
    categories = pd.CategoricalDtype(
        categories=["short", "medium", "extra-long-category-value"]
    )
    return pd.DataFrame({"tier": pd.Series(["short", "medium"], dtype=categories)})


@asset(name="varchar_fallback_showcase")
def varchar_fallback_showcase() -> pd.DataFrame:
    """Columns of otherwise-unsupported objects (here, dicts) fall back to
    VARCHAR/CLOB, sized from - and stringified from - their repr()."""
    return pd.DataFrame(
        {
            "id": [1, 2],
            "payload": [{"k": "v1"}, {"k": "v2"}],
        }
    )


@asset(name="chunked_writes_showcase")
def chunked_writes_showcase() -> pd.DataFrame:
    """1,000 rows against the io manager's chunk_size=250 (see resources.py)
    exercises 4 executemany() batches instead of 1."""
    return pd.DataFrame({"n": np.arange(1000)})


@asset(name="duplicate_columns_showcase")
def duplicate_columns_showcase() -> pd.DataFrame:
    """Intentionally produces a DataFrame with case-insensitively duplicate
    column names ("id" and "ID"). Materializing this asset is expected to
    fail with a clear ValueError from the type handler, rather than silently
    generating an invalid CREATE TABLE with two identical Teradata columns."""
    frame = pd.DataFrame([[1, 2]], columns=["id", "ID"])
    return frame
