"""Basic, unpartitioned I/O manager scenarios.

Covers:
  * a plain round trip (write a DataFrame, read it back in a downstream asset)
  * re-materialization replacing rows instead of appending to them
  * column subsetting via ``AssetIn(metadata={"columns": [...]})``
  * database/schema resolution: an explicit ``key_prefix`` selects the Teradata
    database, falling back to the resource's configured ``database`` when absent
"""

import pandas as pd
from dagster import AssetIn, asset


@asset(name="basic_customers")
def basic_customers() -> pd.DataFrame:
    """Writes a small DataFrame to <database>.basic_customers.

    Materialize this asset twice in a row (e.g. from the Dagster UI) to see that
    the second run *replaces* the two rows rather than appending to them - the
    I/O manager issues a DELETE before every unpartitioned write.
    """
    return pd.DataFrame({"id": [1, 2], "name": ["Ada", "Grace"]})


@asset(
    name="basic_customers_us",
    ins={"basic_customers": AssetIn("basic_customers", metadata={"columns": ["id"]})},
)
def basic_customers_us(basic_customers: pd.DataFrame) -> pd.DataFrame:
    """Reads only the ``id`` column of basic_customers back out.

    ``basic_customers`` here only has the ``id`` column because of the
    ``columns`` metadata on the AssetIn above - the I/O manager generates a
    ``SELECT id FROM ...`` rather than ``SELECT *``.
    """
    assert list(basic_customers.columns) == ["id"]
    return basic_customers


@asset(name="basic_no_prefix")
def basic_no_prefix() -> pd.DataFrame:
    """An asset with no key_prefix and no io_manager ``schema`` config.

    Regression coverage for a real bug: this used to fall back to Teradata's
    ``public`` pseudo-database (which doesn't really exist as a Teradata
    concept) instead of the resource's configured ``database``.
    """
    return pd.DataFrame({"a": [1]})
