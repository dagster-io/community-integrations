"""BTET transaction-mode guard demonstration.

TeradataIOManager only supports ANSI transaction mode: BTET (Teradata/native)
mode requires DDL to be the final statement of a transaction, and a failed
CREATE TABLE there (the expected path once a table already exists) can roll
back the whole transaction - risking duplicate rows. `TeradataDbClient.connect()`
raises a clear ValueError at connect time instead of allowing that unsafe
combination.

This asset uses ``resources.teradata_resource_btet`` (tmode="TERA"). Materializing
it (e.g. from the Dagster UI, or via `dagster asset materialize`) is *expected to
fail* with that ValueError - that failure is the scenario being demonstrated,
not a bug in this example.
"""

import pandas as pd
from dagster import asset


@asset(name="btet_guard_showcase", io_manager_key="btet_io_manager")
def btet_guard_showcase() -> pd.DataFrame:
    return pd.DataFrame({"a": [1]})
