"""Resource wiring for the kitchen-sink example, driven entirely by environment
variables so the same project can point at any Teradata instance (a local Vantage
Express VM, a shared dev system, ...) without editing code.
"""

import os

from dagster_teradata import TeradataPandasIOManager, TeradataResource


def _env(name: str, default: str | None = None) -> str | None:
    return os.getenv(name, default)


# The "primary" resource: ANSI transaction mode (the default and the only mode
# supported by TeradataIOManager / TeradataPandasIOManager - see assets_tmode.py).
teradata_resource = TeradataResource(
    host=_env("TERADATA_HOST"),
    user=_env("TERADATA_USER"),
    password=_env("TERADATA_PASSWORD"),
    database=_env("TERADATA_DATABASE"),
)

# A second resource, identical except it opts into Teradata's native BTET
# transaction mode, used by assets_tmode.py to demonstrate that the I/O manager
# rejects it up front instead of risking duplicate rows.
teradata_resource_btet = TeradataResource(
    host=_env("TERADATA_HOST"),
    user=_env("TERADATA_USER"),
    password=_env("TERADATA_PASSWORD"),
    database=_env("TERADATA_DATABASE"),
    tmode="TERA",
)

# The pandas I/O manager, with a small chunk_size so io_chunked_writes (see
# assets_dtypes.py) actually exercises multiple executemany() batches.
teradata_io_manager = TeradataPandasIOManager(
    teradata=teradata_resource,
    chunk_size=250,
    min_varchar_length=32,
)

# A second I/O manager wired to the BTET resource above, used only by
# assets_tmode.py's btet_guard_showcase asset to demonstrate the tmode guard.
teradata_io_manager_btet = TeradataPandasIOManager(teradata=teradata_resource_btet)
