"""Kitchen-sink Dagster project exercising every dagster-teradata I/O manager
scenario end to end. Run with (from libraries/dagster-teradata):

    dagster dev -m examples.kitchen_sink.definitions

Note: uses relative imports between its own modules, so it must be loaded as
a Python module (-m), not as a file target (-f).

See README.md in this directory for setup and a walkthrough of each asset/job.
"""

from dagster import Definitions, load_assets_from_modules

from . import assets_basic, assets_dtypes, assets_partitions, assets_tmode
from .assets_tdload import tdload_job
from .resources import teradata_io_manager, teradata_io_manager_btet, teradata_resource

assets = load_assets_from_modules(
    [assets_basic, assets_partitions, assets_dtypes, assets_tmode]
)

defs = Definitions(
    assets=assets,
    jobs=[tdload_job],
    resources={
        "io_manager": teradata_io_manager,
        "btet_io_manager": teradata_io_manager_btet,
        "teradata": teradata_resource,
    },
)
