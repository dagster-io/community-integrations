from dagster._core.libraries import DagsterLibraryRegistry
from dagster_dataform.resources import (
    DagsterDataformResource as DagsterDataformResource,
    load_assets_from_dataform as load_assets_from_dataform,
    load_asset_checks_from_dataform as load_asset_checks_from_dataform,
)
from dagster_dataform.dataform_polling_sensor import (
    create_dataform_polling_sensor as create_dataform_polling_sensor,
)
from dagster_dataform.dataform_orchestration_schedule import (
    create_dataform_orchestration_schedule as create_dataform_orchestration_schedule,
)

__version__ = "0.0.1"

DagsterLibraryRegistry.register(
    "example-integration", __version__, is_dagster_package=False
)
