from dagster._core.libraries import DagsterLibraryRegistry
from dagster_dataform.resources import (
    DataformRepositoryResource as DataformRepositoryResource,
    load_dataform_assets as load_dataform_assets,
    load_dataform_asset_check_specs as load_dataform_asset_check_specs,
)
from dagster_dataform.dataform_polling_sensor import (
    create_dataform_workflow_invocation_sensor as create_dataform_workflow_invocation_sensor,
)
from dagster_dataform.dataform_orchestration_schedule import (
    create_dataform_orchestration_schedule as create_dataform_orchestration_schedule,
)

__version__ = "0.0.2"

DagsterLibraryRegistry.register(
    "dagster-dataform", __version__, is_dagster_package=False
)
