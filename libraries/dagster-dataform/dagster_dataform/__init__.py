from dagster._core.libraries import DagsterLibraryRegistry
from dagster_dataform.resources import (
    DataformRepositoryResource as DataformRepositoryResource,
)
from dagster_dataform.dataform_polling_sensor import (
    create_dataform_workflow_invocation_sensor as create_dataform_workflow_invocation_sensor,
    DataformFailureNotificationOpConfig as DataformFailureNotificationOpConfig,
)
from dagster_dataform.dataform_orchestration_schedule import (
    create_dataform_orchestration_schedule as create_dataform_orchestration_schedule,
)

__version__ = "0.0.4"

DagsterLibraryRegistry.register(
    "dagster-dataform", __version__, is_dagster_package=False
)
