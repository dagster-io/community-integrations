from dagster._core.libraries import DagsterLibraryRegistry

from dagster_modal.resources import ModalClient as ModalClient

__version__ = "0.0.4"

DagsterLibraryRegistry.register("dagster-modal", __version__, is_dagster_package=False)
