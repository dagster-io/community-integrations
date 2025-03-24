from dagster._core.libraries import DagsterLibraryRegistry

from dagster_modal.resources import ModalClient as ModalClient

__version__ = "0.0.2"

DagsterLibraryRegistry.register("dagster-modal", __version__)
