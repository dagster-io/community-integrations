from dagster._core.libraries import DagsterLibraryRegistry

from dagster_datahub.lib.evidence_project import EvidenceProject

__version__ = "0.1.5"

__all__ = [EvidenceProject]

DagsterLibraryRegistry.register("dagster-datahub", __version__)
