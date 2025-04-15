from dagster._core.libraries import DagsterLibraryRegistry

from dagster_evidence.lib.evidence_project import EvidenceProject

__version__ = "0.1.5"

__all__ = [EvidenceProject]

DagsterLibraryRegistry.register("dagster-evidence", __version__)
