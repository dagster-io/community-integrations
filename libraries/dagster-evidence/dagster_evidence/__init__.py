from dagster._core.libraries import DagsterLibraryRegistry

from dagster_evidence.lib.evidence_project import EvidenceProject
from dagster_evidence.resource import EvidenceResource

__version__ = "0.1.6"

__all__ = [EvidenceProject, EvidenceResource]

DagsterLibraryRegistry.register(
    "dagster-evidence", __version__, is_dagster_package=False
)
