from dagster._core.libraries import DagsterLibraryRegistry

from dagster_evidence.lib.evidence_project import EvidenceProject
from dagster_evidence.resource import (
    EvidenceResource,
    load_evidence_asset_specs,
    EvidenceFilter,
)

__version__ = "0.1.5"

__all__ = [EvidenceProject, EvidenceResource, load_evidence_asset_specs, EvidenceFilter]

DagsterLibraryRegistry.register(
    "dagster-evidence", __version__, is_dagster_package=False
)
