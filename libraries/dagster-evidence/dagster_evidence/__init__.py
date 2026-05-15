from dagster._core.libraries import DagsterLibraryRegistry

from dagster_evidence.components.evidence_project_v2 import EvidenceProjectComponentV2
from dagster_evidence.components.sources import (
    EvidenceProjectTranslatorData,
    EvidenceSourceTranslatorData,
    ProjectDagsterMetadata,
    SourceDagsterMetadata,
)
from dagster_evidence.components.translator import DagsterEvidenceTranslator
from dagster_evidence.lib.evidence_project import EvidenceProject
from dagster_evidence.resource import EvidenceResource

__version__ = "0.2.0"

__all__ = [
    "EvidenceProjectComponentV2",
    "EvidenceProject",
    "EvidenceResource",
    "DagsterEvidenceTranslator",
    "EvidenceSourceTranslatorData",
    "EvidenceProjectTranslatorData",
    "ProjectDagsterMetadata",
    "SourceDagsterMetadata",
]

DagsterLibraryRegistry.register(
    "dagster-evidence", __version__, is_dagster_package=False
)
