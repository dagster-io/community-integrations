from unittest import mock

import pytest
from dagster import DagsterError

from dagster_teradata.teradata_compute_cluster_manager import (
    TeradataComputeClusterManager,
)


class RecordingConnection:
    """Answers the manager's version and storage checks and records each statement."""

    def __init__(self):
        self.statements: list[str] = []

    def execute_query(self, sql, fetch_results=False, single_result_row=False):
        self.statements.append(sql)
        if "DBC.DBCInfoV" in sql:
            return "20.00.24.01"
        if "DBC.StorageV" in sql:
            return 1
        return None


@pytest.fixture
def connection() -> RecordingConnection:
    return RecordingConnection()


@pytest.fixture
def manager(connection: RecordingConnection) -> TeradataComputeClusterManager:
    return TeradataComputeClusterManager(connection, mock.Mock())


@pytest.mark.parametrize(
    ("profile", "group", "statement"),
    [
        (
            "ShippingCG01",
            "Shipping",
            "DROP COMPUTE PROFILE ShippingCG01 IN COMPUTE GROUP Shipping",
        ),
        ("_cp$#1", None, "DROP COMPUTE PROFILE _cp$#1"),
        (
            "Profil_Größe",
            "Gruppe",
            "DROP COMPUTE PROFILE Profil_Größe IN COMPUTE GROUP Gruppe",
        ),
    ],
)
def test_drop_compute_cluster_uses_ordinary_names(
    manager: TeradataComputeClusterManager,
    connection: RecordingConnection,
    profile: str,
    group: str | None,
    statement: str,
):
    manager.drop_teradata_compute_cluster(profile, group)
    assert connection.statements[-1] == statement


HOSTILE_NAMES = [
    "x') OR ('1'='1",
    "Shipping; DROP USER dbc",
    "Shipping --",
    "Shipping /* comment */",
    'Shipping"',
    "Shipping Group",
    "1Shipping",
    "a" * 129,
]


@pytest.mark.parametrize("name", HOSTILE_NAMES)
@pytest.mark.parametrize(
    "operation",
    [
        lambda manager, name: manager.create_teradata_compute_cluster("cp", name),
        lambda manager, name: manager.drop_teradata_compute_cluster("cp", name),
        lambda manager, name: manager.resume_teradata_compute_cluster("cp", name),
        lambda manager, name: manager.suspend_teradata_compute_cluster("cp", name),
    ],
    ids=["create", "drop", "resume", "suspend"],
)
def test_compute_group_name_is_rejected_before_any_sql_runs(
    manager: TeradataComputeClusterManager,
    connection: RecordingConnection,
    operation,
    name: str,
):
    with pytest.raises(DagsterError, match="compute group name"):
        operation(manager, name)
    assert connection.statements == []


@pytest.mark.parametrize("name", HOSTILE_NAMES)
@pytest.mark.parametrize(
    "operation",
    [
        lambda manager, name: manager.create_teradata_compute_cluster(name, "cg"),
        lambda manager, name: manager.drop_teradata_compute_cluster(name, "cg"),
        lambda manager, name: manager.resume_teradata_compute_cluster(name, "cg"),
        lambda manager, name: manager.suspend_teradata_compute_cluster(name, "cg"),
    ],
    ids=["create", "drop", "resume", "suspend"],
)
def test_compute_profile_name_is_rejected_before_any_sql_runs(
    manager: TeradataComputeClusterManager,
    connection: RecordingConnection,
    operation,
    name: str,
):
    with pytest.raises(DagsterError, match="compute profile name"):
        operation(manager, name)
    assert connection.statements == []
