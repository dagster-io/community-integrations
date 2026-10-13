import json
import subprocess
import sys
import uuid
from collections.abc import Iterator
from pathlib import Path
from typing import Any

import dagster as dg
import pixeltable as pxt
import pytest

from dagster_pixeltable_tests.utils import (
    EXAMPLES_DIR,
    child_env,
    deploy_example,
    load_example,
    run_command,
)

basic = load_example("basic")
FIXTURE = json.loads((EXAMPLES_DIR / "basic" / "documents.json").read_text())


@pytest.fixture
def documents() -> Iterator[pxt.Table]:
    deploy_example("basic", "dagster_demo")
    table = pxt.get_table(basic.TABLE_PATH)
    assert table is not None
    yield table
    pxt.drop_dir("dagster_demo", force=True)


def ingest(tmp_path: Path, records: list[dict[str, Any]]) -> dg.ExecuteInProcessResult:
    records_path = tmp_path / f"{uuid.uuid4().hex}.json"
    records_path.write_text(json.dumps(records))
    job = basic.defs.get_repository_def().get_implicit_global_asset_job_def()
    run_config = {
        "ops": {"ingest_documents": {"config": {"records_path": str(records_path)}}}
    }
    return job.execute_in_process(run_config=run_config, raise_on_error=False)


def materialization(result: dg.ExecuteInProcessResult) -> dict[str, Any]:
    [event] = result.asset_materializations_for_node("ingest_documents")
    return {key: value.value for key, value in event.metadata.items()}


def check_passed(result: dg.ExecuteInProcessResult) -> bool:
    [evaluation] = result.get_asset_check_evaluations()
    return evaluation.passed


def step_error(result: dg.ExecuteInProcessResult) -> str:
    """The messages of the failed step's error and its causes."""
    failure = result.failure_data_for_node("ingest_documents")
    assert failure is not None
    error = failure.error
    messages = []
    while error is not None:
        messages.append(error.message)
        error = error.cause
    return "\n".join(messages)


def table_state(table: pxt.Table) -> tuple[int, list[dict[str, Any]]]:
    rows = table.select(table.source_id, table.body, table.page_count)
    return table.get_metadata()["version"], list(
        rows.order_by(table.source_id).collect()
    )


def test_executor_is_in_process() -> None:
    job = basic.defs.get_repository_def().get_implicit_global_asset_job_def()
    assert job.executor_def.name == "in_process"


def test_replay_keeps_one_row_per_source_id(
    documents: pxt.Table, tmp_path: Path
) -> None:
    for _ in range(2):
        result = ingest(tmp_path, FIXTURE)
        assert result.success
        assert check_passed(result)
        assert materialization(result)["dagster/row_count"] == len(FIXTURE)

    rows = documents.select(
        documents.source_id, documents.body_upper, documents.chars_per_page
    ).collect()
    expected = {
        r["source_id"]: (r["body"].upper(), len(r["body"]) / r["page_count"])
        for r in FIXTURE
    }
    assert {
        r["source_id"]: (r["body_upper"], r["chars_per_page"]) for r in rows
    } == expected


def test_empty_batch_skips_the_write(documents: pxt.Table, tmp_path: Path) -> None:
    assert ingest(tmp_path, FIXTURE).success
    before = table_state(documents)

    result = ingest(tmp_path, [])
    assert result.success
    assert check_passed(result)
    assert materialization(result)["pixeltable/num_rows"] == 0
    assert table_state(documents) == before


@pytest.mark.parametrize(
    ("records", "message"),
    [
        ([{"body": "no id", "page_count": 1}], "Record 0 has no source_id"),
        ([{"source_id": "", "body": "empty id", "page_count": 1}], "has no source_id"),
        ([FIXTURE[0], FIXTURE[0]], "Duplicate source_id 'doc-1'"),
    ],
)
def test_invalid_source_ids_fail_before_the_write(
    documents: pxt.Table, tmp_path: Path, records: list[dict[str, Any]], message: str
) -> None:
    assert ingest(tmp_path, FIXTURE).success
    before = table_state(documents)

    result = ingest(tmp_path, records)
    assert not result.success
    assert message in step_error(result)
    assert table_state(documents) == before


@pytest.mark.parametrize(
    ("record", "message"),
    [
        (
            {"source_id": "doc-1", "body": "x", "page_count": "many"},
            "not a valid literal",
        ),
        (
            {"source_id": "doc-9", "body": "x", "page_count": 1, "author": "a"},
            "Unknown column: author",
        ),
    ],
)
def test_sdk_validation_errors_preserve_rows(
    documents: pxt.Table, tmp_path: Path, record: dict[str, Any], message: str
) -> None:
    assert ingest(tmp_path, FIXTURE).success
    before = table_state(documents)

    result = ingest(tmp_path, [record])
    assert not result.success
    assert message in step_error(result)
    assert table_state(documents) == before


def test_upsert_requires_a_primary_key(documents: pxt.Table, tmp_path: Path) -> None:
    pxt.drop_table(basic.TABLE_PATH)
    pxt.create_table(
        basic.TABLE_PATH,
        {"source_id": pxt.String, "body": pxt.String, "page_count": pxt.Int},
    )

    result = ingest(tmp_path, FIXTURE)
    assert not result.success
    assert "primary key" in step_error(result)


def test_computed_error_on_new_row_is_stored(
    documents: pxt.Table, tmp_path: Path
) -> None:
    assert ingest(tmp_path, FIXTURE).success
    version = documents.get_metadata()["version"]

    result = ingest(
        tmp_path, [{"source_id": "doc-0", "body": "empty", "page_count": 0}]
    )
    assert result.success
    assert materialization(result)["pixeltable/num_excs"] == 1
    assert documents.get_metadata()["version"] > version
    assert documents.count() == len(FIXTURE) + 1
    [row] = (
        documents.where(documents.source_id == "doc-0")
        .select(error=documents.chars_per_page.errortype)
        .collect()
    )
    assert row["error"] == "ZeroDivisionError"

    [evaluation] = result.get_asset_check_evaluations()
    assert not evaluation.passed
    assert evaluation.metadata["errored_source_ids"].value == ["doc-0"]


def test_metadata_reports_the_table(documents: pxt.Table, tmp_path: Path) -> None:
    metadata = materialization(ingest(tmp_path, FIXTURE))
    table_md = documents.get_metadata()

    assert metadata["dagster/table_name"] == "dagster_demo.documents"
    assert metadata["pixeltable/table_id"] == str(table_md["id"])
    assert metadata["pixeltable/version"] == table_md["version"]
    assert metadata["dagster/row_count"] == documents.count() == len(FIXTURE)
    assert metadata["pixeltable/num_rows"] == len(FIXTURE)
    assert metadata["pixeltable/num_excs"] == 0
    assert [(c.name, c.type) for c in metadata["dagster/column_schema"].columns] == [
        ("source_id", "String"),
        ("body", "String"),
        ("page_count", "Int"),
        ("body_upper", "String"),
        ("chars_per_page", "Float"),
    ]


def test_readme_commands(documents: pxt.Table) -> None:
    # the documents fixture ran the README's 'pxt schema update'
    for _ in range(2):
        result = run_command(
            "dagster",
            "asset",
            "materialize",
            "-f",
            "basic/definitions.py",
            "-a",
            "defs",
            "--select",
            "ingest_documents",
            cwd=EXAMPLES_DIR,
        )
        assert "RUN_SUCCESS" in result.stdout + result.stderr
    assert documents.count() == len(FIXTURE)
    assert documents.where(documents.chars_per_page.errortype != None).count() == 0

    readback = subprocess.run(
        [
            sys.executable,
            "-c",
            f"import pixeltable as pxt; print(pxt.get_table({basic.TABLE_PATH!r}).count())",
        ],
        env=child_env(),
        capture_output=True,
        text=True,
        check=False,
    )
    assert readback.returncode == 0, readback.stderr
    assert readback.stdout.split()[-1] == str(len(FIXTURE))
