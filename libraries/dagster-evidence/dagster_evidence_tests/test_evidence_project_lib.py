"""Tests for the EvidenceProject component's deploy command handling."""

import json
import shlex
import sys
from pathlib import Path

import dagster as dg
import pytest
from dagster.components.core.component_tree import ComponentTree

from dagster_evidence.lib.evidence_project import EvidenceProject


@pytest.fixture(name="recorder")
def recorder_fixture(tmp_path: Path) -> Path:
    """An executable that appends its argv as one JSON line to argv.jsonl."""
    log = tmp_path / "argv.jsonl"
    script = tmp_path / "record-argv"
    script.write_text(
        f"#!{sys.executable}\n"
        "import json, sys\n"
        f"with open({str(log)!r}, 'a') as f:\n"
        "    f.write(json.dumps(sys.argv[1:]) + '\\n')\n"
    )
    script.chmod(0o755)
    return script


def run_deploy(tmp_path: Path, recorder: Path, deploy_command: str) -> list[str]:
    """Materialize an EvidenceProject and return the argv its deploy step received."""
    project = tmp_path / "project"
    project.mkdir()
    component = EvidenceProject(
        project_path=str(project),
        asset=dg.AssetSpec("dashboard"),
        deploy_command=deploy_command,
        npm_executable=str(recorder),
    )
    defs = component.build_defs(ComponentTree.for_test().load_context)
    assert dg.materialize(list(defs.assets or [])).success
    lines = (tmp_path / "argv.jsonl").read_text().splitlines()
    return json.loads(lines[-1])


@pytest.mark.parametrize(
    "hostile",
    [
        "x;touch {marker}",
        "x && touch {marker}",
        "x | touch {marker}",
        "$(touch {marker})",
        "`touch {marker}`",
        "x\ntouch {marker}",
    ],
)
def test_deploy_command_does_not_run_shell_syntax(
    tmp_path: Path, recorder: Path, hostile: str
):
    marker = tmp_path / "marker"
    argv = run_deploy(tmp_path, recorder, f"{recorder} {hostile.format(marker=marker)}")
    assert not marker.exists()
    assert argv == shlex.split(hostile.format(marker=marker))


@pytest.mark.parametrize(
    ("args", "expected"),
    [
        ("s3://bucket/dashboard", ["s3://bucket/dashboard"]),
        ("'two words' \"and three words\"", ["two words", "and three words"]),
        ("--from=$EVIDENCE_BUILD_PATH", ["--from={build}"]),
        ("${EVIDENCE_BUILD_PATH}/index.html", ["{build}/index.html"]),
    ],
)
def test_deploy_command_arguments(
    tmp_path: Path, recorder: Path, args: str, expected: list[str]
):
    build = tmp_path / "project" / "build"
    argv = run_deploy(tmp_path, recorder, f"{recorder} {args}")
    assert argv == [arg.format(build=build) for arg in expected]
