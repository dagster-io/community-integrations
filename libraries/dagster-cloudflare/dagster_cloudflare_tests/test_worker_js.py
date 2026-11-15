import shutil
import subprocess
from pathlib import Path

import pytest

WORKER = Path(__file__).resolve().parents[1] / "worker"


@pytest.mark.skipif(shutil.which("node") is None, reason="node is not installed")
def test_worker_unit_tests():
    tests = sorted(str(p) for p in (WORKER / "test").glob("*.test.js"))
    result = subprocess.run(
        ["node", "--test", *tests],
        check=False,
        cwd=WORKER,
        capture_output=True,
        text=True,
        timeout=120,
    )
    assert result.returncode == 0, result.stdout + result.stderr
