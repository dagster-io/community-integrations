"""BTEQ command construction: hostile values must reach BTEQ as data, never as shell."""

import json
import subprocess
import sys
from pathlib import Path

import pytest

from dagster_teradata import TeradataResource
from dagster_teradata.ttu.utils.bteq_util import (
    prepare_bteq_command_for_remote_execution,
)
from dagster_teradata.ttu.utils.encryption_utils import (
    decrypt_remote_file_to_string,
    generate_encrypted_file_with_openssl,
)

HOSTILE_VALUES = [
    'a"; touch {marker}; echo "',
    "a'; touch {marker}; echo '",
    "$(touch {marker})",
    "`touch {marker}`",
    "a b; touch {marker}",
]


@pytest.fixture(name="fake_bteq")
def fake_bteq_fixture(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    """Put a `bteq` on PATH that records its argv and stdin to bteq.json."""
    monkeypatch.setenv("HOME", str(tmp_path))
    bin_dir = tmp_path / "bin"
    bin_dir.mkdir()
    record = tmp_path / "bteq.json"
    script = bin_dir / "bteq"
    script.write_text(
        f"#!{sys.executable}\n"
        "import json, sys\n"
        "stdin = sys.stdin.read()\n"
        f"with open({str(record)!r}, 'w') as f:\n"
        "    json.dump({'argv': sys.argv[1:], 'stdin': stdin}, f)\n"
    )
    script.chmod(0o755)
    monkeypatch.setenv("PATH", f"{bin_dir}:/usr/bin:/bin")
    return record


def run_local_bteq(host: str, user: str, password: str, encoding: str) -> None:
    connection = TeradataResource(
        host=host, user=user, password=password
    ).get_object_to_set_on_execution_context()
    connection.bteq_operator(
        sql="SELECT 1;",
        bteq_script_encoding=encoding,
        bteq_session_encoding=encoding,
    )


@pytest.mark.parametrize("hostile", HOSTILE_VALUES)
def test_local_bteq_does_not_run_shell_syntax(
    tmp_path: Path, fake_bteq: Path, hostile: str
):
    marker = tmp_path / "marker"
    value = hostile.format(marker=marker)
    run_local_bteq(host=value, user=value, password=value, encoding=value)
    recorded = json.loads(fake_bteq.read_text())
    assert not marker.exists()
    assert recorded["argv"][:4] == ["-e", value, "-c", value]


@pytest.mark.parametrize("hostile", HOSTILE_VALUES)
def test_local_bteq_logs_on_through_stdin(
    tmp_path: Path, fake_bteq: Path, hostile: str
):
    password = hostile.format(marker=tmp_path / "marker")
    run_local_bteq(host="tdhost", user="dbuser", password=password, encoding="UTF8")
    recorded = json.loads(fake_bteq.read_text())
    assert recorded["stdin"].splitlines()[0] == f".LOGON tdhost/dbuser,{password}"
    assert not any(password in arg for arg in recorded["argv"])


@pytest.mark.parametrize("hostile", HOSTILE_VALUES)
def test_remote_bteq_command_quotes_encodings(
    tmp_path: Path, fake_bteq: Path, hostile: str
):
    marker = tmp_path / "marker"
    value = hostile.format(marker=marker)
    command = prepare_bteq_command_for_remote_execution(
        timeout=600,
        bteq_script_encoding=value,
        bteq_session_encoding=value,
        timeout_rc=-1,
    )
    subprocess.run(["sh", "-c", command], input=b"", check=True)
    recorded = json.loads(fake_bteq.read_text())
    assert not marker.exists()
    assert recorded["argv"][:4] == ["-e", value, "-c", value]


class LocalShellSSHClient:
    """Runs exec_command through a local `sh -c`, as sshd does on a POSIX host."""

    def __init__(self, cwd: Path):
        self.cwd = cwd

    class _Output:
        def __init__(self, data: bytes, returncode: int):
            self._data = data
            self.channel = self
            self._returncode = returncode

        def read(self) -> bytes:
            return self._data

        def recv_exit_status(self) -> int:
            return self._returncode

    def exec_command(self, command: str):
        result = subprocess.run(
            ["sh", "-c", command], capture_output=True, cwd=self.cwd
        )
        return (
            None,
            self._Output(result.stdout, result.returncode),
            self._Output(result.stderr, result.returncode),
        )


@pytest.mark.parametrize(
    "dirname",
    ["dir;touch marker", "$(touch marker)", "`touch marker`", "two words"],
)
def test_remote_decrypt_quotes_script_path(
    tmp_path: Path, fake_bteq: Path, dirname: str
):
    marker = tmp_path / "marker"
    remote_dir = tmp_path / dirname
    remote_dir.mkdir()
    plain = tmp_path / "script.txt"
    plain.write_text(".LOGON tdhost/dbuser,secret\nSELECT 1;\n.EXIT\n")
    encrypted = remote_dir / "bteq_script.enc"
    generate_encrypted_file_with_openssl(str(plain), "one-time-key", str(encrypted))
    exit_status, _, _ = decrypt_remote_file_to_string(
        LocalShellSSHClient(tmp_path), str(encrypted), "one-time-key", "bteq"
    )
    assert exit_status == 0
    assert not marker.exists()
    assert json.loads(fake_bteq.read_text())["stdin"] == plain.read_text()
