"""SSH host key verification against a real paramiko server on localhost."""

import socket
import threading
from collections.abc import Iterator
from dataclasses import dataclass, field
from pathlib import Path

import paramiko
import pytest
from dagster import DagsterError

from dagster_teradata import TeradataResource


@pytest.fixture(name="host_key", scope="module")
def host_key_fixture() -> paramiko.PKey:
    return paramiko.RSAKey.generate(2048)


@dataclass
class SSHServer:
    port: int
    authenticated_users: list[str] = field(default_factory=list)


class _PasswordServer(paramiko.ServerInterface):
    def __init__(self, server: SSHServer):
        self.server = server

    def get_allowed_auths(self, username: str) -> str:
        return "password"

    def check_auth_password(self, username: str, password: str) -> int:
        self.server.authenticated_users.append(username)
        return paramiko.AUTH_SUCCESSFUL


@pytest.fixture(name="ssh_server")
def ssh_server_fixture(
    host_key: paramiko.PKey, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> Iterator[SSHServer]:
    """A password-only SSH server with no shell; HOME points at an empty directory."""
    monkeypatch.setenv("HOME", str(tmp_path))
    listener = socket.create_server(("127.0.0.1", 0))
    server = SSHServer(port=listener.getsockname()[1])
    transports: list[paramiko.Transport] = []

    def serve() -> None:
        while True:
            try:
                conn, _ = listener.accept()
            except OSError:
                return
            transport = paramiko.Transport(conn)
            transport.add_server_key(host_key)
            transports.append(transport)
            try:
                transport.start_server(server=_PasswordServer(server))
            except (paramiko.SSHException, EOFError):
                pass

    threading.Thread(target=serve, daemon=True).start()
    yield server
    listener.close()
    for transport in transports:
        transport.close()


def write_known_hosts(path: Path, port: int, key: paramiko.PKey) -> str:
    path.write_text(f"[127.0.0.1]:{port} {key.get_name()} {key.get_base64()}\n")
    return str(path)


def run_bteq(port: int, **ssh_options):
    return td_connection().bteq_operator(
        sql="SELECT 1;", **remote_options(port), **ssh_options
    )


def run_ddl(port: int, **ssh_options):
    return td_connection().ddl_operator(
        ddl=["CREATE TABLE t (a INT);"], **remote_options(port), **ssh_options
    )


def run_tdload(port: int, **ssh_options):
    return td_connection().tdload_operator(
        source_file_name="/tmp/in.csv",
        target_table="t",
        **remote_options(port),
        **ssh_options,
    )


def td_connection():
    return TeradataResource(
        host="tdhost", user="dbuser", password="dbpass"
    ).get_object_to_set_on_execution_context()


def remote_options(port: int) -> dict:
    return {
        "remote_host": "127.0.0.1",
        "remote_port": port,
        "remote_user": "dagster",
        "remote_password": "secret",
    }


OPERATORS = [run_bteq, run_ddl, run_tdload]


@pytest.mark.parametrize("run_operator", OPERATORS)
def test_unknown_host_key_is_rejected(ssh_server: SSHServer, run_operator):
    with pytest.raises(DagsterError, match="not in known_hosts"):
        run_operator(ssh_server.port)
    assert ssh_server.authenticated_users == []


@pytest.mark.parametrize("run_operator", OPERATORS)
def test_host_key_from_known_hosts_path_is_accepted(
    ssh_server: SSHServer, host_key: paramiko.PKey, tmp_path: Path, run_operator
):
    known_hosts = write_known_hosts(tmp_path / "known_hosts", ssh_server.port, host_key)
    # The test server has no shell, so the operator fails after logging in.
    with pytest.raises(DagsterError):
        run_operator(ssh_server.port, ssh_known_hosts_path=known_hosts)
    assert ssh_server.authenticated_users == ["dagster"]


@pytest.mark.parametrize("run_operator", OPERATORS)
def test_host_key_from_user_known_hosts_is_accepted(
    ssh_server: SSHServer, host_key: paramiko.PKey, tmp_path: Path, run_operator
):
    (tmp_path / ".ssh").mkdir()
    write_known_hosts(tmp_path / ".ssh" / "known_hosts", ssh_server.port, host_key)
    with pytest.raises(DagsterError):
        run_operator(ssh_server.port)
    assert ssh_server.authenticated_users == ["dagster"]


@pytest.mark.parametrize("run_operator", OPERATORS)
def test_changed_host_key_is_rejected(
    ssh_server: SSHServer, tmp_path: Path, run_operator
):
    other_key = paramiko.RSAKey.generate(2048)
    known_hosts = write_known_hosts(
        tmp_path / "known_hosts", ssh_server.port, other_key
    )
    with pytest.raises(DagsterError):
        run_operator(ssh_server.port, ssh_known_hosts_path=known_hosts)
    assert ssh_server.authenticated_users == []
