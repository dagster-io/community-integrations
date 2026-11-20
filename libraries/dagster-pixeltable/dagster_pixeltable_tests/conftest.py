import socket
import uuid
from collections.abc import Iterator
from pathlib import Path

import pixeltable as pxt
import pixeltable_pgserver
import pytest

from dagster_pixeltable_tests.utils import run_command


def _free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


@pytest.fixture(scope="session", autouse=True)
def pixeltable_home(tmp_path_factory: pytest.TempPathFactory) -> Iterator[Path]:
    """One Pixeltable home for the session, set before the SDK first reads it."""
    home = tmp_path_factory.mktemp("pixeltable_home").resolve()
    with pytest.MonkeyPatch.context() as mp:
        mp.setenv("PIXELTABLE_HOME", str(home))
        # the pxt daemon of another home may hold the default port
        mp.setenv("PXT_PORT", str(_free_port()))
        try:
            yield home
        finally:
            run_command("pxt", "daemon", "stop", cwd=home, check=False)
            # the SDK leaves its embedded Postgres server running after the process exits
            if (home / "pgdata" / "postmaster.pid").exists():
                # pixeltable_pgserver generates pg_ctl() at import time
                pixeltable_pgserver.pg_ctl(  # ty: ignore[unresolved-attribute]
                    ["-w", "-m", "fast", "stop"], pgdata=home / "pgdata"
                )


@pytest.fixture
def catalog_dir() -> Iterator[str]:
    """A catalog directory owned by the test."""
    name = f"test_{uuid.uuid4().hex[:8]}"
    pxt.create_dir(name)
    yield name
    pxt.drop_dir(name, force=True)
