import pytest
import subprocess


@pytest.fixture(scope="session", autouse=True)
def built_binary():
    subprocess.run(["cargo", "build", "--features", "pipes-tests"], check=True)
