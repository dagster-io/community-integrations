from typing_extensions import TYPE_CHECKING
import pytest
import subprocess


if TYPE_CHECKING:
    pass


@pytest.fixture(scope="session", autouse=True)
def built_jar():
    subprocess.run(["./gradlew", "build"], check=True)
