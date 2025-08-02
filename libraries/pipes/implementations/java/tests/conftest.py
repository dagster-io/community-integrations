import pytest
import subprocess


@pytest.fixture(scope="session", autouse=True)
def built_jar():
    subprocess.run(["./gradlew", "build", "installDist"], check=True)
