import pytest
import subprocess

from dagster_pipes_tests import PipesSuite


@pytest.fixture(scope="session", autouse=True)
def built_jar():
    subprocess.run(["./gradlew", "build"], check=True)


class JavaPipesSuite(PipesSuite):
    BASE_ARGS = [
        "java",
        "-cp",
        "build/libs/dagster-pipes-java-1.0-SNAPSHOT.jar",
        "pipes.MainTest",
    ]
