from dagster_pipes_tests import PipesTestSuite


class TestJavaPipes(PipesTestSuite):
    BASE_ARGS = [
        "java",
        "-cp",
        "build/libs/dagster-pipes-java-1.0-SNAPSHOT.jar",
        "pipes.MainTest",
    ]
