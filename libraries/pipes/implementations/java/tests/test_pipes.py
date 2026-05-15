from dagster_pipes_tests import PipesTestSuite


class TestJavaPipes(PipesTestSuite):
    BASE_ARGS = ["build/install/dagster-pipes-java/bin/mainTest"]
