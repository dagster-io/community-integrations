from dagster_pipes_tests import PipesTestSuite


class TestingPipesTestSuite(PipesTestSuite):
    BASE_ARGS = ["hello", "there"]
