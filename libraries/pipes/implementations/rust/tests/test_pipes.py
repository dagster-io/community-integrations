from dagster_pipes_tests import PipesTestSuite


class TestRustPipes(PipesTestSuite):
    BASE_ARGS = ["./target/debug/pipes_tests"]

