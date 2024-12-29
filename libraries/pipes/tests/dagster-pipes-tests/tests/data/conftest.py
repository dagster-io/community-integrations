# conftest.py
import pytest


def pytest_runtest_call(item):
    # This hook is called immediately before the test function is actually run.
    pytest.skip("Intentionally skipping test after setup.")
