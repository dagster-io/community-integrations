import unittest
from unittest.mock import patch, MagicMock
from dagster_modal import ModalClient


class TestModalClient(unittest.TestCase):
    @patch("dagster.PipesSubprocessClient.run")
    def test_run_creates_subprocess_with_func_ref(self, mock_run):
        client = ModalClient()
        func_ref = "example_function_ref"
        context = MagicMock()

        client.run(func_ref=func_ref, context=context)

        mock_run.assert_called_once_with(
            command=["modal", "run", func_ref],
            context=context,
            extras=None,
            env=None,
        )
