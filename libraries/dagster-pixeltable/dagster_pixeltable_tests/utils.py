import importlib.util
import os
import shutil
import subprocess
import sys
from pathlib import Path
from types import ModuleType

import pixeltable as pxt

EXAMPLES_DIR = Path(__file__).parents[1] / "examples"


def run_command(
    *args: str, cwd: Path, env: dict[str, str] | None = None, check: bool = True
) -> subprocess.CompletedProcess[str]:
    """Run a console script from this environment's bin directory."""
    executable = shutil.which(args[0], path=Path(sys.executable).parent)
    assert executable is not None, f"{args[0]} is not installed in this environment"
    result = subprocess.run(
        [executable, *args[1:]],
        cwd=cwd,
        env=env,
        capture_output=True,
        text=True,
        check=False,
    )
    if check and result.returncode != 0:
        raise AssertionError(
            f"{' '.join(args)} exited with {result.returncode}\n"
            f"stdout:\n{result.stdout}\nstderr:\n{result.stderr}"
        )
    return result


def load_example(name: str) -> ModuleType:
    """Import examples/<name>/definitions.py."""
    path = EXAMPLES_DIR / name / "definitions.py"
    spec = importlib.util.spec_from_file_location(f"{name}_definitions", path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def deploy_example(name: str, catalog_dir: str) -> None:
    """Drop catalog_dir and create it from examples/<name>/app.py, as the README does."""
    pxt.drop_dir(catalog_dir, force=True, if_not_exists="ignore")
    run_command(
        "pxt", "schema", "update", f"{name}/app.py", catalog_dir, cwd=EXAMPLES_DIR
    )


def child_env(**overrides: str) -> dict[str, str]:
    env = dict(os.environ)
    env.update(overrides)
    return env
