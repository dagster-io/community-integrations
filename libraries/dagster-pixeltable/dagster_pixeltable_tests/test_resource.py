import subprocess
import sys
import textwrap
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from pathlib import Path
from typing import Any

import dagster as dg
import pixeltable as pxt
import pytest

from dagster_pixeltable import PixeltableResource
from dagster_pixeltable_tests.utils import child_env


@contextmanager
def ready(resource: PixeltableResource) -> Iterator[PixeltableResource]:
    with resource.yield_for_execution(dg.build_init_resource_context()) as runtime:
        yield runtime


def must_not_call(name: str) -> Callable[..., Any]:
    def fail(*args: Any, **kwargs: Any) -> Any:
        raise AssertionError(f"{name}() was called")

    return fail


def run_python(code: str, cwd: Path, **env: str) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        [sys.executable, "-c", textwrap.dedent(code)],
        cwd=cwd,
        env=child_env(**env),
        capture_output=True,
        text=True,
        check=False,
    )


def test_import_and_construction_leave_pixeltable_uninitialized(
    tmp_path: Path,
) -> None:
    home = tmp_path / "home"
    result = run_python(
        f"""
        from dagster_pixeltable import PixeltableResource

        PixeltableResource(expected_home={str(home)!r})
        """,
        cwd=tmp_path,
        PIXELTABLE_HOME=str(home),
    )
    assert result.returncode == 0, result.stderr
    # the SDK creates its home on first initialization
    assert not home.exists()


def test_setup_opens_existing_table(pixeltable_home: Path, catalog_dir: str) -> None:
    path = f"{catalog_dir}.items"
    pxt.create_table(path, {"name": pxt.String}).insert([{"name": "a"}])
    counts = []

    @dg.asset
    def items(pixeltable: PixeltableResource) -> None:
        counts.append(pixeltable.get_table(path).count())

    resource = PixeltableResource(expected_home=str(pixeltable_home))
    assert dg.materialize([items], resources={"pixeltable": resource}).success
    assert counts == [1]


def test_repeated_runs_and_two_instances_share_the_home(
    pixeltable_home: Path, catalog_dir: str
) -> None:
    path = f"{catalog_dir}.items"
    pxt.create_table(path, {"name": pxt.String})
    counts = []

    @dg.asset
    def items(first: PixeltableResource, second: PixeltableResource) -> None:
        counts.append((first.get_table(path).count(), second.get_table(path).count()))

    resources = {
        "first": PixeltableResource(expected_home=str(pixeltable_home)),
        "second": PixeltableResource(expected_home=f"{pixeltable_home}/"),
    }
    for _ in range(2):
        assert dg.materialize([items], resources=resources).success
    assert counts == [(0, 0), (0, 0)]


@pytest.mark.parametrize("asset_fails", [False, True])
def test_table_access_ends_with_teardown(
    pixeltable_home: Path, catalog_dir: str, asset_fails: bool
) -> None:
    path = f"{catalog_dir}.items"
    pxt.create_table(path, {"name": pxt.String})
    runtime = []

    @dg.asset
    def items(pixeltable: PixeltableResource) -> None:
        runtime.append(pixeltable)
        pixeltable.get_table(path)
        if asset_fails:
            raise RuntimeError("asset failed")

    configured = PixeltableResource(expected_home=str(pixeltable_home))
    result = dg.materialize(
        [items], resources={"pixeltable": configured}, raise_on_error=False
    )
    assert result.success is not asset_fails
    assert len(runtime) == 1 and runtime[0] is not configured
    # Dagster sets up a copy, so the configured instance never had access
    for resource in (configured, runtime[0]):
        with pytest.raises(RuntimeError, match="only inside a Dagster execution"):
            resource.get_table(path)


def test_setup_rejects_home_of_earlier_initialization(
    pixeltable_home: Path, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    assert pxt.home() == pixeltable_home
    other = tmp_path / "other"
    monkeypatch.setenv("PIXELTABLE_HOME", str(other))
    monkeypatch.setattr(pxt, "init", must_not_call("pxt.init"))
    resource = PixeltableResource(expected_home=str(other))
    with pytest.raises(
        ValueError, match=f"uses the Pixeltable home '{pixeltable_home}'"
    ):
        resource.setup_for_execution(dg.build_init_resource_context())
    assert not other.exists()


def test_conflicting_expected_homes_fail(pixeltable_home: Path, tmp_path: Path) -> None:
    @dg.asset
    def items(first: PixeltableResource, second: PixeltableResource) -> None:
        pass

    resources = {
        "first": PixeltableResource(expected_home=str(pixeltable_home)),
        "second": PixeltableResource(expected_home=str(tmp_path / "other")),
    }
    with pytest.raises(dg.DagsterResourceFunctionError) as exc_info:
        dg.materialize([items], resources=resources)
    assert isinstance(exc_info.value.__cause__, ValueError)
    assert "expected_home is" in str(exc_info.value.__cause__)


@pytest.mark.parametrize("env_home", ["~/pixeltable", "relative/home"])
def test_setup_rejects_relative_pixeltable_home(
    monkeypatch: pytest.MonkeyPatch, env_home: str
) -> None:
    monkeypatch.setenv("PIXELTABLE_HOME", env_home)
    monkeypatch.setattr(pxt, "home", must_not_call("pxt.home"))
    resource = PixeltableResource(expected_home="/srv/pixeltable")
    with pytest.raises(ValueError, match="PIXELTABLE_HOME must be an absolute path"):
        resource.setup_for_execution(dg.build_init_resource_context())


def test_setup_rejects_relative_expected_home(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(pxt, "home", must_not_call("pxt.home"))
    resource = PixeltableResource(expected_home="relative/home")
    with pytest.raises(ValueError, match="expected_home must be an absolute path"):
        resource.setup_for_execution(dg.build_init_resource_context())


def test_setup_rejects_cached_relative_home(tmp_path: Path) -> None:
    # relative to the working directory, the cached home names the expected directory
    result = run_python(
        f"""
        import os

        import dagster as dg
        import pixeltable as pxt
        from dagster_pixeltable import PixeltableResource

        pxt.home()
        os.environ["PIXELTABLE_HOME"] = {str(tmp_path / "home")!r}
        resource = PixeltableResource(expected_home=os.environ["PIXELTABLE_HOME"])
        try:
            resource.setup_for_execution(dg.build_init_resource_context())
        except ValueError as e:
            print(e)
        else:
            raise SystemExit("setup accepted a relative home")
        """,
        cwd=tmp_path,
        PIXELTABLE_HOME="home",
    )
    assert result.returncode == 0, result.stderr
    assert "is not an absolute path: 'home'" in result.stdout
    assert not (tmp_path / "home" / "pgdata").exists()


def test_missing_table_is_not_created(pixeltable_home: Path, catalog_dir: str) -> None:
    with ready(PixeltableResource(expected_home=str(pixeltable_home))) as resource:
        with pytest.raises(pxt.Error, match="does not exist"):
            resource.get_table(f"{catalog_dir}.missing")
        with pytest.raises(pxt.Error):
            resource.get_table(f"{catalog_dir}.not a name")
    assert pxt.list_tables(catalog_dir) == []


@pytest.mark.parametrize(
    "path",
    [
        "pxt://org:db/dir.items",
        "pxt://local:db/dir.items",
        "https://www.pixeltable.com/t/org:db/dir.items",
        "http://www.pixeltable.com/t/org:db/dir.items",
        "https://pixeltable.com/t/org:db/dir.items",
        "http://pixeltable.com/t/org:db/dir.items",
    ],
)
def test_remote_paths_are_rejected_before_the_sdk(
    pixeltable_home: Path, monkeypatch: pytest.MonkeyPatch, path: str
) -> None:
    with ready(PixeltableResource(expected_home=str(pixeltable_home))) as resource:
        monkeypatch.setattr(pxt, "get_table", must_not_call("pxt.get_table"))
        with pytest.raises(ValueError, match="opens only local tables"):
            resource.get_table(path)


def test_version_reference_is_read_only(
    pixeltable_home: Path, catalog_dir: str
) -> None:
    path = f"{catalog_dir}.items"
    table = pxt.create_table(path, {"name": pxt.String})
    table.insert([{"name": "a"}])
    version = table.get_metadata()["version"]
    table.insert([{"name": "b"}])

    with ready(PixeltableResource(expected_home=str(pixeltable_home))) as resource:
        old = resource.get_table(f"{path}:{version}")
        assert old.count() == 1
        with pytest.raises(pxt.Error, match="snapshot"):
            old.insert([{"name": "c"}])
    assert table.count() == 2
