import os
from pathlib import Path

import pixeltable as pxt
from dagster import ConfigurableResource, InitResourceContext
from pydantic import Field, PrivateAttr

# pxt.get_table() opens these outside the catalog of this process; the web forms normalize to pxt:// URIs
_REMOTE_PREFIXES = (
    "pxt://",
    "https://www.pixeltable.com/t/",
    "http://www.pixeltable.com/t/",
    "https://pixeltable.com/t/",
    "http://pixeltable.com/t/",
)


class PixeltableResource(ConfigurableResource):
    """Opens existing tables in the Pixeltable instance of the Dagster process.

    Pixeltable keeps one instance per process, chosen by the ``PIXELTABLE_HOME`` environment variable when the
    SDK first initializes. ``expected_home`` does not select that instance: setup fails unless the process
    already uses it.

    Examples:
        .. code-block:: python

            import dagster as dg
            from dagster_pixeltable import PixeltableResource

            @dg.asset
            def documents(pixeltable: PixeltableResource) -> dg.MaterializeResult:
                table = pixeltable.get_table("my_app.documents")
                status = table.insert([{"source_id": "a", "body": "hello"}])
                return dg.MaterializeResult(metadata={"num_rows": status.num_rows})

            defs = dg.Definitions(
                assets=[documents],
                resources={"pixeltable": PixeltableResource(expected_home="/srv/pixeltable")},
                executor=dg.in_process_executor,
            )
    """

    expected_home: str = Field(
        description=(
            "Absolute path of the Pixeltable home this job uses. Setup fails unless it matches the home of the "
            "process, which PIXELTABLE_HOME sets before the SDK first initializes."
        ),
    )

    _ready: bool = PrivateAttr(default=False)

    @classmethod
    def _is_dagster_maintained(cls) -> bool:
        return False

    def setup_for_execution(self, context: InitResourceContext) -> None:
        self._ready = False
        expected = Path(self.expected_home).expanduser()
        if not expected.is_absolute():
            raise ValueError(
                f"expected_home must be an absolute path, got {self.expected_home!r}"
            )
        expected = expected.resolve()

        # checked before pxt.home(), which creates the home directory and fixes the process configuration
        env_home = os.environ.get("PIXELTABLE_HOME")
        if env_home:
            if not Path(env_home).is_absolute():
                raise ValueError(
                    f"PIXELTABLE_HOME must be an absolute path with no '~', got {env_home!r}. Pixeltable does not "
                    "expand it, and a relative path depends on the working directory."
                )
            _check_home(expected, Path(env_home).resolve())

        # an earlier initialization in this process can hold a different home than the environment names now
        effective = pxt.home()
        if not effective.is_absolute():
            raise ValueError(
                f"The Pixeltable home of this process is not an absolute path: {str(effective)!r}"
            )
        _check_home(expected, effective.resolve())

        pxt.init()
        self._ready = True

    def teardown_after_execution(self, context: InitResourceContext) -> None:
        self._ready = False

    def get_table(self, table_path: str) -> pxt.Table:
        """Open an existing local table, view, or snapshot.

        Args:
            table_path: The full catalog path, such as ``my_app.documents``. A version suffix, such as
                ``my_app.documents:7``, opens a read-only handle to that version.
        """
        if not self._ready:
            raise RuntimeError(
                "PixeltableResource.get_table() runs only inside a Dagster execution, after resource setup."
            )
        if table_path.startswith(_REMOTE_PREFIXES):
            raise ValueError(
                f"PixeltableResource opens only local tables, got {table_path!r}"
            )
        table = pxt.get_table(table_path)
        # with the default if_not_exists="error", a missing table raises instead
        assert table is not None
        return table


def _check_home(expected: Path, effective: Path) -> None:
    if effective != expected:
        raise ValueError(
            f"This process uses the Pixeltable home {str(effective)!r}, but expected_home is {str(expected)!r}. "
            "Set PIXELTABLE_HOME to the expected home before starting the Dagster process."
        )
