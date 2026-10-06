import importlib
import importlib.metadata

from dagster._core.libraries import DagsterLibraryRegistry
from dagster_teradata.resources import (
    TeradataDagsterConnection as TeradataDagsterConnection,
    TeradataResource as TeradataResource,
    fetch_last_updated_timestamps as fetch_last_updated_timestamps,
    teradata_resource as teradata_resource,
)

from dagster_teradata.teradata_compute_cluster_manager import (
    TeradataComputeClusterManager as TeradataComputeClusterManager,
)

from dagster_teradata.ttu.bteq import Bteq as Bteq

from dagster_teradata.io_manager import (
    TeradataDbClient as TeradataDbClient,
    TeradataDbIOManager as TeradataDbIOManager,
    TeradataIOManager as TeradataIOManager,
    build_teradata_io_manager as build_teradata_io_manager,
)

__version__ = "0.0.8"

# pandas, polars and pyspark are optional dependencies, so their type handlers are
# exposed lazily: importing dagster_teradata must not require any of them.
# Maps each optional module to the extra that provides it and the names it exports.
_OPTIONAL_EXPORTS: dict[str, tuple[str, str, frozenset[str]]] = {
    "pandas_type_handler": (
        "pandas",
        "pandas",
        frozenset(
            {
                "TeradataPandasIOManager",
                "TeradataPandasTypeHandler",
                "teradata_pandas_io_manager",
            }
        ),
    ),
    "polars_type_handler": (
        "polars",
        "polars",
        frozenset(
            {
                "TeradataPolarsIOManager",
                "TeradataPolarsTypeHandler",
                "teradata_polars_io_manager",
            }
        ),
    ),
    "pyspark_type_handler": (
        "pyspark",
        "pyspark",
        frozenset(
            {
                "TeradataPySparkIOManager",
                "TeradataPySparkTypeHandler",
            }
        ),
    ),
}

_LAZY_EXPORTS = frozenset().union(
    *(names for _module, _extra, names in _OPTIONAL_EXPORTS.values())
)


def __getattr__(name: str):
    for handler_module, (
        requirement,
        extra,
        exported_names,
    ) in _OPTIONAL_EXPORTS.items():
        if name not in exported_names:
            continue
        try:
            importlib.import_module(requirement)
        except ImportError as exc:
            raise ImportError(
                f"'{name}' requires {requirement}, which is not installed. Install "
                f'it with `pip install "dagster-teradata[{extra}]"`.'
            ) from exc
        try:
            module = importlib.import_module(f"dagster_teradata.{handler_module}")
        except AttributeError as exc:
            # The requirement is importable but too old: e.g. pyspark 3.3 lacks
            # types.TimestampNTZType, which the handler references at module scope,
            # so the failure is an AttributeError. An ImportError here comes from
            # some other dependency and is left to propagate unchanged, as is an
            # AttributeError raised on an object that is not from the requirement.
            missing_on = getattr(exc, "obj", None)
            if missing_on is not None and not (
                getattr(missing_on, "__name__", "") or ""
            ).startswith(requirement):
                raise
            try:
                installed = importlib.metadata.version(requirement)
            except importlib.metadata.PackageNotFoundError:
                installed = "unknown"
            raise ImportError(
                f"'{name}' could not be loaded with the installed {requirement} "
                f"(version {installed}); it is likely older than dagster-teradata "
                f"supports. Install a supported version with "
                f'`pip install --upgrade "dagster-teradata[{extra}]"`.'
            ) from exc
        return getattr(module, name)
    raise AttributeError(f"module 'dagster_teradata' has no attribute '{name}'")


def __dir__() -> list:
    return sorted(set(globals()) | _LAZY_EXPORTS)


DagsterLibraryRegistry.register(
    "dagster-teradata", __version__, is_dagster_package=False
)
