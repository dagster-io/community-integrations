"""Regression test for the pydantic ``extension`` shadow warning.

Pydantic 2 emits a ``UserWarning`` when a subclass redeclares a field
that already exists on the parent model. ``UPathIOManager`` (Dagster
core) declares ``extension: Optional[str] = None``; subclasses that
overrode it as a plain ``extension: str = ".parquet"`` triggered the
warning on every import. Declaring the override as ``ClassVar`` stops
pydantic from treating it as a model field, silencing the warning.

This test reloads both IO manager modules and asserts no shadow
``UserWarning`` is emitted.
"""

import importlib
import warnings


def _reload_io_managers() -> None:
    import dagster_polars.io_managers.delta as delta_module
    import dagster_polars.io_managers.parquet as parquet_module

    importlib.reload(parquet_module)
    importlib.reload(delta_module)


def test_no_extension_shadow_warning_on_import() -> None:
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        _reload_io_managers()

    shadow = [
        str(w.message)
        for w in caught
        if "shadows an attribute" in str(w.message)
    ]
    assert not shadow, f"Pydantic shadow warning(s) leaked: {shadow}"
