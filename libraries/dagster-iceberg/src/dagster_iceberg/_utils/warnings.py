from dagster import __version__
from packaging import version


def preview(wrapped=None):
    if version.parse(__version__) >= version.parse("1.10.0"):
        from dagster._annotations import (
            preview as decorator,  # ty: ignore
        )
    else:
        from dagster._annotations import (
            experimental as decorator,  # ty: ignore
        )

    if wrapped is not None:
        return decorator(wrapped)

    return decorator
