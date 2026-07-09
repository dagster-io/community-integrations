from typing import Any


class ElasticsearchBulkIndexError(Exception):
    """Raised when one or more documents fail during a bulk index operation.

    Wraps :class:`elasticsearch.helpers.BulkIndexError` so callers can read
    per-document errors directly via :attr:`errors` without digging into
    ``__cause__``.
    """

    def __init__(self, message: str, errors: list[dict[str, Any]]) -> None:
        super().__init__(message)
        self.errors = errors
