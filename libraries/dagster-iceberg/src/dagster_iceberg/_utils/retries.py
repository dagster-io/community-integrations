import logging
from abc import ABCMeta, abstractmethod

from pyiceberg.table import Table
from tenacity import (
    RetryError,
    Retrying,
    retry_if_exception_type,
    stop_after_attempt,
    wait_random,
)


class IcebergOperationException(Exception): ...


class IcebergOperationWithRetry(metaclass=ABCMeta):
    def __init__(self, table: Table):
        self.table = table
        self.logger = logging.getLogger(
            f"dagster_iceberg._utils.{self.__class__.__name__}",
        )

    @abstractmethod
    def operation(self, *args, **kwargs): ...

    def refresh(self):
        self.table.refresh()

    def execute(
        self,
        retries: int,
        exception_types: type[BaseException] | tuple[type[BaseException], ...],
        *args,
        **kwargs,
    ):
        try:
            for retry in Retrying(
                stop=stop_after_attempt(retries),
                reraise=True,
                wait=wait_random(0.1, 0.99),
                retry=retry_if_exception_type(exception_types),
            ):
                with retry:
                    try:
                        self.operation(*args, **kwargs)
                        # Reset the attempt number on success
                        retry.retry_state.attempt_number = 1
                    except exception_types as e:
                        # Do not refresh on the final try
                        if retry.retry_state.attempt_number < retries:
                            self.refresh()
                        raise e
        except RetryError as e:
            raise IcebergOperationException(
                f"Max retries exceeded for class {self.__class__!s}",
            ) from e
