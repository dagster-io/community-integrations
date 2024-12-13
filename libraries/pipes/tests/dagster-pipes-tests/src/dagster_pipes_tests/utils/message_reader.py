from contextlib import contextmanager
from threading import Event, Thread
from typing import Iterator

from dagster import PipesFileMessageReader
from dagster._core.pipes.context import (
    PipesMessageHandler,
)
from dagster_pipes import (
    PipesDefaultMessageWriter,
    PipesParams,
)


# TODO: replace this with PipesMessageReader(cleanup_file=False) once this parameter lands in the main repo
class PipesTestingFileMessageReader(PipesFileMessageReader):
    @contextmanager
    def read_messages(
        self,
        handler: "PipesMessageHandler",
    ) -> Iterator[PipesParams]:
        """Same as original method but doesn't delete the file after reading messages."""
        is_session_closed = Event()
        thread = None
        try:
            open(self._path, "w").close()  # create file
            thread = Thread(
                target=self._reader_thread,
                args=(handler, is_session_closed),
                daemon=True,
            )
            thread.start()
            yield {
                PipesDefaultMessageWriter.FILE_PATH_KEY: self._path,
            }
        finally:
            is_session_closed.set()
            if thread:
                thread.join()
