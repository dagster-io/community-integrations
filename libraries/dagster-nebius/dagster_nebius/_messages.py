"""Read the existing dagster-pipes S3 writer format in the supervision loop."""

import json
from collections.abc import Iterator
from contextlib import contextmanager
from typing import Any, cast

from botocore.exceptions import ClientError
from dagster._core.pipes.client import PipesMessageReader
from dagster._core.pipes.context import PipesMessageHandler
from dagster_pipes import (
    PIPES_PROTOCOL_VERSION,
    PIPES_PROTOCOL_VERSION_FIELD,
    PipesMessage,
    PipesParams,
)

from dagster_nebius._utils import Deadline, failure


class _S3MessageReader(PipesMessageReader):
    """Invocation-local reader. The Jobs supervisor calls poll; no background threads.

    S3 calls must have finite connect/read timeouts. Missing chunks are retried on
    the next poll; other failures propagate to the supervisor's cancellation path.
    """

    def __init__(self, client: Any, bucket: str, prefix: str):
        self.client = client
        self.bucket = bucket
        self.prefix = prefix
        self.index = 1
        self.opened = False
        self.closed = False
        self.handler: PipesMessageHandler | None = None

    @contextmanager
    def read_messages(self, handler: PipesMessageHandler) -> Iterator[PipesParams]:
        self.handler = handler
        try:
            yield {
                "bucket": self.bucket,
                "key_prefix": self.prefix,
                "include_stdio_in_messages": True,
            }
        finally:
            self.handler = None

    def no_messages_debug_text(self) -> str:
        return "Use open_dagster_pipes(message_writer=PipesS3MessageWriter(s3_client))."

    def poll(self, deadline: Deadline | None = None) -> None:
        # Limit each poll so prolific writers cannot starve status/cancel checks.
        for _ in range(100):
            if self.closed:
                return
            if deadline is not None:
                deadline.remaining()
            try:
                obj = self.client.get_object(
                    Bucket=self.bucket, Key=f"{self.prefix}/{self.index}.json"
                )
            except ClientError as exc:
                if exc.response.get("Error", {}).get("Code") in {"NoSuchKey", "404"}:
                    return
                raise failure(
                    "Cannot read Pipes storage; check S3 permissions and connectivity"
                ) from None
            body = obj["Body"]
            try:
                payload = body.read().decode("utf-8")
            finally:
                body.close()
            lines = [line for line in payload.splitlines() if line.strip()]
            if not lines:
                raise failure("Empty Pipes message chunk")
            for line in lines:
                self._handle(line)
            self.index += 1

    def _handle(self, line: str) -> None:
        try:
            message = json.loads(line)
        except (ValueError, TypeError):
            raise failure("Malformed Pipes JSON") from None
        if (
            not isinstance(message, dict)
            or message.get(PIPES_PROTOCOL_VERSION_FIELD) != PIPES_PROTOCOL_VERSION
            or "method" not in message
            or "params" not in message
        ):
            raise failure("Invalid or unsupported Pipes message")
        method = message["method"]
        if self.closed or (not self.opened and method != "opened"):
            raise failure("Invalid Pipes lifecycle order")
        if method == "opened":
            if self.opened:
                raise failure("Multiple Pipes writers/restarts are not supported")
            self.opened = True
        if method == "closed":
            if message["params"] and "exception" in message["params"]:
                raise failure("Remote Pipes process reported an exception")
            self.closed = True
        if self.handler is None:
            raise RuntimeError("Pipes reader has no active session")
        try:
            self.handler.handle_message(cast(PipesMessage, message))
        except Exception:
            raise failure(
                "Invalid Pipes event; check asset keys and event schema"
            ) from None
