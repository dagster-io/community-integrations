"""A local stand-in for the dispatcher Worker.

It follows the HTTP contract in ``dagster_cloudflare.containers.run_launcher``,
but runs each command as a local process instead of a Cloudflare Container.
The Dagster command is real, so a launched run really executes.
"""

import json
import os
import signal
import subprocess
import sys
import threading
from dataclasses import dataclass, field
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any

TOKEN = "test-token"


@dataclass
class FakeRun:
    payload: dict[str, Any]
    process: subprocess.Popen | None = None
    canceled: bool = False
    forced_state: dict[str, Any] | None = None

    def status(self) -> dict[str, Any]:
        if self.forced_state is not None:
            return self.forced_state
        code = self.process.poll() if self.process else None
        if code is None:
            return {"state": "canceling" if self.canceled else "running", "message": ""}
        if self.canceled:
            return {"state": "canceled", "message": "", "exitCode": code}
        if code == 0:
            return {"state": "success", "message": "Container exited with code 0."}
        return {
            "state": "failed",
            "message": f"Container exited with code {code}.",
            "exitCode": code,
        }


@dataclass
class FakeWorker:
    runs: dict[str, FakeRun] = field(default_factory=dict)
    requests: list[tuple[str, str]] = field(default_factory=list)
    drop_next_post_response: bool = False
    execute: bool = True

    def start(self) -> "FakeWorker":
        worker = self

        class Handler(BaseHTTPRequestHandler):
            def log_message(self, *args):
                pass

            def _reply(self, code: int, body: dict[str, Any]) -> None:
                data = json.dumps(body).encode()
                self.send_response(code)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(data)))
                self.end_headers()
                self.wfile.write(data)

            def _run_id(self) -> str | None:
                if self.headers.get("Authorization") != f"Bearer {TOKEN}":
                    self._reply(401, {"error": "Unauthorized"})
                    return None
                worker.requests.append((self.command, self.path))
                return self.path.removeprefix("/runs/")

            def do_GET(self):
                run_id = self._run_id()
                if run_id is None:
                    return
                run = worker.runs.get(run_id)
                self._reply(
                    200, run.status() if run else {"state": "missing", "message": ""}
                )

            def do_POST(self):
                run_id = self._run_id()
                if run_id is None:
                    return
                payload = json.loads(
                    self.rfile.read(int(self.headers["Content-Length"]))
                )
                if run_id not in worker.runs:
                    run = FakeRun(payload)
                    if worker.execute:
                        command = payload["command"]
                        # The container image would have the same Python as the code.
                        if command[0] == "dagster":
                            command = [sys.executable, "-m", "dagster", *command[1:]]
                        run.process = subprocess.Popen(
                            command, env={**os.environ, **payload.get("env", {})}
                        )
                    worker.runs[run_id] = run
                if worker.drop_next_post_response:
                    worker.drop_next_post_response = False
                    self.close_connection = True
                    self.connection.shutdown(2)
                    return
                self._reply(202, worker.runs[run_id].status())

            def do_DELETE(self):
                run_id = self._run_id()
                if run_id is None:
                    return
                run = worker.runs.get(run_id)
                if not run or not run.process or run.process.poll() is not None:
                    self._reply(200, {"terminated": False})
                    return
                run.canceled = True
                run.process.send_signal(signal.SIGTERM)
                run.process.wait(timeout=60)
                self._reply(200, {"terminated": True, **run.status()})

        self.server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        self.thread.start()
        return self

    @property
    def endpoint(self) -> str:
        return f"http://127.0.0.1:{self.server.server_address[1]}"

    def stop(self) -> None:
        self.server.shutdown()
        for run in self.runs.values():
            if run.process and run.process.poll() is None:
                run.process.kill()
                run.process.wait()
