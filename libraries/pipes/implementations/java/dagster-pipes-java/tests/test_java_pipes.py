from dagster import (
    AssetExecutionContext,
    MaterializeResult,
    PipesSubprocessClient,
    asset,
    materialize,
    MetadataValue,
    AssetKey,
    DataVersion,
    AssetCheckSpec,
    AssetCheckResult,
)

import pytest
from dagster_pipes import PipesAssetCheckSeverity
from typing import Dict, Any, Optional, List, cast
from pathlib import Path
from pytest_cases import parametrize
from dagster._core.pipes.utils import (
    PipesEnvContextInjector,
    PipesTempFileContextInjector,
    PipesFileMessageReader,
)
from dagster._core.pipes.client import PipesContextInjector
import json
from hypothesis_jsonschema import from_schema
from contextlib import contextmanager
from threading import Event, Thread
from typing import Iterator

from dagster_pipes import (
    PipesDefaultMessageWriter,
    PipesParams,
)
from dagster._core.pipes.context import (
    PipesMessageHandler,
)
from dagster._core.pipes.client import PipesMessageReader
from dagster_aws.pipes import PipesS3ContextInjector, PipesS3MessageReader


@contextmanager
def read_messages(
    self: PipesFileMessageReader,
    handler: "PipesMessageHandler",
) -> Iterator[PipesParams]:
    is_session_closed = Event()
    thread = None
    try:
        open(self._path, "w").close()  # create file   # type: ignore
        thread = Thread(
            target=self._reader_thread,  # type: ignore
            args=(handler, is_session_closed),
            daemon=True,
        )
        thread.start()
        yield {PipesDefaultMessageWriter.FILE_PATH_KEY: self._path}  # type: ignore
    finally:
        is_session_closed.set()
        if thread:
            thread.join()


# patch the read_messages method of PipesFileMessageReader to avoid deleting the file
# once the session is closed
# which is useful for testing messages
PipesFileMessageReader.read_messages = read_messages


extras_strategy = from_schema({"type": ["object"]})


ROOT_DIR = Path(__file__).parent.parent

CLASS_PATH = ROOT_DIR / "build/classes/java/main/pipes/PipesMappingParamsLoader.class"


# metadata must have string keys
METADATA_LIST = [
    {
        "foo": "bar",
    },
    {
        "foo": "bar",
        "baz": 1,
    },
    {
        "foo": "bar",
        "baz": 1,
        "qux": [1, 2, 3],
    },
    {
        "foo": "bar",
        "baz": 1,
        "qux": [1, 2, 3],
        "quux": {"a": 1, "b": 2},
    },
    {
        "foo": "bar",
        "baz": 1,
        "qux": [1, 2, 3],
        "quux": {"a": 1, "b": 2},
        "corge": None,
    },
]


# this is just any json
CUSTOM_MESSAGE_PAYLOADS = METADATA_LIST.copy() + [
    1,
    1.0,
    "foo",
    [1, 2, 3],
]


@parametrize("metadata", METADATA_LIST)
def test_java_pipes_reconstruction(
    metadata: Dict[str, Any],
    tmpdir_factory,
    capsys,
):
    work_dir = tmpdir_factory.mktemp("work_dir")

    extras_path = work_dir / "extras.json"

    with open(str(extras_path), "w") as f:
        json.dump(metadata, f)

    @asset
    def java_asset(
        context: AssetExecutionContext, pipes_subprocess_client: PipesSubprocessClient
    ) -> MaterializeResult:
        job_name = context.dagster_run.job_name

        args = [
            "java",
            "-jar",
            str(ROOT_DIR / "build/libs/dagster-pipes-java-1.0-SNAPSHOT.jar"),
            "--env",
            f"--extras={str(extras_path)}",
            f"--jobName={job_name}",
        ]

        return pipes_subprocess_client.run(
            context=context,
            command=args,
            extras=metadata,
        ).get_materialize_result()

    result = materialize(
        [java_asset],
        resources={"pipes_subprocess_client": PipesSubprocessClient()},
        raise_on_error=False,
    )

    assert result.success


def test_java_pipes_components(
    context_injector: PipesContextInjector,
    message_reader: PipesMessageReader,
    tmpdir_factory,
    capsys,
):
    @asset
    def java_asset(
        context: AssetExecutionContext, pipes_subprocess_client: PipesSubprocessClient
    ) -> MaterializeResult:
        args = [
            "java",
            "-jar",
            str(ROOT_DIR / "build/libs/dagster-pipes-java-1.0-SNAPSHOT.jar"),
            "--env",
            "--full",
        ]

        if isinstance(context_injector, PipesS3ContextInjector):
            args.append("--s3-context")

        if isinstance(message_reader, PipesS3MessageReader):
            args.extend(["--message-writer", "s3"])

        invocation = pipes_subprocess_client.run(
            context=context,
            command=args,
        )

        custom_messages = invocation.get_custom_messages()

        assert len(custom_messages) == 1
        assert custom_messages[0] == "Hello from Java!"

        return invocation.get_materialize_result()

    result = materialize(
        [java_asset],
        resources={
            "pipes_subprocess_client": PipesSubprocessClient(
                context_injector=context_injector, message_reader=message_reader
            )
        },
        raise_on_error=False,
    )

    assert result.success


@parametrize("metadata", METADATA_LIST)
@parametrize(
    "context_injector", [PipesEnvContextInjector(), PipesTempFileContextInjector()]
)
def test_java_pipes_extras(
    context_injector: PipesContextInjector,
    metadata: Dict[str, Any],
    tmpdir_factory,
    capsys,
):
    work_dir = tmpdir_factory.mktemp("work_dir")

    metadata_path = work_dir / "metadata.json"

    with open(str(metadata_path), "w") as f:
        json.dump(metadata, f)

    @asset
    def java_asset(
        context: AssetExecutionContext, pipes_subprocess_client: PipesSubprocessClient
    ) -> MaterializeResult:
        job_name = context.dagster_run.job_name

        args = [
            "java",
            "-jar",
            str(ROOT_DIR / "build/libs/dagster-pipes-java-1.0-SNAPSHOT.jar"),
            "--full",
            "--env",
            f"--extras={metadata_path}",
            f"--jobName={job_name}",
        ]

        invocation_result = pipes_subprocess_client.run(
            context=context,
            command=args,
            extras=metadata,
        )

        materialization = invocation_result.get_materialize_result()

        return materialization

    result = materialize(
        [java_asset],
        resources={
            "pipes_subprocess_client": PipesSubprocessClient(
                context_injector=context_injector
            )
        },
        raise_on_error=False,
    )

    assert result.success

    captured = capsys.readouterr()

    assert (
        "[pipes] did not receive any messages from external process" not in captured.err
    )


def test_java_pipes_exception_logging(
    tmpdir_factory,
    capsys,
):
    work_dir = tmpdir_factory.mktemp("work_dir")

    messages_file = work_dir / "messages"

    @asset
    def java_asset(
        context: AssetExecutionContext, pipes_subprocess_client: PipesSubprocessClient
    ):
        args = [
            "java",
            "-jar",
            str(ROOT_DIR / "build/libs/dagster-pipes-java-1.0-SNAPSHOT.jar"),
            "--full",
            "--throw-error",
        ]

        invocation_result = pipes_subprocess_client.run(
            context=context,
            command=args,
        )

        yield from invocation_result.get_results()

    result = materialize(
        [java_asset],
        resources={
            "pipes_subprocess_client": PipesSubprocessClient(
                message_reader=PipesFileMessageReader(str(messages_file))
            )
        },
        raise_on_error=False,
    )

    with open(str(messages_file), "r") as f:
        for line in f.readlines():
            message = json.loads(line)
            method = message["method"]

            if method == "closed":
                exception = message["params"]["exception"]

                assert exception["name"] == "pipes.DagsterPipesException"
                assert exception["message"] == "Very bad Java exception happened!"
                assert exception["stack"] is not None

    result.all_events

    assert not result.success

    captured = capsys.readouterr()

    assert (
        "[pipes] did not receive any messages from external process" not in captured.err
    )


def test_java_pipes_logging(
    tmpdir_factory,
    capsys,
):
    work_dir = tmpdir_factory.mktemp("work_dir")

    messages_file = work_dir / "messages"

    @asset
    def java_asset(
        context: AssetExecutionContext, pipes_subprocess_client: PipesSubprocessClient
    ):
        args = [
            "java",
            "-jar",
            str(ROOT_DIR / "build/libs/dagster-pipes-java-1.0-SNAPSHOT.jar"),
            "--full",
            "--logging",
        ]

        invocation_result = pipes_subprocess_client.run(
            context=context,
            command=args,
        )

        yield from invocation_result.get_results()

    result = materialize(
        [java_asset],
        resources={
            "pipes_subprocess_client": PipesSubprocessClient(
                message_reader=PipesFileMessageReader(str(messages_file))
            )
        },
        raise_on_error=False,
    )

    assert result.success

    captured = capsys.readouterr()

    err = captured.err

    for level in ["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"]:
        # example log line we are looking for:
        # 2024-11-13 16:54:55 +0100 - dagster - WARNING - __ephemeral_asset_job__ - 2716d101-cf11-4baa-b22d-d2530cb8b121 - java_asset - Warning message

        for line in err.split("\n"):
            if f"{level.lower().capitalize()} message" in line:
                assert level in line

    assert (
        "[pipes] did not receive any messages from external process" not in captured.err
    )


@parametrize("custom_message_payload", CUSTOM_MESSAGE_PAYLOADS)
def test_java_pipes_custom_message(
    custom_message_payload: Any,
    tmpdir_factory,
    capsys,
):
    work_dir = tmpdir_factory.mktemp("work_dir")

    custom_payload_path = work_dir / "custom_payload.json"

    with open(str(custom_payload_path), "w") as f:
        json.dump({"payload": custom_message_payload}, f)

    @asset
    def java_asset(
        context: AssetExecutionContext, pipes_subprocess_client: PipesSubprocessClient
    ) -> MaterializeResult:
        job_name = context.dagster_run.job_name

        args = [
            "java",
            "-jar",
            str(ROOT_DIR / "build/libs/dagster-pipes-java-1.0-SNAPSHOT.jar"),
            "--full",
            "--env",
            f"--jobName={job_name}",
            "--custom-payload-path",
            str(custom_payload_path),
        ]

        invocation_result = pipes_subprocess_client.run(
            context=context,
            command=args,
        )

        assert invocation_result.get_custom_messages()[-1] == custom_message_payload

        materialization = invocation_result.get_materialize_result()

        return materialization

    result = materialize(
        [java_asset],
        resources={"pipes_subprocess_client": PipesSubprocessClient()},
        raise_on_error=False,
    )

    assert result.success

    captured = capsys.readouterr()

    assert (
        "[pipes] did not receive any messages from external process" not in captured.err
    )


def assert_known_metadata(metadata: Dict[str, MetadataValue]):
    # {'bool_true': BoolMetadataValue(value=True), 'float': FloatMetadataValue(value=0.1), 'int': IntMetadataValue(value=1), 'url': UrlMetadataValue(url='https://dagster.io'), 'path': PathMetadataValue(path='/dev/null'), 'null': NullMetadataValue(), 'md': MarkdownMetadataValue(md_str='**markdown**'), 'json': JsonMetadataValue(data={'quux': {'a': 1, 'b': 2}, 'corge': None, 'qux': [1, 2, 3], 'foo': 'bar', 'baz': 1}), 'bool_false': BoolMetadataValue(value=False), 'text': TextMetadataValue(text='hello'), 'asset': DagsterAssetMetadataValue(asset_key=AssetKey(['foo', 'bar'])), 'dagster_run': DagsterRunMetadataValue(run_id='db892d7f-0031-4747-973d-22e8b9095d9d'), 'notebook': NotebookMetadataValue(path='notebook.ipynb')}

    assert metadata is not None

    assert metadata.get("bool_true") == MetadataValue.bool(True)
    assert metadata.get("bool_false") == MetadataValue.bool(False)
    assert metadata.get("float") == MetadataValue.float(0.1)
    assert metadata.get("int") == MetadataValue.int(1)
    assert metadata.get("url") == MetadataValue.url("https://dagster.io")
    assert metadata.get("path") == MetadataValue.path("/dev/null")
    assert metadata.get("null") == MetadataValue.null()
    assert metadata.get("md") == MetadataValue.md("**markdown**")
    assert metadata.get("json") == MetadataValue.json(
        {
            "quux": {"a": 1, "b": 2},
            "corge": None,
            "qux": [1, 2, 3],
            "foo": "bar",
            "baz": 1,
        }
    )
    assert metadata.get("text") == MetadataValue.text("hello")
    assert metadata.get("asset") == MetadataValue.asset(AssetKey(["foo", "bar"]))
    assert metadata.get("dagster_run") == MetadataValue.dagster_run(
        "db892d7f-0031-4747-973d-22e8b9095d9d"
    )
    assert metadata.get("notebook") == MetadataValue.notebook("notebook.ipynb")


@parametrize("data_version", [None, "alpha"])
@parametrize("asset_key", [None, ["java_asset"]])
def test_java_pipes_report_asset_materialization(
    data_version: Optional[str],
    asset_key: Optional[List[str]],
    tmpdir_factory,
    capsys,
):
    work_dir = tmpdir_factory.mktemp("work_dir")

    messages_file = work_dir / "messages"

    with open(str(messages_file), "w"):
        pass

    asset_materialization_dict = {}

    if data_version is not None:
        asset_materialization_dict["dataVersion"] = data_version

    if asset_key is not None:
        asset_materialization_dict["assetKey"] = "/".join(asset_key)

    asset_materialization_path = work_dir / "asset_materialization.json"

    with open(str(asset_materialization_path), "w") as f:
        json.dump(asset_materialization_dict, f)

    @asset
    def java_asset(
        context: AssetExecutionContext, pipes_subprocess_client: PipesSubprocessClient
    ) -> MaterializeResult:
        job_name = context.dagster_run.job_name

        args = [
            "java",
            "-jar",
            str(ROOT_DIR / "build/libs/dagster-pipes-java-1.0-SNAPSHOT.jar"),
            "--full",
            "--env",
            f"--jobName={job_name}",
            "--report-asset-materialization",
            str(asset_materialization_path),
        ]

        invocation_result = pipes_subprocess_client.run(
            context=context,
            command=args,
        )

        materialization = invocation_result.get_materialize_result()

        if data_version is not None:
            assert cast(DataVersion, materialization.data_version).value == data_version
        else:
            assert materialization.data_version is None

        if materialization.metadata is not None:
            assert_known_metadata(materialization.metadata)

        # assert materialization.metadata is not None

        return materialization

    result = materialize(
        [java_asset],
        resources={
            "pipes_subprocess_client": PipesSubprocessClient(
                message_reader=PipesFileMessageReader(str(messages_file))
            )
        },
        raise_on_error=True,
    )

    assert result.success

    captured = capsys.readouterr()

    assert (
        "[pipes] did not receive any messages from external process" not in captured.err
    )


@parametrize("passed", [True, False])
@parametrize("severity", ["WARN", "ERROR"])
@parametrize("asset_key", [None, ["java_asset"]])
def test_java_pipes_report_asset_check(
    passed: bool,
    asset_key: Optional[List[str]],
    severity: PipesAssetCheckSeverity,
    tmpdir_factory,
    capsys,
):
    work_dir = tmpdir_factory.mktemp("work_dir")

    messages_file = work_dir / "messages"

    with open(str(messages_file), "w"):
        pass

    report_asset_check_dict = {
        "passed": passed,
        "severity": severity,
        "checkName": "my_check",
    }

    if asset_key is not None:
        report_asset_check_dict["assetKey"] = "/".join(asset_key)

    report_asset_check_path = work_dir / "asset_materialization.json"

    with open(str(report_asset_check_path), "w") as f:
        json.dump(report_asset_check_dict, f)

    @asset(
        check_specs=[AssetCheckSpec(name="my_check", asset=AssetKey(["java_asset"]))]
    )
    def java_asset(
        context: AssetExecutionContext, pipes_subprocess_client: PipesSubprocessClient
    ):
        job_name = context.dagster_run.job_name

        args = [
            "java",
            "-jar",
            str(ROOT_DIR / "build/libs/dagster-pipes-java-1.0-SNAPSHOT.jar"),
            "--full",
            "--env",
            f"--jobName={job_name}",
            "--report-asset-check",
            str(report_asset_check_path),
        ]

        invocation_result = pipes_subprocess_client.run(
            context=context,
            command=args,
        )

        results = invocation_result.get_results()

        check_result = results[0]

        assert isinstance(check_result, AssetCheckResult)

        assert check_result.passed == passed

        if check_result.metadata is not None:
            assert_known_metadata(check_result.metadata)

        yield from results

    result = materialize(
        [java_asset],
        resources={
            "pipes_subprocess_client": PipesSubprocessClient(
                message_reader=PipesFileMessageReader(str(messages_file))
            )
        },
        raise_on_error=True,
    )

    assert result.success

    captured = capsys.readouterr()

    assert (
        "[pipes] did not receive any messages from external process" not in captured.err
    )
