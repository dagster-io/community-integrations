import json
from typing import (
    Any,
    Dict,
    List,
    Optional,
    cast,
)

import dagster._check as check
import pytest
from dagster import (
    AssetCheckResult,
    AssetCheckSeverity,
    AssetCheckSpec,
    AssetExecutionContext,
    AssetKey,
    DataVersion,
    MaterializeResult,
    MetadataValue,
    PipesSubprocessClient,
    asset,
    materialize,
)
from dagster._core.definitions.metadata import normalize_metadata_value
from dagster._core.pipes.client import PipesContextInjector, PipesMessageReader
from dagster._core.pipes.utils import (
    PipesEnvContextInjector,
    PipesFileMessageReader,
    PipesTempFileContextInjector,
)
from dagster_aws.pipes import PipesS3ContextInjector, PipesS3MessageReader
from dagster_pipes import (
    PIPES_METADATA_TYPE_INFER,
    PipesAssetCheckSeverity,
    PipesMetadataType,
)
from pytest_cases import parametrize

from dagster_pipes_tests.constants import (
    CUSTOM_PAYLOAD_CASES_PATH,
    METADATA_CASES_PATH,
    METADATA_PATH,
    PIPES_CONFIG,
)
from dagster_pipes_tests.utils.message_reader import PipesTestingFileMessageReader

# todo: make PipesFileMessageReader more test-friendly (currently it deletes the file after reading)


# metadata must have string keys
METADATA_CASES = json.loads(METADATA_CASES_PATH.read_text())
METADATA_CASES[4]["corge"] = None  # json can't represent None, so we add it manually
# this is just any json
CUSTOM_MESSAGE_CASES = json.loads(CUSTOM_PAYLOAD_CASES_PATH.read_text())

METADATA = json.loads(METADATA_PATH.read_text())


def _resolve_metadata_value(
    value: Any, metadata_type: PipesMetadataType
) -> MetadataValue:
    # TODO: make this a @staticmethod in Pipes and reuse here
    if metadata_type == PIPES_METADATA_TYPE_INFER:
        return normalize_metadata_value(value)
    elif metadata_type == "text":
        return MetadataValue.text(value)
    elif metadata_type == "url":
        return MetadataValue.url(value)
    elif metadata_type == "path":
        return MetadataValue.path(value)
    elif metadata_type == "notebook":
        return MetadataValue.notebook(value)
    elif metadata_type == "json":
        return MetadataValue.json(value)
    elif metadata_type == "md":
        return MetadataValue.md(value)
    elif metadata_type == "float":
        return MetadataValue.float(value)
    elif metadata_type == "int":
        return MetadataValue.int(value)
    elif metadata_type == "bool":
        return MetadataValue.bool(value)
    elif metadata_type == "dagster_run":
        return MetadataValue.dagster_run(value)
    elif metadata_type == "asset":
        return MetadataValue.asset(AssetKey.from_user_string(value))
    elif metadata_type == "table":
        return MetadataValue.table(value)
    elif metadata_type == "null":
        return MetadataValue.null()
    else:
        check.failed(f"Unexpected metadata type {metadata_type}")


def assert_known_metadata(metadata: Dict[str, MetadataValue]):
    assert metadata is not None

    for key in METADATA:
        assert metadata.get(key) == _resolve_metadata_value(
            METADATA[key]["raw_value"], METADATA[key]["type"]
        )


class PipesTestSuite:
    # this should point to the base args which will be used
    # to run all the tests
    BASE_ARGS = ["change", "me"]

    @parametrize("metadata", METADATA_CASES)
    def test_context_reconstruction(
        self,
        metadata: Dict[str, Any],
        tmpdir_factory,
        capsys,
    ):
        """This test doesn't test anything in Dagster. Instead, it provides parameters to the external process, which should check if they are loaded correctly."""

        work_dir = tmpdir_factory.mktemp("work_dir")

        extras_path = work_dir / "extras.json"

        with open(str(extras_path), "w") as f:
            json.dump(metadata, f)

        @asset
        def my_asset(
            context: AssetExecutionContext,
            pipes_subprocess_client: PipesSubprocessClient,
        ) -> MaterializeResult:
            job_name = context.run.job_name

            args = self.BASE_ARGS + [
                "--env",
                f"--extras={str(extras_path)}",
                f"--job-name={job_name}",
            ]

            return pipes_subprocess_client.run(
                context=context,
                command=args,
                extras=metadata,
            ).get_materialize_result()

        result = materialize(
            [my_asset],
            resources={"pipes_subprocess_client": PipesSubprocessClient()},
            raise_on_error=False,
        )

        assert result.success

    def test_components(
        self,
        context_injector: PipesContextInjector,
        message_reader: PipesMessageReader,
        tmpdir_factory,
        capsys,
    ):
        @asset
        def my_asset(
            context: AssetExecutionContext,
            pipes_subprocess_client: PipesSubprocessClient,
        ) -> MaterializeResult:
            args = self.BASE_ARGS + [
                "--env",
                "--full",
            ]

            if isinstance(context_injector, PipesS3ContextInjector):
                args.extend(["--context-loader", "s3"])

            if isinstance(message_reader, PipesS3MessageReader):
                args.extend(["--message-writer", "s3"])

            invocation = pipes_subprocess_client.run(
                context=context,
                command=args,
            )

            custom_messages = invocation.get_custom_messages()

            assert len(custom_messages) == 1
            assert custom_messages[0] == "Hello from external process!"

            return invocation.get_materialize_result()

        result = materialize(
            [my_asset],
            resources={
                "pipes_subprocess_client": PipesSubprocessClient(
                    context_injector=context_injector, message_reader=message_reader
                )
            },
            raise_on_error=False,
        )

        assert result.success

    @parametrize("metadata", METADATA_CASES)
    @parametrize(
        "context_injector", [PipesEnvContextInjector(), PipesTempFileContextInjector()]
    )
    def test_extras(
        self,
        context_injector: PipesContextInjector,
        metadata: Dict[str, Any],
        tmpdir_factory,
        capsys,
    ):
        """This test doesn't test anything in Dagster. Instead, it provides extras to the external process, which should check if they are loaded correctly."""

        work_dir = tmpdir_factory.mktemp("work_dir")

        metadata_path = work_dir / "metadata.json"

        with open(str(metadata_path), "w") as f:
            json.dump(metadata, f)

        @asset
        def my_asset(
            context: AssetExecutionContext,
            pipes_subprocess_client: PipesSubprocessClient,
        ) -> MaterializeResult:
            job_name = context.run.job_name

            args = self.BASE_ARGS + [
                "--full",
                "--env",
                f"--extras={metadata_path}",
                f"--job-name={job_name}",
            ]

            invocation_result = pipes_subprocess_client.run(
                context=context,
                command=args,
                extras=metadata,
            )

            materialization = invocation_result.get_materialize_result()

            return materialization

        result = materialize(
            [my_asset],
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
            "[pipes] did not receive any messages from external process"
            not in captured.err
        )

    def test_error_reporting(
        self,
        tmpdir_factory,
        capsys,
    ):
        """This test checks if the external process sends an exception message correctly."""

        if not PIPES_CONFIG.general.error_reporting:
            pytest.skip("general.error_reporting is not enabled in pipes.toml")

        work_dir = tmpdir_factory.mktemp("work_dir")

        messages_file = work_dir / "messages"

        message_reader = PipesTestingFileMessageReader(str(messages_file))

        @asset
        def my_asset(
            context: AssetExecutionContext,
            pipes_subprocess_client: PipesSubprocessClient,
        ):
            args = self.BASE_ARGS + [
                "--full",
                "--throw-error",
            ]

            invocation_result = pipes_subprocess_client.run(
                context=context,
                command=args,
            )

            yield from invocation_result.get_results()

        result = materialize(
            [my_asset],
            resources={
                "pipes_subprocess_client": PipesSubprocessClient(
                    message_reader=message_reader
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

                    assert exception["name"] is not None
                    assert exception["message"] == "Very bad error has happened!"
                    assert exception["stack"] is not None

        result.all_events

        assert not result.success

        captured = capsys.readouterr()

        assert (
            "[pipes] did not receive any messages from external process"
            not in captured.err
        ), captured.err

    def test_message_log(
        self,
        tmpdir_factory,
        capsys,
    ):
        """This test checks if the external process sends log messages (like info and warning) correctly."""

        if not PIPES_CONFIG.messages.log:
            pytest.skip("messages.log is not enabled in pipes.toml")

        work_dir = tmpdir_factory.mktemp("work_dir")

        messages_file = work_dir / "messages"

        @asset
        def my_asset(
            context: AssetExecutionContext,
            pipes_subprocess_client: PipesSubprocessClient,
        ):
            args = self.BASE_ARGS + [
                "--full",
                "--logging",
            ]

            invocation_result = pipes_subprocess_client.run(
                context=context,
                command=args,
            )

            yield from invocation_result.get_results()

        result = materialize(
            [my_asset],
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

        expected_levels = {"DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"}
        logged_levels = set()

        for level in expected_levels:
            # example log line we are looking for:
            # 2024-11-13 16:54:55 +0100 - dagster - WARNING - __ephemeral_asset_job__ - 2716d101-cf11-4baa-b22d-d2530cb8b121 - my_asset - Warning message

            for line in err.split("\n"):
                if f"{level.lower().capitalize()} message" in line:
                    assert level in line
                    logged_levels.add(level)
                    
        assert logged_levels == expected_levels
        assert (
            "[pipes] did not receive any messages from external process"
            not in captured.err
        )

    @parametrize("custom_message_payload", CUSTOM_MESSAGE_CASES)
    def test_message_report_custom_message(
        self,
        custom_message_payload: Any,
        tmpdir_factory,
        capsys,
    ):
        """This test checks if the external process sends custom messages correctly."""

        if not PIPES_CONFIG.messages.report_custom_message:
            pytest.skip("messages.report_custom_message is not enabled in pipes.toml")

        work_dir = tmpdir_factory.mktemp("work_dir")

        custom_payload_path = work_dir / "custom_payload.json"

        with open(str(custom_payload_path), "w") as f:
            json.dump({"payload": custom_message_payload}, f)

        @asset
        def my_asset(
            context: AssetExecutionContext,
            pipes_subprocess_client: PipesSubprocessClient,
        ) -> MaterializeResult:
            job_name = context.run.job_name

            args = self.BASE_ARGS + [
                "--full",
                "--env",
                f"--job-name={job_name}",
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
            [my_asset],
            resources={"pipes_subprocess_client": PipesSubprocessClient()},
            raise_on_error=False,
        )

        assert result.success

        captured = capsys.readouterr()

        assert (
            "[pipes] did not receive any messages from external process"
            not in captured.err
        )

    @parametrize("data_version", [None, "alpha"])
    @parametrize("asset_key", [None, ["my_asset"]])
    def test_message_report_asset_materialization(
        self,
        data_version: Optional[str],
        asset_key: Optional[List[str]],
        tmpdir_factory,
        capsys,
    ):
        """This test checks if the external process sends asset materialization correctly."""
        if not PIPES_CONFIG.messages.report_asset_materialization:
            pytest.skip(
                "messages.report_asset_materialization is not enabled in pipes.toml"
            )

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
        def my_asset(
            context: AssetExecutionContext,
            pipes_subprocess_client: PipesSubprocessClient,
        ) -> MaterializeResult:
            job_name = context.run.job_name

            args = self.BASE_ARGS + [
                "--full",
                "--env",
                f"--job-name={job_name}",
                "--report-asset-materialization",
                str(asset_materialization_path),
            ]

            invocation_result = pipes_subprocess_client.run(
                context=context,
                command=args,
            )

            materialization = invocation_result.get_materialize_result()

            if data_version is not None:
                assert (
                    cast(DataVersion, materialization.data_version).value
                    == data_version
                )
            else:
                assert materialization.data_version is None

            if materialization.metadata is not None:
                assert_known_metadata(materialization.metadata)  # type: ignore

            # assert materialization.metadata is not None

            return materialization

        result = materialize(
            [my_asset],
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
            "[pipes] did not receive any messages from external process"
            not in captured.err
        )

    @parametrize("passed", [True, False])
    @parametrize("severity", ["WARN", "ERROR"])
    @parametrize("asset_key", [None, ["my_asset"]])
    def test_message_report_asset_check(
        self,
        passed: bool,
        asset_key: Optional[List[str]],
        severity: PipesAssetCheckSeverity,
        tmpdir_factory,
        capsys,
    ):
        """This test checks if the external process sends asset checks correctly."""

        if not PIPES_CONFIG.messages.report_asset_check:
            pytest.skip(
                "messages.report_asset_check is not enabled in pipes.toml"
            )

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
            check_specs=[AssetCheckSpec(name="my_check", asset=AssetKey(["my_asset"]))]
        )
        def my_asset(
            context: AssetExecutionContext,
            pipes_subprocess_client: PipesSubprocessClient,
        ):
            job_name = context.run.job_name

            args = self.BASE_ARGS + [
                "--full",
                "--env",
                f"--job-name={job_name}",
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
                assert_known_metadata(check_result.metadata)  # type: ignore

            assert check_result.severity == AssetCheckSeverity(severity)

            yield from results

        result = materialize(
            [my_asset],
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
            "[pipes] did not receive any messages from external process"
            not in captured.err
        )
