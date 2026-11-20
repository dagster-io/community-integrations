import json
from contextlib import contextmanager
from io import BytesIO
from types import SimpleNamespace

import pytest
from botocore.exceptions import ClientError
from dagster import Failure

from dagster_nebius._messages import _S3MessageReader
from dagster_nebius_tests.test_pipes import events, message


@contextmanager
def active(client):
    handled = []
    reader = _S3MessageReader(client, "pipes-test", "run/attempt")
    with reader.read_messages(SimpleNamespace(handle_message=handled.append)):
        yield reader, handled


def test_delayed_chunk_gap_and_no_replay(s3):
    def put(n, messages):
        s3.put_object(
            Bucket="pipes-test",
            Key=f"run/attempt/{n}.json",
            Body="\n".join(json.dumps(m) for m in messages),
        )

    with active(s3) as (reader, handled):
        reader.poll()
        assert handled == []
        put(1, events()[:2])
        put(3, events()[3:])
        reader.poll()
        assert len(handled) == 2
        assert not reader.closed
        reader.poll()
        assert len(handled) == 2
        put(2, events()[2:3])
        reader.poll()
        assert reader.closed
        assert len(handled) == 4
        reader.poll()
        assert len(handled) == 4


@pytest.mark.parametrize(
    "payload",
    [
        "garbage",
        "[]",
        "{}",
        "",
        json.dumps(message("closed", {})),
        "\n".join(
            json.dumps(m) for m in [message("opened", {}), message("opened", {})]
        ),
        "\n".join(
            json.dumps(m)
            for m in [
                message("opened", {}),
                message("closed", {"exception": {"message": "SECRET"}}),
            ]
        ),
    ],
)
def test_invalid_chunks_fail(s3, payload):
    s3.put_object(Bucket="pipes-test", Key="run/attempt/1.json", Body=payload)
    with active(s3) as (reader, _), pytest.raises(Failure):
        reader.poll()


def test_denied_storage_not_treated_as_missing():
    def denied(**kwargs):
        raise ClientError(
            {"Error": {"Code": "AccessDenied", "Message": "SECRET"}}, "GetObject"
        )

    with (
        active(SimpleNamespace(get_object=denied)) as (reader, _),
        pytest.raises(Failure, match="permissions") as exc,
    ):
        reader.poll()
    assert "SECRET" not in str(exc.value)


def test_response_body_closed_on_decode_error():
    body = BytesIO(b"\xff")
    with (
        active(SimpleNamespace(get_object=lambda **k: {"Body": body})) as (reader, _),
        pytest.raises(UnicodeDecodeError),
    ):
        reader.poll()
    assert body.closed


def test_closed_session_does_not_fail_on_expired_deadline(s3):
    from dagster_nebius._utils import Deadline

    with active(s3) as (reader, _):
        reader.closed = True
        deadline = Deadline(1, clock=lambda: 0)
        deadline.clock = lambda: 2
        reader.poll(deadline)
