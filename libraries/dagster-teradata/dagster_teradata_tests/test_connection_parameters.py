from unittest import mock

from dagster_teradata import TeradataResource


@mock.patch("teradatasql.connect")
def test_get_conn(mock_connect):
    resource = TeradataResource(
        host="host", user="login", password="password", database="schema", port="1025"
    )

    with resource.get_connection():
        pass

    assert mock_connect.call_count == 1
    args, kwargs = mock_connect.call_args
    assert args == ()
    assert kwargs["host"] == "host"
    assert kwargs["database"] == "schema"
    assert kwargs["port"] == "1025"
    assert kwargs["user"] == "login"
    assert kwargs["password"] == "password"


@mock.patch("teradatasql.connect")
def test_get_tmode_conn(mock_connect):
    resource = TeradataResource(
        host="host",
        user="login",
        password="password",
        database="schema",
        port="1025",
        tmode="tera",
    )

    with resource.get_connection():
        pass

    assert mock_connect.call_count == 1
    args, kwargs = mock_connect.call_args
    assert kwargs["tmode"] == "tera"


@mock.patch("teradatasql.connect")
def test_get_sslmode_conn(mock_connect):
    resource = TeradataResource(
        host="host",
        user="login",
        password="password",
        database="schema",
        port="1025",
        sslmode="require",
    )

    with resource.get_connection():
        pass

    assert mock_connect.call_count == 1
    args, kwargs = mock_connect.call_args
    assert kwargs["sslmode"] == "require"


@mock.patch("teradatasql.connect")
def test_get_sslverifyca_conn(mock_connect):
    resource = TeradataResource(
        host="host",
        user="login",
        password="password",
        database="schema",
        port="1025",
        sslmode="verify-ca",
        sslca="/tmp/cert",
    )

    with resource.get_connection():
        pass

    assert mock_connect.call_count == 1
    args, kwargs = mock_connect.call_args
    assert kwargs["sslmode"] == "verify-ca"
    assert kwargs["sslca"] == "/tmp/cert"


@mock.patch("teradatasql.connect")
def test_get_sslverifyfull_conn(mock_connect):
    resource = TeradataResource(
        host="host",
        user="login",
        password="password",
        database="schema",
        port="1025",
        sslmode="verify-full",
        sslca="/tmp/cert",
    )

    with resource.get_connection():
        pass

    assert mock_connect.call_count == 1
    args, kwargs = mock_connect.call_args
    assert kwargs["sslmode"] == "verify-full"
    assert kwargs["sslca"] == "/tmp/cert"


@mock.patch("teradatasql.connect")
def test_get_sslcrc_conn(mock_connect):
    resource = TeradataResource(
        host="host",
        user="login",
        password="password",
        database="schema",
        port="1025",
        sslcrc="sslcrc",
    )

    with resource.get_connection():
        pass

    assert mock_connect.call_count == 1
    args, kwargs = mock_connect.call_args
    assert kwargs["sslcrc"] == "sslcrc"


@mock.patch("teradatasql.connect")
def test_get_sslprotocol_conn(mock_connect):
    resource = TeradataResource(
        host="host",
        user="login",
        password="password",
        database="schema",
        port="1025",
        sslprotocol="protocol",
    )

    with resource.get_connection():
        pass

    assert mock_connect.call_count == 1
    args, kwargs = mock_connect.call_args
    assert kwargs["sslprotocol"] == "protocol"


@mock.patch("teradatasql.connect")
def test_get_sslcipher_conn(mock_connect):
    resource = TeradataResource(
        host="host",
        user="login",
        password="password",
        database="schema",
        port="1025",
        sslcipher="cipher",
    )

    with resource.get_connection():
        pass

    assert mock_connect.call_count == 1
    args, kwargs = mock_connect.call_args
    assert kwargs["sslcipher"] == "cipher"


@mock.patch("teradatasql.connect")
def test_get_proxy_conn(mock_connect):
    resource = TeradataResource(
        host="host",
        user="login",
        password="password",
        database="schema",
        port="1025",
        http_proxy="http://proxy",
        http_proxy_user="proxyuser",
        http_proxy_password="proxypass",
        https_proxy="https://secureproxy",
        https_proxy_user="secureuser",
        https_proxy_password="securepass",
        proxy_bypass_hosts="bypass.host.com",
    )

    with resource.get_connection():
        pass

    assert mock_connect.call_count == 1
    args, kwargs = mock_connect.call_args
    assert kwargs["http_proxy"] == "http://proxy"
    assert kwargs["http_proxy_user"] == "proxyuser"
    assert kwargs["http_proxy_password"] == "proxypass"
    assert kwargs["https_proxy"] == "https://secureproxy"
    assert kwargs["https_proxy_user"] == "secureuser"
    assert kwargs["https_proxy_password"] == "securepass"
    assert kwargs["proxy_bypass_hosts"] == "bypass.host.com"
