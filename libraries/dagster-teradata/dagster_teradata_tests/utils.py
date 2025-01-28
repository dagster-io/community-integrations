from unittest import mock


def create_mock_connector(*_args, **_kwargs):
    return connect_with_fetchall_returning(None)


def connect_with_fetchall_returning(value):
    cursor_mock = mock.MagicMock()
    cursor_mock.fetchall.return_value = value
    teradata_connect = mock.MagicMock()
    teradata_connect.cursor.return_value = cursor_mock
    m = mock.Mock()
    m.return_value = teradata_connect
    return m
