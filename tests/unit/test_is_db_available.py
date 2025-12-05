import pytest
from unittest.mock import MagicMock
from include.is_available.is_db_available import is_db_available_callable


def test_db_available(mocker):
    """DB is available when PostgresHook.get_conn() succeeds."""
    mock_conn = MagicMock()

    mock_hook = MagicMock()
    mock_hook.get_conn.return_value = mock_conn

    mocker.patch(
        "include.is_available.is_db_available.PostgresHook",
        return_value=mock_hook
    )

    result = is_db_available_callable()

    assert result is True
    mock_hook.get_conn.assert_called_once()
    mock_conn.close.assert_called_once()


def test_db_not_available_connection_error(mocker):
    """DB is unavailable when PostgresHook.get_conn() raises an exception."""
    mock_hook = MagicMock()
    mock_hook.get_conn.side_effect = Exception("boom")

    mocker.patch(
        "include.is_available.is_db_available.PostgresHook",
        return_value=mock_hook
    )

    result = is_db_available_callable()

    assert result is False


def test_db_exception_on_close(mocker):
    """Even if close() fails, the function should still return False."""
    mock_conn = MagicMock()
    mock_conn.close.side_effect = Exception("cannot close")

    mock_hook = MagicMock()
    mock_hook.get_conn.return_value = mock_conn

    mocker.patch(
        "include.is_available.is_db_available.PostgresHook",
        return_value=mock_hook
    )

    result = is_db_available_callable()

    # As soon as .close() fails, the whole try/except catches it → returns False
    assert result is False
