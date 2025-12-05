import pytest
from unittest.mock import MagicMock
from include.is_available.is_spark_available import is_spark_available_callable

def test_spark_available(mocker):
    """Spark available when 'master' exists in extra_dejson."""
    mock_conn = MagicMock()
    mock_conn.extra_dejson = {"master": "local[]"}

    mocker.patch(
        "include.is_available.is_spark_available.BaseHook.get_connection",
        return_value=mock_conn
    )

    result = is_spark_available_callable()

    assert result is True

def test_spark_missing_master(mocker):
    """Spark unavailable when 'master' is NOT in extra_dejson."""
    mock_conn = MagicMock()
    mock_conn.extra_dejson = {}   # No master field

    mocker.patch(
        "include.is_available.is_spark_available.BaseHook.get_connection",
        return_value=mock_conn
    )

    result = is_spark_available_callable()

    assert result is False

def test_spark_connection_exception(mocker):
    """Spark unavailable when get_connection raises exception."""
    mocker.patch(
        "include.is_available.is_spark_available.BaseHook.get_connection",
        side_effect=Exception("boom")
    )

    result = is_spark_available_callable()

    assert result is False