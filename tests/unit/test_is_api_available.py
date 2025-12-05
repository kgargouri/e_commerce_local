import pytest
from unittest.mock import MagicMock
from airflow.sensors.base import PokeReturnValue
from include.is_available.is_api_available import is_api_available_callable

def test_api_available_200(mocker):
    # ---- Mock connection ----
    mock_conn = MagicMock()
    mock_conn.host = "https://dummyjson.com"
    mock_conn.extra_dejson = {
        "endpoint": "/products",
        "headers": {"Authorization": "Token 123"}
    }

    mocker.patch(
        "include.is_available.is_api_available.BaseHook.get_connection",
        return_value=mock_conn
    )

    # ---- Mock request ----
    mock_response = MagicMock()
    # simulation 200 :
    mock_response.status_code = 200

    mocker.patch(
        "include.is_available.is_api_available.requests.get",
        return_value=mock_response
    )

    # ---- Call ----
    result = is_api_available_callable()

    # ---- Assertions ----
    assert isinstance(result, PokeReturnValue)
    assert result.is_done is True
    assert result.xcom_value == "https://dummyjson.com/products"


def test_api_not_available_status_not_200(mocker):
    # ---- Mock connection ----
    mock_conn = MagicMock()
    mock_conn.host = "https://dummyjson.com"
    mock_conn.extra_dejson = {
        "endpoint": "/products",
        "headers": {}
    }

    mocker.patch(
        "include.is_available.is_api_available.BaseHook.get_connection",
        return_value=mock_conn
    )

    # ---- Mock request ----
    mock_response = MagicMock()
    # simulation 404 :
    mock_response.status_code = 404

    mocker.patch(
        "include.is_available.is_api_available.requests.get",
        return_value=mock_response
    )

    # ---- Call ----
    result = is_api_available_callable()

    # ---- Assertions ----
    assert result is False


def test_api_exception(mocker):
    # ---- Mock connection ----
    mock_conn = MagicMock()
    mock_conn.host = "https://dummyjson.com"
    mock_conn.extra_dejson = {"endpoint": "/products", "headers": {}}

    mocker.patch(
        "include.is_available.is_api_available.BaseHook.get_connection",
        return_value=mock_conn
    )

    # ---- Mock request ----
    # simulate network failure
    mocker.patch(
        "include.is_available.is_api_available.requests.get",
        side_effect=Exception("boom")
    )

    # ---- Call ----
    result = is_api_available_callable()

    # ---- Assertions ----
    assert result is False
