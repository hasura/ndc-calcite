"""PyTest configuration and fixtures."""
from typing import Any
from unittest.mock import MagicMock
import jaydebeapi
import pytest
from py_graphql_sql import Connection, Cursor, connect

@pytest.fixture
def mock_jdbc_connection() -> MagicMock:
    """Create a mock JDBC connection."""
    conn = MagicMock()
    conn.cursor.return_value = MagicMock()
    return conn

@pytest.fixture
def mock_connection(monkeypatch: pytest.MonkeyPatch, mock_jdbc_connection: MagicMock) -> Connection:
    """Create a mock connection."""
    def mock_connect(*args: Any, **kwargs: Any) -> MagicMock:
        return mock_jdbc_connection

    monkeypatch.setattr(jaydebeapi, 'connect', mock_connect)
    return connect('driver', 'url')

@pytest.fixture
def mock_cursor(mock_connection: Connection) -> Cursor:
    """Create a mock cursor."""
    return mock_connection.cursor()
