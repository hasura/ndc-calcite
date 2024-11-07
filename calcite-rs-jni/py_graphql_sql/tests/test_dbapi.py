"""Tests for DB-API 2.0 compliance."""

from py_graphql_sql import (
    Error,
    Warning,  # pylint: disable=redefined-builtin
    apilevel,
    connect,
    paramstyle,
    threadsafety,
)

def test_dbapi_globals() -> None:
    """Test that required DB-API 2.0 globals are present and correct."""
    assert apilevel == '2.0'
    assert threadsafety in (0, 1, 2, 3)
    assert paramstyle in ('qmark', 'numeric', 'named', 'format', 'pyformat')

def test_exceptions_hierarchy() -> None:
    """Test that the exception hierarchy is correct."""
    assert issubclass(Warning, Exception)
    assert issubclass(Error, Exception)

def test_module_interface() -> None:
    """Test that the module interface is complete."""
    assert hasattr(connect, '__call__')
