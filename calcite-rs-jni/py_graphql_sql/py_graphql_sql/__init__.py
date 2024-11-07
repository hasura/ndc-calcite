"""DB-API 2.0 compliant JDBC connector."""

from .connection import Connection, connect
from .cursor import Cursor
from .exceptions import (
    Error, Warning, InterfaceError, DatabaseError,
    DataError, OperationalError, IntegrityError,
    InternalError, ProgrammingError, NotSupportedError
)

# DB-API 2.0 required globals
apilevel = '2.0'
threadsafety = 1  # Threads may share the module but not connections
paramstyle = 'qmark'  # Question mark style, e.g. ...WHERE name=?

__all__ = [
    'Connection',
    'Cursor',
    'connect',
    'apilevel',
    'threadsafety',
    'paramstyle',
    'Error',
    'Warning',
    'InterfaceError',
    'DatabaseError',
    'DataError',
    'OperationalError',
    'IntegrityError',
    'InternalError',
    'ProgrammingError',
    'NotSupportedError',
]
