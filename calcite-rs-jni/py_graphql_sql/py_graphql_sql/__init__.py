"""DB-API 2.0 compliant JDBC connector for GraphQL."""
import logging
from datetime import date, datetime, time, timedelta
from typing import Any, Type

from .connection import connect, Connection
from .cursor import Cursor
from .exceptions import (
    Warning, Error, InterfaceError, DatabaseError,
    DataError, OperationalError, IntegrityError, InternalError,
    ProgrammingError, NotSupportedError
)

# Add SQLAlchemy dialect registration
from sqlalchemy.dialects import registry
from .sqlalchemy.hasura.ddnbase import HasuraDDNDialect

# Set up logging
logging.basicConfig(level=logging.DEBUG)
# logging.getLogger('sqlalchemy.engine').setLevel(logging.INFO)
logger = logging.getLogger(__name__)

try:
    logger.debug("Attempting to register the 'hasura.py_graphql_sql' dialect.")
    registry.register('hasura.py_graphql_sql', 'py_graphql_sql.sqlalchemy.hasura.ddnbase', 'HasuraDDNDialect')
    registry.register('hasura', 'py_graphql_sql.sqlalchemy.hasura.ddnbase', 'HasuraDDNDialect')
    logger.debug(registry.load('hasura.py_graphql_sql'))
    logger.debug("Successfully registered the 'hasura.py_graphql_sql' dialect.")
except Exception as e:
    logger.error("Failed to register the 'hasura.py_graphql_sql' dialect.", exc_info=True)

# DB-API 2.0 Module Interface
apilevel = "2.0"
threadsafety = 1  # Threads may share module, but not connections
paramstyle = "qmark"  # JDBC uses ? style

# DB-API 2.0 Type Objects
class DBAPITypeObject:
    def __init__(self, *values: str) -> None:
        self.values = values

    def __eq__(self, other: Any) -> bool:
        return other in self.values

    def __repr__(self) -> str:
        return f'DBAPITypeObject{self.values!r}'


# Required DB API type objects
STRING = DBAPITypeObject('VARCHAR', 'CHAR', 'TEXT', 'STRING')
BINARY = DBAPITypeObject('BLOB', 'BINARY', 'VARBINARY')
NUMBER = DBAPITypeObject('INT', 'INTEGER', 'FLOAT', 'REAL', 'DOUBLE', 'DECIMAL', 'NUMERIC')
DATETIME = DBAPITypeObject('DATETIME', 'TIMESTAMP')
ROWID = DBAPITypeObject('ROWID')

# Required type constructors
def Date(year: int, month: int, day: int) -> date:
    """Construct a date value."""
    return date(year, month, day)

def Time(hour: int, minute: int, second: int) -> time:
    """Construct a time value."""
    return time(hour, minute, second)

def Timestamp(year: int, month: int, day: int,
              hour: int, minute: int, second: int) -> datetime:
    """Construct a timestamp value."""
    return datetime(year, month, day, hour, minute, second)

def DateFromTicks(ticks: float) -> date:
    """Construct a date from UNIX ticks."""
    return date.fromtimestamp(ticks)

def TimeFromTicks(ticks: float) -> time:
    """Construct a time from UNIX ticks."""
    return datetime.fromtimestamp(ticks).time()

def TimestampFromTicks(ticks: float) -> datetime:
    """Construct a timestamp from UNIX ticks."""
    return datetime.fromtimestamp(ticks)

# Binary constructor
def Binary(string: bytes) -> bytes:
    """Construct a binary object."""
    return bytes(string)

# Version information
VERSION = (0, 1, 0)
__version__ = '.'.join(map(str, VERSION))

__all__ = [
    # DB-API 2.0 main exports
    'connect', 'Connection', 'Cursor',
    'apilevel', 'threadsafety', 'paramstyle',

    # DB-API 2.0 type objects
    'STRING', 'BINARY', 'NUMBER', 'DATETIME', 'ROWID',

    # DB-API 2.0 type constructors
    'Date', 'Time', 'Timestamp',
    'DateFromTicks', 'TimeFromTicks', 'TimestampFromTicks',
    'Binary',

    # DB-API 2.0 exceptions
    'Warning', 'Error', 'InterfaceError', 'DatabaseError',
    'DataError', 'OperationalError', 'IntegrityError',
    'InternalError', 'ProgrammingError', 'NotSupportedError',

    # Version info
    'VERSION', '__version__',

    # Add SQLAlchemy dialect
    'HasuraDDNDialect'
]
