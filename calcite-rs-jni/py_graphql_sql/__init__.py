"""DB-API 2.0 compliant JDBC connector for GraphQL."""
# Set up logging
import logging
logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)

# Re-export everything from the inner module
from .py_graphql_sql import (
    # DB-API 2.0 main exports
    connect, Connection, Cursor,
    apilevel, threadsafety, paramstyle,

    # DB-API 2.0 type objects
    STRING, BINARY, NUMBER, DATETIME, ROWID,

    # DB-API 2.0 type constructors
    Date, Time, Timestamp,
    DateFromTicks, TimeFromTicks, TimestampFromTicks,
    Binary,

    # DB-API 2.0 exceptions
    Warning, Error, InterfaceError, DatabaseError,
    DataError, OperationalError, IntegrityError,
    InternalError, ProgrammingError, NotSupportedError,

    # Version info
    VERSION, __version__,

    # SQLAlchemy dialect
    HasuraDDNDialect
)

# SQLAlchemy registration
from sqlalchemy.dialects import registry
logger.debug("Attempting to register the 'hasura.py_graphql_sql' dialect.")
registry.register('hasura.py_graphql_sql', 'py_graphql_sql.sqlalchemy.hasura.ddnbase', 'HasuraDDNDialect')
registry.register('hasura', 'py_graphql_sql.sqlalchemy.hasura.ddnbase', 'HasuraDDNDialect')
logger.debug("Registered the 'hasura.py_graphql_sql' dialect.")

# Make all these available at package level
__all__ = [
    'connect', 'Connection', 'Cursor',
    'apilevel', 'threadsafety', 'paramstyle',
    'STRING', 'BINARY', 'NUMBER', 'DATETIME', 'ROWID',
    'Date', 'Time', 'Timestamp',
    'DateFromTicks', 'TimeFromTicks', 'TimestampFromTicks',
    'Binary',
    'Warning', 'Error', 'InterfaceError', 'DatabaseError',
    'DataError', 'OperationalError', 'IntegrityError',
    'InternalError', 'ProgrammingError', 'NotSupportedError',
    'VERSION', '__version__',
    'HasuraDDNDialect'
]
