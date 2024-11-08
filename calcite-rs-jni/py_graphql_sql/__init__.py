"""DB-API 2.0 compliant JDBC connector for GraphQL."""

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
registry.register('hasura.graphql', 'py_graphql_sql.sqlalchemy.hasura.ddnbase', 'HasuraDDNDialect')

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
