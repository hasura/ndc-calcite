import importlib

from sqlalchemy.engine import default
from sqlalchemy import types
from sqlalchemy.types import (
    Integer, Float, String, DateTime, Boolean,
    Date, Time, TIMESTAMP, DECIMAL
)
from typing import Any, Dict, List, Optional
import logging

logger = logging.getLogger(__name__)


class HasuraDDNDialect(default.DefaultDialect):
    name = 'hasura'
    driver = 'py_graphql_sql'

    # Existing flags
    supports_alter = False
    supports_transactions = False
    supports_native_boolean = True
    supports_statement_cache = False
    postfetch_lastrowid = False

    # Schema Support
    supports_schemas = True  # Calcite does support schemas via INFORMATION_SCHEMA

    # View Support
    supports_views = True  # Calcite supports views in its SQL layer

    # Row Count Support
    supports_sane_rowcount = True  # Calcite provides accurate row counts for queries
    supports_sane_multi_rowcount = False  # Multiple statement execution not typically used

    # Insert/Update Features - Should all be False for read-only
    supports_default_values = False  # No INSERT support
    supports_empty_insert = False  # No INSERT support
    supports_multivalues_insert = False  # No INSERT support
    implicit_returning = False  # No RETURNING clause since no writes

    # SQL Language Features
    requires_name_normalize = False  # Calcite handles case sensitivity properly
    supports_native_decimal = True  # Calcite supports DECIMAL type
    supports_unicode_statements = True  # Calcite handles Unicode in SQL
    supports_unicode_binds = True  # Calcite handles Unicode parameters
    supports_is_distinct_from = True  # Calcite supports IS DISTINCT FROM

    # Additional Calcite-specific flags
    supports_window_functions = True  # Calcite supports window functions
    supports_json = True  # Calcite has JSON operations
    supports_native_arrays = False  # Conservative setting for array types

    # Type mapping from JDBC/Calcite types to SQLAlchemy
    type_map = {
        'INTEGER': Integer,
        'INT': Integer,
        'FLOAT': Float,
        'VARCHAR': String,
        'TIMESTAMP': TIMESTAMP,
        'TIMESTAMP(0)': TIMESTAMP,  # Add support for timestamp with precision
        'BOOLEAN': Boolean,
        'DATE': Date,
        'TIME': Time,
        'DECIMAL': DECIMAL,
        'JavaType(int)': Integer,
        'JavaType(class java.lang.String)': String,
        'JavaType(class java.lang.Integer)': Integer,
        'JavaType(class java.lang.Short)': Integer,
    }

    def get_schema_names(self, connection, **kw) -> List[str]:
        """Return fixed schema names."""
        try:
            return ['GRAPHQL', 'metadata']
        except Exception as e:
            logger.error(f"Error getting schema names: {str(e)}")
            return []

    def get_table_names(self, connection, schema: Optional[str] = None, **kw) -> List[str]:
        """Get table names from metadata.TABLES."""
        if schema is None:
            schema = 'GRAPHQL'

        try:
            query = """
               SELECT DISTINCT tableName 
               FROM metadata.TABLES 
               WHERE tableSchem = ?
               ORDER BY tableName
               """
            result = connection.execute(query, [schema])
            return [row[0] for row in result]
        except Exception as e:
            logger.error(f"Error getting table names for schema {schema}: {str(e)}")
            return []

    def get_columns(self, connection, table_name: str, schema: Optional[str] = None, **kw) -> List[Dict]:
        """Get column information from metadata.COLUMNS."""
        if schema is None:
            schema = 'GRAPHQL'

        try:
            query = """
               SELECT 
                   columnName,
                   typeName,
                   nullable,
                   columnSize,
                   decimalDigits,
                   numPrecRadix,
                   ordinalPosition,
                   columnDef,
                   isNullable
               FROM metadata.COLUMNS 
               WHERE tableSchem = ?
               AND tableName = ?
               ORDER BY ordinalPosition
               """
            result = connection.execute(query, [schema, table_name])

            columns = []
            for row in result:
                # Convert JDBC type info to SQLAlchemy type
                type_name = row[1]
                column_size = row[3]
                decimal_digits = row[4]
                nullable = row[2] == 1  # JDBC nullable is an int

                # Determine SQLAlchemy type
                sql_type = self._get_column_type(type_name, column_size, decimal_digits)

                column = {
                    'name': row[0],
                    'type': sql_type,
                    'nullable': nullable,
                    'default': row[7],  # columnDef
                    'autoincrement': False,  # Read-only connection
                    'primary_key': False,  # Would need additional metadata
                    'ordinal_position': row[6],
                }
                columns.append(column)
            return columns

        except Exception as e:
            logger.error(f"Error getting columns for {schema}.{table_name}: {str(e)}")
            return []

    def _get_column_type(self, type_name: str, size: Optional[int],
                         decimal_digits: Optional[int]) -> types.TypeEngine:
        """Convert JDBC type information to SQLAlchemy type."""
        if type_name is None:
            return String()

        # Handle full type name including precision/scale
        type_name = type_name.strip()

        # Direct lookup first
        if type_name in self.type_map:
            base_type = self.type_map[type_name]
        else:
            # Try without precision/scale
            base_name = type_name.split('(')[0].upper()
            base_type = self.type_map.get(base_name, String)

        # Add precision/scale for specific types
        if base_type == DECIMAL and size is not None:
            return DECIMAL(precision=size, scale=decimal_digits or 0)
        elif base_type == String and size is not None:
            return String(length=size)

        return base_type()

    def get_view_names(self, connection, schema: Optional[str] = None, **kw) -> List[str]:
        """Get view names if any exist."""
        if schema is None:
            schema = 'GRAPHQL'

        try:
            query = """
               SELECT DISTINCT tableName 
               FROM metadata.TABLES 
               WHERE tableSchem = ?
               AND tableType = 'VIEW'
               ORDER BY tableName
               """
            result = connection.execute(query, [schema])
            return [row[0] for row in result]
        except Exception as e:
            logger.error(f"Error getting view names for schema {schema}: {str(e)}")
            return []

    def has_table(self, connection, table_name: str, schema: Optional[str] = None, **kw) -> bool:
        """Check if a table exists in the given schema."""
        if schema is None:
            schema = 'GRAPHQL'

        try:
            query = """
                SELECT 1
                FROM metadata.TABLES
                WHERE tableSchem = ?
                AND tableName = ?
                """
            result = connection.execute(query, [schema, table_name])
            return result.fetchone() is not None
        except Exception as e:
            logger.error(f"Error checking if table exists {schema}.{table_name}: {str(e)}")
            return False

    @classmethod
    def dbapi(cls):
        try:
            logger.info(f"Called Hasura dbapi; {cls.driver}")
            driver_module = importlib.import_module(cls.driver)
            return driver_module
        except Exception as e:
            logger.info(f"Error loading Hasura dbapi: {e}")
            raise

    def create_connect_args(self, url):
        """Convert SQLAlchemy URL to your connect() parameters"""
        jdbc_args = dict(url.query)
        host = jdbc_args.pop('url', '')
        return [], {
            'host': host,
            'jdbc_args': jdbc_args
        }

    def do_rollback(self, dbapi_connection):
        """Don't roll back - this is a read-only connection"""
        pass

    # Stub implementations for unsupported features
    def get_pk_constraint(self, connection, table_name: str, schema: Optional[str] = None, **kw) -> Dict:
        """Not implemented for read-only connection."""
        return {'constrained_columns': [], 'name': None}

    def get_foreign_keys(self, connection, table_name: str, schema: Optional[str] = None, **kw) -> List:
        """Not implemented for read-only connection."""
        return []

    def get_indexes(self, connection, table_name: str, schema: Optional[str] = None, **kw) -> List:
        """Not implemented for read-only connection."""
        return []
