"""DB-API 2.0 Connection implementation for read-only GraphQL JDBC connector."""
from __future__ import annotations
from contextlib import AbstractContextManager
from typing import Optional, Any, List, Type

import jaydebeapi
import jpype
import os
import glob

from .exceptions import DatabaseError, InterfaceError, NotSupportedError
from .db_types import JDBCArgs, JDBCPath

JDBC_DRIVER = "com.hasura.GraphQLDriver"
EXCLUDED_JAR = "graphql-jdbc-driver-1.0.0.jar"

class Connection(AbstractContextManager):
    """DB-API 2.0 Connection class for read-only operations."""

    def __init__(
        self,
        host: str,
        jdbc_args: Optional[JDBCArgs] = None,
        driver_paths: Optional[List[str]] = None
    ) -> None:
        """Initialize connection."""
        try:
            # Start JVM if it's not already started
            if not jpype.isJVMStarted():
                # Build classpath from all JARs in provided directories
                classpath: List[str] = []
                if driver_paths:
                    for path in driver_paths:
                        if not os.path.exists(path):
                            raise DatabaseError(f"Driver path not found: {path}")

                        # Find all JAR files in the directory
                        jar_files = glob.glob(os.path.join(path, "*.jar"))

                        # Add all JARs except the excluded one
                        for jar in jar_files:
                            if os.path.basename(jar) != EXCLUDED_JAR:
                                classpath.append(jar)

                if not classpath:
                    raise DatabaseError("No JAR files found in provided paths")

                # Join all paths with OS-specific path separator
                classpath_str = os.pathsep.join(classpath)

                jpype.startJVM(
                    jpype.getDefaultJVMPath(),
                    f"-Djava.class.path={classpath_str}",
                    convertStrings=True
                )

            # Construct JDBC URL
            jdbc_url = f"jdbc:graphql:{host}"

            # Create Properties object
            props = jpype.JClass('java.util.Properties')()

            # Add any properties from jdbc_args
            if jdbc_args:
                if isinstance(jdbc_args, dict):
                    for key, value in jdbc_args.items():
                        props.setProperty(key, str(value))
                elif isinstance(jdbc_args, list) and len(jdbc_args) > 0:
                    props.setProperty("role", jdbc_args[0])

            # Connect using URL and properties
            self._jdbc_connection = jaydebeapi.connect(
                jclassname=JDBC_DRIVER,
                url=jdbc_url,
                driver_args=[props],
                jars=None
            )
            self.closed: bool = False

        except Exception as e:
            raise DatabaseError(f"Failed to connect: {str(e)}") from e

    def __enter__(self) -> 'Connection':
        """Enter context manager."""
        return self

    def __exit__(
        self,
        exc_type: Optional[Type[BaseException]],
        exc_val: Optional[BaseException],
        exc_tb: Optional[Any]
    ) -> None:
        """Exit context manager."""
        self.close()

    def close(self) -> None:
        """Close the connection."""
        if not self.closed:
            try:
                self._jdbc_connection.close()
            finally:
                self.closed = True

    def commit(self) -> None:
        """Not supported in read-only connection."""
        raise NotSupportedError("This is a read-only connection")

    def rollback(self) -> None:
        """Not supported in read-only connection."""
        raise NotSupportedError("This is a read-only connection")

    @property
    def autocommit(self) -> bool:
        """Always True for read-only connection."""
        return True

    @autocommit.setter
    def autocommit(self, value: bool) -> None:
        """Not supported in read-only connection."""
        if not value:
            raise NotSupportedError("This is a read-only connection")

    def cursor(self) -> Any:
        """Create a new cursor."""
        if self.closed:
            raise InterfaceError("Connection is closed")
        return self._jdbc_connection.cursor()

    @property
    def jdbc_connection(self) -> Any:
        """Get the underlying JDBC connection."""
        if self.closed:
            raise InterfaceError("Connection is closed")
        return self._jdbc_connection

def connect(
    host: str,
    jdbc_args: Optional[JDBCArgs] = None,
    driver_paths: Optional[List[str]] = None,
) -> Connection:
    """
    Create a new read-only database connection.

    Args:
        host: The GraphQL server host (e.g., 'http://localhost:3000/graphql')
        jdbc_args: Optional connection arguments (dict or list)
        driver_paths: List of paths to directories containing JAR files

    Returns:
        Connection: A new database connection
    """
    return Connection(host, jdbc_args, driver_paths)
