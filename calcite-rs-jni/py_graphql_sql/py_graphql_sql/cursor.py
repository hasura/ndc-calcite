"""DB-API 2.0 Cursor implementation."""
from __future__ import annotations
from typing import Any, Optional, Sequence, Tuple, Type
from contextlib import AbstractContextManager

from .db_types import ConnectionProtocol, Row, RowSequence
from .exceptions import DatabaseError


class Cursor(AbstractContextManager["Cursor"]):
    """DB-API 2.0 Cursor class."""

    description: Optional[
        Sequence[
            Tuple[
                str,  # name
                Any,  # type_code
                Optional[int],  # display_size
                Optional[int],  # internal_size
                Optional[int],  # precision
                Optional[int],  # scale
                Optional[bool],  # null_ok
            ]
        ]
    ]

    def __init__(self, connection: ConnectionProtocol) -> None:
        """Initialize cursor."""
        self._connection = connection
        self._cursor = connection.jdbc_connection.cursor()
        self.arraysize: int = 1
        self.description = None
        self.rowcount: int = -1

    def __enter__(self) -> Cursor:
        """Enter context manager."""
        return self

    def __exit__(
        self,
        exc_type: Optional[Type[BaseException]],
        exc_val: Optional[BaseException],
        exc_tb: Optional[Any],
    ) -> None:
        """Exit context manager."""
        self.close()

    def close(self) -> None:
        """Close the cursor."""
        self._cursor.close()

    def execute(self, operation: str, parameters: Optional[Sequence[Any]] = None) -> Cursor:
        """Execute a database operation."""
        try:
            if parameters:
                self._cursor.execute(operation, parameters)
            else:
                self._cursor.execute(operation)

            if self._cursor.description:
                self.description = self._cursor.description

            self.rowcount = self._cursor.rowcount
            return self
        except Exception as e:
            raise DatabaseError(str(e)) from e

    def executemany(self, operation: str, seq_of_parameters: Sequence[Sequence[Any]]) -> None:
        """Execute the same operation multiple times."""
        try:
            for parameters in seq_of_parameters:
                self.execute(operation, parameters)
        except Exception as e:
            raise DatabaseError(str(e)) from e

    def fetchone(self) -> Optional[Row]:
        """Fetch the next row."""
        try:
            row = self._cursor.fetchone()
            if row is None:
                return None
            return tuple(row) if isinstance(row, (list, tuple)) else (row,)
        except Exception as e:
            raise DatabaseError(str(e)) from e

    def fetchmany(self, size: Optional[int] = None) -> RowSequence:
        """Fetch the next set of rows."""
        try:
            if size is None:
                size = self.arraysize
            rows = self._cursor.fetchmany(size)
            if not rows:
                return []
            return [tuple(row) if isinstance(row, (list, tuple)) else (row,) for row in rows]
        except Exception as e:
            raise DatabaseError(str(e)) from e

    def fetchall(self) -> RowSequence:
        """Fetch all remaining rows."""
        try:
            rows = self._cursor.fetchall()
            if not rows:
                return []
            return [tuple(row) if isinstance(row, (list, tuple)) else (row,) for row in rows]
        except Exception as e:
            raise DatabaseError(str(e)) from e
