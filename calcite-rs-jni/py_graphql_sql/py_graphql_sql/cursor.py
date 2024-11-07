"""DB-API 2.0 Cursor implementation."""
from __future__ import annotations
from typing import Any, Optional, Sequence, Tuple, Type
from contextlib import AbstractContextManager

from .db_types import ConnectionProtocol, Row, RowSequence
from .exceptions import DatabaseError, InterfaceError, ProgrammingError

class Cursor(AbstractContextManager):
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
        self._lastrowid = None
        self.closed = False

    def __enter__(self) -> 'Cursor':
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

    @property
    def connection(self) -> ConnectionProtocol:
        """Return the cursor's connection."""
        return self._connection

    @property
    def lastrowid(self) -> Optional[Any]:
        """Return the rowid of the last modified row."""
        return self._lastrowid

    def close(self) -> None:
        """Close the cursor."""
        if not self.closed:
            try:
                self._cursor.close()
            finally:
                self.closed = True

    def _check_closed(self) -> None:
        """Check if cursor is closed."""
        if self.closed:
            raise InterfaceError("Cursor is closed")

    def execute(self, operation: str, parameters: Optional[Sequence[Any]] = None) -> 'Cursor':
        """Execute a database operation."""
        self._check_closed()
        try:
            if parameters:
                self._cursor.execute(operation, parameters)
            else:
                self._cursor.execute(operation)

            if self._cursor.description:
                self.description = self._cursor.description

            self.rowcount = self._cursor.rowcount

            # Attempt to get lastrowid if available
            try:
                self._lastrowid = self._cursor.lastrowid
            except AttributeError:
                self._lastrowid = None

            return self
        except Exception as e:
            raise DatabaseError(str(e)) from e

    def executemany(self, operation: str, seq_of_parameters: Sequence[Sequence[Any]]) -> None:
        """Execute the same operation multiple times."""
        self._check_closed()
        try:
            for parameters in seq_of_parameters:
                self.execute(operation, parameters)
        except Exception as e:
            raise DatabaseError(str(e)) from e

    def fetchone(self) -> Optional[Row]:
        """Fetch the next row."""
        self._check_closed()
        try:
            row = self._cursor.fetchone()
            if row is None:
                return None
            return tuple(row) if isinstance(row, (list, tuple)) else (row,)
        except Exception as e:
            raise DatabaseError(str(e)) from e

    def fetchmany(self, size: Optional[int] = None) -> RowSequence:
        """Fetch the next set of rows."""
        self._check_closed()
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
        self._check_closed()
        try:
            rows = self._cursor.fetchall()
            if not rows:
                return []
            return [tuple(row) if isinstance(row, (list, tuple)) else (row,) for row in rows]
        except Exception as e:
            raise DatabaseError(str(e)) from e

    def nextset(self) -> Optional[bool]:
        """Move to next result set."""
        self._check_closed()
        try:
            has_next = self._cursor.nextset()
            if has_next:
                if self._cursor.description:
                    self.description = self._cursor.description
                self.rowcount = self._cursor.rowcount
            return has_next
        except AttributeError:
            return None
        except Exception as e:
            raise DatabaseError(str(e)) from e

    def setinputsizes(self, sizes: Sequence[Any]) -> None:
        """Predefine memory areas for parameters."""
        self._check_closed()
        # Implementation optional per DB-API spec
        pass

    def setoutputsize(self, size: int, column: Optional[int] = None) -> None:
        """Set a column buffer size for fetches of large columns."""
        self._check_closed()
        # Implementation optional per DB-API spec
        pass
