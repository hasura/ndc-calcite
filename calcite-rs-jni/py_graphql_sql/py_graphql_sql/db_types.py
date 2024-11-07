"""Type definitions for the DB-API package."""
from typing import Any, Dict, Optional, Protocol, Sequence, Tuple, Union

class ConnectionProtocol(Protocol):
    """Protocol defining the Connection interface."""

    closed: bool
    _jdbc_connection: Any

    def close(self) -> None: ...

    def commit(self) -> None: ...

    def rollback(self) -> None: ...

    def cursor(self) -> "CursorProtocol": ...

    @property
    def autocommit(self) -> bool: ...

    @autocommit.setter
    def autocommit(self, value: bool) -> None: ...

    @property
    def jdbc_connection(self) -> Any:
        """Return the JDBC connection object."""
        return self._jdbc_connection


class CursorProtocol(Protocol):
    """Protocol defining the Cursor interface."""

    arraysize: int
    closed: bool
    description: Optional[
        Sequence[
            Tuple[
                str,
                Any,
                Optional[int],
                Optional[int],
                Optional[int],
                Optional[int],
                Optional[bool],
            ]
        ]
    ]
    rowcount: int
    lastrowid: Optional[Any]

    def close(self) -> None: ...

    def execute(
        self, operation: str, parameters: Optional[Sequence[Any]] = None
    ) -> "CursorProtocol": ...

    def executemany(
        self, operation: str, seq_of_parameters: Sequence[Sequence[Any]]
    ) -> None: ...

    def fetchone(self) -> Optional[Tuple[Any, ...]]: ...

    def fetchmany(self, size: Optional[int] = None) -> Sequence[Tuple[Any, ...]]: ...

    def fetchall(self) -> Sequence[Tuple[Any, ...]]: ...

    def nextset(self) -> Optional[bool]: ...

    def setinputsizes(self, sizes: Sequence[Any]) -> None: ...

    def setoutputsize(self, size: int, column: Optional[int] = None) -> None: ...


# Type definitions
Row = Tuple[Any, ...]
RowSequence = Sequence[Row]
JDBCArgs = Optional[Union[Sequence[Any], Dict[str, Any]]]
JDBCPath = Optional[str]

__all__ = [
    'ConnectionProtocol',
    'CursorProtocol',
    'Row',
    'RowSequence',
    'JDBCArgs',
    'JDBCPath',
]
