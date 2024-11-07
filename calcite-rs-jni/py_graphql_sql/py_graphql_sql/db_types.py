""""Type definitions for the DB-API package."""
from typing import Any, Optional, Sequence, Tuple, Protocol
from typing_extensions import TypeAlias


# Row type definitions
Row: TypeAlias = Tuple[Any, ...]
RowSequence: TypeAlias = Sequence[Row]

# JDBC related types
JDBCArgs: TypeAlias = Optional[Sequence[Any]]
JDBCPath: TypeAlias = Optional[str]


class ConnectionProtocol(Protocol):
    """Protocol defining the Connection interface."""

    closed: bool
    _jdbc_connection: Any

    def close(self) -> None: ...

    def commit(self) -> None: ...

    def rollback(self) -> None: ...

    def cursor(self) -> "CursorProtocol": ...

    @property
    def jdbc_connection(self):
        return self._jdbc_connection


class CursorProtocol(Protocol):
    """Protocol defining the Cursor interface."""

    arraysize: int
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

    def close(self) -> None: ...

    def execute(
        self, operation: str, parameters: Optional[Sequence[Any]] = None
    ) -> "CursorProtocol": ...

    def executemany(
        self, operation: str, seq_of_parameters: Sequence[Sequence[Any]]
    ) -> None: ...

    def fetchone(self) -> Optional[Row]: ...

    def fetchmany(self, size: Optional[int] = None) -> RowSequence: ...

    def fetchall(self) -> RowSequence: ...
