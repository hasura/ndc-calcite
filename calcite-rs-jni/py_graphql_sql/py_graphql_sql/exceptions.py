"""DB-API 2.0 required exceptions."""

class Error(Exception):
    """Base class for all DB-API 2.0 errors."""

class Warning(Exception):  # pylint: disable=redefined-builtin
    """Important warnings."""

class InterfaceError(Error):
    """Database interface errors."""

class DatabaseError(Error):
    """Database errors."""

class DataError(DatabaseError):
    """Data errors."""

class OperationalError(DatabaseError):
    """Database operation errors."""

class IntegrityError(DatabaseError):
    """Database integrity errors."""

class InternalError(DatabaseError):
    """Database internal errors."""

class ProgrammingError(DatabaseError):
    """Programming errors."""

class NotSupportedError(DatabaseError):
    """Feature not supported errors."""
