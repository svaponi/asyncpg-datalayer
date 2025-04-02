import asyncpg


class _DatalayerException(Exception):
    def __init__(self, cause: Exception, message: str | None = None) -> None:
        super().__init__(
            message
            if message
            else f"caused by {type(cause).__module__}.{type(cause).__name__}: {cause}"
        )
        self.__cause__ = cause


class PoolOverflowException(_DatalayerException):
    """
    Raised when the connection pool is full.
    Can be mitigated by increasing the pool_size in backend or decreasing max_instance_request_concurrency in Cloud Run.
    """

    def __init__(self, cause: Exception) -> None:
        super().__init__(cause)


class TooManyConnectionsException(_DatalayerException):
    """
    Raised when the database cannot accept new connections.
    Can be mitigated by increasing the max_connection param in the database.
    """

    def __init__(self, cause: Exception) -> None:
        super().__init__(cause)


class ConstraintViolationException(_DatalayerException):
    """
    Raised when an operation fails due to a database-level constraint, such as a unique key violation,
    foreign key constraint failure, or check constraint breach.
    """

    def __init__(self, cause: asyncpg.IntegrityConstraintViolationError) -> None:
        super().__init__(cause, getattr(cause, "message", "constraint violation"))
