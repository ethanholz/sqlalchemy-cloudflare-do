"""SQLAlchemy for SQLite-backed Cloudflare Durable Objects."""

from collections.abc import Callable

from sqlalchemy import create_engine
from sqlalchemy.dialects import registry
from sqlalchemy.engine import Connection, Engine

from . import dbapi

# Also works when bundled in a Worker without installed entry-point metadata.
registry.register(
    "cloudflare_do", "sqlalchemy_cloudflare_do.dialect", "DurableObjectDialect"
)

__all__ = ["create_engine_from_storage", "run_in_transaction"]


def create_engine_from_storage(storage, **kwargs) -> Engine:
    """Create an autocommit engine for the current Durable Object.

    Parameters
    ----------
    storage
        SQLite-backed ``ctx.storage``, not a namespace binding or object stub.
    **kwargs
        Additional SQLAlchemy ``create_engine`` options.
    """
    connect_args = dict(kwargs.pop("connect_args", {}))
    connect_args["storage"] = storage
    return create_engine("cloudflare_do://", connect_args=connect_args, **kwargs)


def run_in_transaction[T](engine: Engine, callback: Callable[[Connection], T]) -> T:
    """Run synchronous SQLAlchemy work inside ``storage.transactionSync()``.

    Parameters
    ----------
    engine
        An engine created for the current Durable Object's storage.
    callback
        A synchronous callable receiving a SQLAlchemy connection. Exceptions
        escape the storage callback and roll back all its writes. Do not
        return an awaitable, swallow errors, or perform asynchronous I/O.

    Returns
    -------
    T
        The callback's return value after the transaction succeeds.
    """
    with engine.connect() as connection:
        native = connection.connection.dbapi_connection
        if not isinstance(native, dbapi.Connection):
            raise TypeError("Expected a cloudflare_do engine")
        return native.run_in_transaction(lambda: callback(connection))
