"""Small DB-API 2.0 adapter for DurableObjectStorage (no network I/O)."""

import inspect
import math
from collections.abc import Mapping

apilevel = "2.0"
threadsafety = 0  # A binding belongs to one Durable Object's isolate.
paramstyle = "qmark"
Binary = bytes

try:
    from pyodide.ffi import jsnull as _jsnull
except ImportError:
    _jsnull = None


class Warning(Exception):
    pass


class Error(Exception):
    pass


class InterfaceError(Error):
    pass


class DatabaseError(Error):
    pass


class DataError(DatabaseError):
    pass


class OperationalError(DatabaseError):
    pass


class IntegrityError(DatabaseError):
    pass


class InternalError(DatabaseError):
    pass


class ProgrammingError(DatabaseError):
    pass


class NotSupportedError(DatabaseError):
    pass


def _database_error(error):
    message = str(error)
    lowered = message.lower()
    if "constraint failed" in lowered or "sqlite_constraint" in lowered:
        return IntegrityError(message)
    if "syntax error" in lowered or "no such" in lowered or "bindings" in lowered:
        return ProgrammingError(message)
    return OperationalError(message)


def _python(value):
    if value is _jsnull:
        return None
    if hasattr(value, "to_py"):
        value = value.to_py()
    if isinstance(value, (memoryview, bytearray)):
        return bytes(value)
    return value


def connect(*, storage):
    """Connect to the current object's SQLite-backed ``ctx.storage``."""
    return Connection(storage)


class Connection:
    """An autocommit connection; closing it never closes the object's storage."""

    def __init__(self, storage):
        try:
            from pyodide.ffi import JsProxy
        except ImportError:
            self._javascript = False
        else:
            if not isinstance(storage, JsProxy):
                # The Workers SDK wraps storage. Its public converter gives us
                # the raw binding, so we own callback proxy lifetimes ourselves.
                from workers import python_to_rpc

                storage = python_to_rpc(storage)
            self._javascript = isinstance(storage, JsProxy)
        if not callable(getattr(getattr(storage, "sql", None), "exec", None)):
            raise InterfaceError("Pass ctx.storage from a SQLite-backed Durable Object")
        if not callable(getattr(storage, "transactionSync", None)):
            raise InterfaceError("Storage must provide transactionSync()")
        self.storage = storage
        self.closed = False

    def _check_open(self):
        if self.closed:
            raise InterfaceError("Connection is closed")

    def cursor(self):
        self._check_open()
        return Cursor(self)

    def close(self):
        self.closed = True

    def commit(self):
        self._check_open()  # AUTOCOMMIT: transactionSync owns atomic boundaries.

    def rollback(self):
        self._check_open()  # AUTOCOMMIT: cannot undo statements already executed.

    def _bind(self, value):
        if value is None:
            return _jsnull
        if isinstance(value, str):
            return value
        if isinstance(value, bool):
            return int(value)
        if isinstance(value, int):
            if abs(value) > 2**53 - 1:
                raise DataError("Integer exceeds JavaScript's safe integer range")
            return value
        if isinstance(value, float):
            if not math.isfinite(value):
                raise DataError("SQL bindings must be finite numbers")
            return value
        if isinstance(value, (bytes, bytearray, memoryview)):
            value = bytes(value)
            if not self._javascript:
                return value
            from js import Uint8Array

            buffer = Uint8Array.new(len(value))
            buffer.assign(value)
            return buffer.buffer
        raise ProgrammingError(f"Unsupported SQL binding type: {type(value).__name__}")

    def _execute(self, operation, parameters):
        self._check_open()
        if not isinstance(operation, str):
            raise ProgrammingError("SQL must be a string")
        if isinstance(parameters, (Mapping, str, bytes, bytearray)):
            raise ProgrammingError(
                "Use a sequence of positional parameters, not a mapping"
            )
        bindings = tuple(self._bind(value) for value in parameters)
        try:
            native = self.storage.sql.exec(operation, *bindings)
            columns = list(_python(native.columnNames))
            rows = [
                tuple(_python(value) for value in row)
                for row in _python(native.raw().toArray())
            ]
            # rowsWritten includes index/trigger writes: it is NOT DB-API rowcount.
            written = native.rowsWritten
            metadata = self.storage.sql.exec("SELECT changes(), last_insert_rowid()")
            changes, lastrowid = _python(metadata.raw().toArray())[0]
            rowcount = int(changes) if not columns or written else -1
            return columns, rows, rowcount, int(lastrowid)
        except Error:
            raise
        except Exception as error:
            raise _database_error(error) from error

    def run_in_transaction(self, callback):
        """Run a synchronous callback atomically, preserving Python exceptions."""
        self._check_open()
        result = None
        failure = None

        def run():
            nonlocal result, failure
            try:
                result = callback()
                if inspect.isawaitable(result):
                    if inspect.iscoroutine(result):
                        result.close()
                    raise TypeError(
                        "transactionSync callbacks must not return awaitables"
                    )
            except BaseException as error:
                failure = error
                raise

        if self._javascript:
            from pyodide.ffi import create_proxy

            proxy = create_proxy(run)
        else:
            proxy = run
        try:
            self.storage.transactionSync(proxy)
        except BaseException:
            if failure is not None:
                raise failure
            raise
        finally:
            if self._javascript:
                proxy.destroy()
        return result


class Cursor:
    """Buffer native cursors synchronously so none survive an await boundary."""

    arraysize = 1

    def __init__(self, connection):
        self.connection = connection
        self.closed = False
        self._reset()

    def _reset(self):
        self.description = None
        self.rowcount = -1
        self.lastrowid = None
        self._rows = []
        self._position = 0

    def _check_open(self):
        self.connection._check_open()
        if self.closed:
            raise InterfaceError("Cursor is closed")

    def execute(self, operation, parameters=()):
        self._check_open()
        self._reset()
        columns, self._rows, self.rowcount, self.lastrowid = self.connection._execute(
            operation, parameters
        )
        if columns:
            self.description = [
                (name, None, None, None, None, None, None) for name in columns
            ]
        return self

    def executemany(self, operation, seq_of_parameters):
        self._check_open()
        self._reset()

        def batch():
            rowcount = 0
            for parameters in seq_of_parameters:
                self.execute(operation, parameters)
                if self.description is not None:
                    raise NotSupportedError(
                        "executemany() cannot return rows; use execute()"
                    )
                rowcount += self.rowcount
            return rowcount

        try:
            self.rowcount = self.connection.run_in_transaction(batch)
        except BaseException:
            self._reset()
            raise
        self.lastrowid = None  # Undefined for executemany in DB-API 2.0.
        return self

    def fetchone(self):
        self._check_open()
        if self.description is None:
            raise ProgrammingError("The statement did not return rows")
        if self._position == len(self._rows):
            return None
        row = self._rows[self._position]
        self._position += 1
        return row

    def fetchmany(self, size=None):
        self._check_open()
        if self.description is None:
            raise ProgrammingError("The statement did not return rows")
        size = self.arraysize if size is None else size
        if size < 0:
            raise ProgrammingError("Fetch size must not be negative")
        rows = []
        for _ in range(size):
            row = self.fetchone()
            if row is None:
                break
            rows.append(row)
        return rows

    def fetchall(self):
        self._check_open()
        if self.description is None:
            raise ProgrammingError("The statement did not return rows")
        rows = self._rows[self._position :]
        self._position = len(self._rows)
        return rows

    def __iter__(self):
        return self

    def __next__(self):
        row = self.fetchone()
        if row is None:
            raise StopIteration
        return row

    def close(self):
        self.closed = True
        # SQLAlchemy may read rowcount/lastrowid after soft-closing a DML
        # cursor. Release buffered rows, but retain execution metadata.
        self._rows = []
        self._position = 0

    def setinputsizes(self, sizes):
        pass

    def setoutputsize(self, size, column=None):
        pass
