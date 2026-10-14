# SQLAlchemy for Cloudflare Durable Objects

A synchronous SQLAlchemy 2.0 dialect for **SQLite-backed Durable Objects in
Python Workers**. It calls `ctx.storage.sql.exec()` directly: no HTTP bridge,
Cloudflare API token, local SQLite driver, or greenlet is needed.

Storage is private to an object. A Worker namespace binding or Durable Object
stub is **not** a database connection; run SQLAlchemy inside the object itself.
Legacy KV-backed objects do not support the SQL API.

## Usage

Add this package to your Worker's `pyproject.toml` dependencies. For an unpublished
checkout, use a local source as in [`examples/worker/pyproject.toml`](examples/worker/pyproject.toml).
Python 3.13+ and SQLAlchemy 2.0 are required.

```python
from sqlalchemy import text
from sqlalchemy_cloudflare_do import create_engine_from_storage, run_in_transaction
from workers import DurableObject, Response

class Database(DurableObject):
    def __init__(self, ctx, env):
        super().__init__(ctx, env)
        self.engine = create_engine_from_storage(ctx.storage)

    async def fetch(self, request):
        def increment(connection):
            connection.execute(text(
                "CREATE TABLE IF NOT EXISTS counter(id INTEGER PRIMARY KEY, value INTEGER)"
            ))
            return connection.execute(text(
                "INSERT INTO counter VALUES (1, 1) "
                "ON CONFLICT(id) DO UPDATE SET value = value + 1 RETURNING value"
            )).scalar_one()

        value = run_in_transaction(self.engine, increment)
        return Response.json({"count": value})
```

Configure the class with a SQLite migration:

```toml
compatibility_flags = ["python_workers"]

[[durable_objects.bindings]]
name = "DATABASE"
class_name = "Database"

[[migrations]]
tag = "v1"
new_sqlite_classes = ["Database"]
```

The dialect is also registered as `cloudflare_do`:

```python
import sqlalchemy_cloudflare_do  # Registers even without entry-point metadata.
from sqlalchemy import create_engine

engine = create_engine("cloudflare_do://", connect_args={"storage": ctx.storage})
```

Engines default to `NullPool` and `AUTOCOMMIT`. Keep an engine on its owning
Durable Object; never share storage across objects or threads. Closing a
connection or disposing an engine does not close or delete object storage.

## Transactions: important

Cloudflare forbids SQL `BEGIN`, `COMMIT`, `ROLLBACK` and `SAVEPOINT`. SQLAlchemy
connections therefore use **AUTOCOMMIT only**:

- `connection.commit()` / `rollback()` and `Session.commit()` / `rollback()` do
  not control storage transactions. ORM commit still flushes pending changes.
- `engine.begin()` and `Session.begin()` do **not** make multiple statements
  atomic or undo them on failure. SQLAlchemy's logical transaction bookkeeping
  still exists, but there is no matching DB-API transaction.
- Use `run_in_transaction(engine, callback)` for atomic work. It invokes the
  callback inside **`storage.transactionSync()`**, rolling back when an exception
  escapes. Do not catch and suppress failures inside the callback.
- Callbacks must complete synchronously: no `await`, async I/O or returned
  awaitables. Returned awaitables are rejected and writes are rolled back.
- DB-API `executemany()` is atomic via `transactionSync()`. SQLAlchemy can also
  split bulk `RETURNING` inserts into several statements; wrap the entire
  operation in `run_in_transaction()` for all-or-nothing semantics.
- Savepoints / `begin_nested()` and other isolation levels are unsupported.

ORM work can bind a session to the callback's connection:

```python
from sqlalchemy.orm import Session

# User is your usual SQLAlchemy mapped class.
def add_user(connection):
    with Session(bind=connection) as session:
        user = User(name="Alice")
        session.add(user)
        session.flush()
        return user.id

user_id = run_in_transaction(engine, add_user)
```

## Supported behavior and limits

SQLite compilation and type conversion are reused from SQLAlchemy, including
Core, ORM, schema creation/reflection, JSON, dates/times, booleans, BLOBs,
`RETURNING`, and SQLite upserts. BLOB bindings become JavaScript `ArrayBuffer`s;
results become Python bytes. Empty result sets retain their column names and
positional rows preserve duplicate column names. Reflection hides Cloudflare's
reserved `__cf_*` tables.

Only the `main` schema is supported, not temporary tables or attached databases.
Cloudflare blocks `sqlite_version()` and `PRAGMA database_list`; the dialect
uses a conservative SQLite 3.35 feature baseline, not a reported server version.

Native cursors are fully consumed synchronously before returning. Results are
**buffered in memory**, not streamed; limit large queries. Rowcount comes from
SQLite `changes()`, not Cloudflare's billing-oriented `rowsWritten` (which also
counts index writes). Rowcount for read-only / returning results may be `-1`.

SQLAlchemy's generated bulk insert pages respect Cloudflare's **100 bound
parameters per statement**. Explicit oversized SQL statements are still subject
to platform limits. Bind integers only within JavaScript's safe range
(`-(2**53 - 1)` through `2**53 - 1`); larger integers and nonfinite floats are
rejected rather than silently losing precision. Large integers already in
storage may lose precision on retrieval; store exact large values as text.

There is no `create_async_engine()` adapter: the storage SQL API is synchronous
and can be called normally inside an async Worker handler. No `sqlite3` module
is imported by this package. Custom Python SQLite functions and cross-object
transactions are not supported.

## Run checks

Host-side tests use an API-shaped SQLite storage double; they do not establish
Workers FFI compatibility on their own:

```sh
uv sync
uv run pytest -q
uv run ruff check src tests examples/worker/src/entry.py
uv run ruff format --check src tests examples/worker/src/entry.py
uv build
```

The example also has a real-runtime `/check` smoke test covering ORM generated
keys, types/BLOBs/nulls, empty/duplicate columns, reflection, affected rowcounts,
callback exception rollback, atomic batch constraint failures, nested batches,
and rollback across bulk `RETURNING` pages:

```sh
cd examples/worker
uv run pywrangler dev
# In another terminal:
curl --fail http://localhost:8787/check  # {"ok": true, ...}
curl --fail http://localhost:8787/       # Persistent counter
```

The `/check` route resets its own `dialect_check_users` table. It does not touch
the counter. This is a development example, not a public SQL endpoint.
After editing the dialect source, refresh the example's bundled local dependency
with `uv run pywrangler sync --force` before restarting the dev server.

## Cloudflare references

- [SQLite-backed storage API](https://developers.cloudflare.com/durable-objects/api/sqlite-storage-api/):
  `sql.exec()`, cursor metadata, binary values, transactions and numeric precision.
- [Durable Objects in Python](https://developers.cloudflare.com/durable-objects/get-started/)
  and [SQLite storage configuration](https://developers.cloudflare.com/durable-objects/best-practices/access-durable-objects-storage/).
- [Python Workers FFI](https://developers.cloudflare.com/workers/languages/python/ffi/)
  and [package management](https://developers.cloudflare.com/workers/languages/python/packages/).
- [Durable Objects limits](https://developers.cloudflare.com/durable-objects/platform/limits/).
- [workerd's SQL function/pragma allowlists](https://github.com/cloudflare/workerd/blob/main/src/workerd/util/sqlite.c++).

Inspired by [sqlalchemy-cloudflare-d1](https://github.com/collierking/sqlalchemy-cloudflare-d1),
but implemented for the synchronous Durable Objects storage API rather than D1.
