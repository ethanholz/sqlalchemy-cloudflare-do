"""SQLite compilation and reflection for the Durable Objects SQL API."""

from sqlalchemy import exc
from sqlalchemy.dialects.sqlite.base import SQLiteDialect
from sqlalchemy.pool import NullPool

from . import dbapi as storage_dbapi


class DurableObjectDialect(SQLiteDialect):
    name = "cloudflare_do"
    driver = "storage"
    supports_statement_cache = True
    # Cloudflare limits each statement to 100 bound parameters.
    insertmanyvalues_max_parameters = 100

    def __init__(self, dbapi=None, isolation_level="AUTOCOMMIT", **kwargs):
        if isolation_level not in (None, "AUTOCOMMIT"):
            raise exc.ArgumentError("Durable Objects only support AUTOCOMMIT isolation")
        # Skip SQLiteDialect's local sqlite3 version checks; this DB-API
        # speaks to a storage binding rather than a local SQLite library.
        super().__init__(dbapi=None, isolation_level="AUTOCOMMIT", **kwargs)
        self.dbapi = dbapi

    @classmethod
    def import_dbapi(cls):
        return storage_dbapi

    @classmethod
    def get_pool_class(cls, url):
        return NullPool

    def create_connect_args(self, url):
        if any(
            (url.host, url.port, url.username, url.password, url.database, url.query)
        ):
            raise exc.ArgumentError(
                "Use cloudflare_do:// with connect_args={'storage': ctx.storage}"
            )
        return [], {}

    def _get_server_version_info(self, connection):
        # workerd forbids sqlite_version(). This is a feature baseline for
        # SQLAlchemy reflection/RETURNING, NOT the actual engine version.
        return (3, 35, 0)

    def get_schema_names(self, connection, **kw):
        # database_list and the temp database are blocked by workerd.
        return ["main"]

    def _get_table_pragma(self, connection, pragma, table_name, schema=None):
        if schema not in (None, "main") or table_name.startswith("__cf_"):
            return []
        # The SQLite implementation tries temp if main returns no rows.
        return super()._get_table_pragma(connection, pragma, table_name, schema="main")

    def get_temp_table_names(self, connection, **kw):
        return []

    def get_temp_view_names(self, connection, **kw):
        return []

    def get_isolation_level(self, dbapi_connection):
        return "AUTOCOMMIT"

    def get_isolation_level_values(self, dbapi_connection):
        return ["AUTOCOMMIT"]

    def set_isolation_level(self, dbapi_connection, level):
        if level != "AUTOCOMMIT":
            raise storage_dbapi.NotSupportedError(
                "Only AUTOCOMMIT isolation is supported"
            )

    def do_savepoint(self, connection, name):
        raise storage_dbapi.NotSupportedError(
            "SQL savepoints are unavailable; use run_in_transaction()"
        )

    def do_rollback_to_savepoint(self, connection, name):
        self.do_savepoint(connection, name)

    def do_release_savepoint(self, connection, name):
        self.do_savepoint(connection, name)

    def get_table_names(self, connection, schema=None, **kw):
        # Cloudflare's hidden KV table is visible in sqlite_master, but cannot
        # be read, reflected or modified through the SQL API.
        return [
            name
            for name in super().get_table_names(connection, schema=schema, **kw)
            if not name.startswith("__cf_")
        ]

    def is_disconnect(self, e, connection, cursor):
        return isinstance(e, storage_dbapi.InterfaceError) and (
            connection is not None and connection.closed
        )
