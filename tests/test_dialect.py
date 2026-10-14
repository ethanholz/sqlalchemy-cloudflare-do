"""Host-side checks using an API-shaped SQLite storage double, not a driver."""

import sqlite3
from datetime import date, datetime, time
from decimal import Decimal
from types import SimpleNamespace

import pytest
from sqlalchemy import (
    JSON,
    Boolean,
    Column,
    Date,
    DateTime,
    Integer,
    LargeBinary,
    MetaData,
    Numeric,
    String,
    Table,
    Time,
    create_engine,
    inspect,
    select,
    text,
)
from sqlalchemy.exc import ArgumentError, IntegrityError, ProgrammingError
from sqlalchemy.orm import DeclarativeBase, Session, mapped_column

from sqlalchemy_cloudflare_do import (
    create_engine_from_storage,
    dbapi,
    run_in_transaction,
)


class Storage:
    def __init__(self):
        self.database = sqlite3.connect(":memory:", isolation_level=None)
        self.database.setlimit(sqlite3.SQLITE_LIMIT_VARIABLE_NUMBER, 100)
        self.sql = SimpleNamespace(exec=self.execute)
        self.statements = []
        self.transactions = 0
        self.depth = 0

    def execute(self, statement, *bindings):
        self.statements.append((statement, bindings))
        if any(
            blocked in statement.lower()
            for blocked in ("sqlite_version(", "database_list", "temp.")
        ):
            raise RuntimeError("not authorized: SQLITE_AUTH")
        if statement.split()[0].upper() in {
            "BEGIN",
            "COMMIT",
            "ROLLBACK",
            "SAVEPOINT",
            "RELEASE",
        }:
            raise RuntimeError("Transaction statements are not supported")
        before = self.database.total_changes
        cursor = self.database.execute(statement, bindings)
        columns = [column[0] for column in cursor.description or ()]
        rows = cursor.fetchall()
        return SimpleNamespace(
            columnNames=columns,
            raw=lambda: SimpleNamespace(toArray=lambda: rows),
            # Simulate index billing so changes() must be used for rowcount.
            rowsWritten=(self.database.total_changes - before) * 2,
        )

    def transactionSync(self, callback):
        self.transactions += 1
        self.depth += 1
        name = f"transaction_{self.depth}"
        self.database.execute(f"SAVEPOINT {name}")
        try:
            result = callback()
        except BaseException:
            self.database.execute(f"ROLLBACK TO {name}")
            raise
        finally:
            self.database.execute(f"RELEASE {name}")
            self.depth -= 1
        return result


@pytest.fixture
def storage():
    value = Storage()
    yield value
    value.database.close()


@pytest.fixture
def engine(storage):
    value = create_engine_from_storage(storage)
    yield value
    value.dispose()


def test_core_types_reflection_and_metadata(engine):
    metadata = MetaData()
    table = Table(
        "values_table",
        metadata,
        Column("id", Integer, primary_key=True),
        Column("name", String, unique=True),
        Column("active", Boolean),
        Column("day", Date),
        Column("clock", Time),
        Column("stamp", DateTime),
        Column("payload", JSON),
        Column("blob", LargeBinary),
        Column("amount", Numeric(10, 2)),
    )
    metadata.create_all(engine)
    values = {
        "name": "Alice's ?; -- name",
        "active": True,
        "day": date(2025, 1, 2),
        "clock": time(3, 4, 5),
        "stamp": datetime(2025, 1, 2, 3, 4, 5),
        "payload": {"tags": ["python", None]},
        "blob": b"\x00\xff",
        "amount": Decimal("12.34"),
    }
    with engine.connect() as connection:
        inserted = connection.execute(table.insert().values(**values))
        assert inserted.inserted_primary_key == (1,)
        assert connection.execute(select(table)).one()._mapping == {"id": 1, **values}
        assert (
            connection.execute(
                table.update().where(table.c.id == 1).values(active=False)
            ).rowcount
            == 1
        )
        assert (
            connection.execute(
                table.update().where(table.c.id == 9).values(active=False)
            ).rowcount
            == 0
        )
        empty = connection.execute(select(table).where(table.c.id == 9))
        assert list(empty.keys()) == list(table.c.keys())
        assert empty.all() == []
        duplicate = connection.exec_driver_sql("SELECT 1 AS value, 2 AS value")
        assert duplicate.one() == (1, 2)
        assert connection.get_isolation_level() == "AUTOCOMMIT"
    assert inspect(engine).get_table_names() == ["values_table"]
    reflected = Table("values_table", MetaData(), autoload_with=engine)
    assert set(reflected.c.keys()) == set(table.c.keys())
    assert inspect(engine).get_unique_constraints("values_table")[0][
        "column_names"
    ] == ["name"]


def test_orm_flush_update_delete(engine):
    class Base(DeclarativeBase):
        pass

    class User(Base):
        __tablename__ = "users"
        id = mapped_column(Integer, primary_key=True)
        name = mapped_column(String, nullable=False, unique=True)

    Base.metadata.create_all(engine)
    with Session(engine) as session:
        user = User(name="Alice")
        session.add(user)
        session.commit()
        assert user.id == 1
        user.name = "Bob"
        session.commit()
        assert session.scalars(select(User.name)).all() == ["Bob"]
        session.delete(user)
        session.commit()
        assert session.scalars(select(User)).all() == []


def test_transactions_rollback_exceptions_and_return_values(engine, storage):
    with engine.connect() as connection:
        connection.exec_driver_sql(
            "CREATE TABLE users(id INTEGER PRIMARY KEY, name TEXT UNIQUE)"
        )
    assert (
        run_in_transaction(
            engine,
            lambda c: c.exec_driver_sql(
                "INSERT INTO users(name) VALUES (?) RETURNING id", ("Alice",)
            ).scalar_one(),
        )
        == 1
    )

    def fail(connection):
        connection.exec_driver_sql("INSERT INTO users(name) VALUES (?)", ("Bob",))
        connection.exec_driver_sql("INSERT INTO users(name) VALUES (?)", ("Alice",))

    with pytest.raises(IntegrityError):
        run_in_transaction(engine, fail)
    with engine.connect() as connection:
        assert connection.exec_driver_sql("SELECT name FROM users").all() == [
            ("Alice",)
        ]

    error = ValueError("abort")

    def abort(connection):
        connection.exec_driver_sql("INSERT INTO users(name) VALUES ('Charlie')")
        raise error

    with pytest.raises(ValueError) as raised:
        run_in_transaction(engine, abort)
    assert raised.value is error
    with engine.connect() as connection:
        assert (
            connection.exec_driver_sql("SELECT count(*) FROM users").scalar_one() == 1
        )
    assert storage.transactions == 3


def test_async_callbacks_are_rejected_and_rolled_back(engine):
    with engine.connect() as connection:
        connection.exec_driver_sql("CREATE TABLE t(n INTEGER)")

    async def future():
        return 1

    def callback(connection):
        connection.exec_driver_sql("INSERT INTO t VALUES (1)")
        return future()

    with pytest.raises(TypeError, match="awaitables"):
        run_in_transaction(engine, callback)
    with engine.connect() as connection:
        assert connection.exec_driver_sql("SELECT count(*) FROM t").scalar_one() == 0


def test_executemany_atomicity_and_counts(engine, storage):
    with engine.connect() as connection:
        connection.exec_driver_sql("CREATE TABLE t(n INTEGER UNIQUE)")
        assert (
            connection.exec_driver_sql(
                "INSERT INTO t VALUES (?)", [(1,), (2,)]
            ).rowcount
            == 2
        )
        assert (
            connection.exec_driver_sql(
                "UPDATE t SET n=n WHERE n=?", [(1,), (999,)]
            ).rowcount
            == 1
        )
        with pytest.raises(IntegrityError):
            connection.exec_driver_sql("INSERT INTO t VALUES (?)", [(3,), (1,)])
        assert connection.exec_driver_sql("SELECT n FROM t ORDER BY n").all() == [
            (1,),
            (2,),
        ]
    assert storage.transactions == 3


def test_core_insert_rowcount_survives_soft_close(engine):
    table = Table("counts", MetaData(), Column("n", Integer))
    table.create(engine)
    with engine.connect() as connection:
        assert connection.execute(table.insert(), [{"n": 1}, {"n": 2}]).rowcount == 2
        assert connection.execute(table.insert().values(n=3)).rowcount == 1


def test_bulk_returning_obeys_platform_parameter_limit(engine, storage):
    table = Table(
        "bulk",
        MetaData(),
        Column("id", Integer, primary_key=True),
        Column("name", String),
    )
    table.create(engine)
    with engine.connect() as connection:
        result = connection.execute(
            table.insert().returning(table.c.id), [{"name": str(i)} for i in range(201)]
        )
        assert result.scalars().all() == list(range(1, 202))
    assert all(len(bindings) <= 100 for _, bindings in storage.statements)


def test_autocommit_is_explicit_and_savepoints_fail(engine, storage):
    assert engine.dialect._on_connect_isolation_level == "AUTOCOMMIT"
    with pytest.raises(ArgumentError, match="AUTOCOMMIT"):
        create_engine_from_storage(storage, isolation_level="SERIALIZABLE")
    with engine.connect() as connection:
        with pytest.raises(ArgumentError):
            connection.execution_options(isolation_level="READ UNCOMMITTED")
        with pytest.raises(dbapi.NotSupportedError, match="savepoints"):
            connection.begin_nested()
        connection.exec_driver_sql("CREATE TABLE t(n INTEGER)")
        connection.exec_driver_sql("INSERT INTO t VALUES (1)")
        connection.rollback()
        assert connection.exec_driver_sql("SELECT n FROM t").scalar_one() == 1


def test_url_registration_and_hidden_tables(storage):
    storage.execute("CREATE TABLE __cf_kv(key TEXT)")
    engine = create_engine("cloudflare_do://", connect_args={"storage": storage})
    assert inspect(engine).get_table_names() == []
    assert inspect(engine).get_schema_names() == ["main"]
    assert inspect(engine).get_temp_table_names() == []
    assert inspect(engine).has_table("__cf_kv") is False
    engine.dispose()
    with pytest.raises(ArgumentError, match="cloudflare_do://"):
        create_engine("cloudflare_do://account:token@database")
    with pytest.raises(dbapi.InterfaceError, match="ctx.storage"):
        dbapi.connect(storage=object())


def test_dbapi_cursor_lifecycle(storage):
    connection = dbapi.connect(storage=storage)
    cursor = connection.cursor()
    cursor.execute("SELECT 1 UNION ALL SELECT 2 UNION ALL SELECT 3")
    assert cursor.fetchone() == (1,)
    assert cursor.fetchmany(1) == [(2,)]
    assert cursor.fetchall() == [(3,)]
    assert cursor.fetchone() is None
    cursor.execute("SELECT 1 WHERE 0")
    assert cursor.description[0][0] == "1"
    assert cursor.fetchall() == []
    with pytest.raises(dbapi.ProgrammingError):
        cursor.execute("SELECT ?", {"x": 1})
    assert cursor.description is None
    cursor.execute("CREATE TABLE t(n INTEGER)")
    with pytest.raises(dbapi.ProgrammingError):
        cursor.fetchall()
    with pytest.raises(dbapi.ProgrammingError):
        cursor.fetchmany(0)
    cursor.executemany("INSERT INTO t VALUES (?)", [])
    assert cursor.rowcount == 0
    assert cursor.lastrowid is None
    with pytest.raises(dbapi.NotSupportedError):
        cursor.executemany("INSERT INTO t VALUES (?) RETURNING n", [(1,)])
    assert storage.database.execute("SELECT count(*) FROM t").fetchone() == (0,)
    cursor.close()
    with pytest.raises(dbapi.InterfaceError):
        cursor.execute("SELECT 1")
    connection.close()
    with pytest.raises(dbapi.InterfaceError):
        connection.cursor()
    assert storage.database.execute("SELECT 1").fetchone() == (1,)


@pytest.mark.parametrize("value", [2**53, -(2**53), float("nan"), float("inf")])
def test_unsafe_numbers_are_rejected(storage, value):
    with pytest.raises(dbapi.DataError):
        dbapi.connect(storage=storage).cursor().execute("SELECT ?", (value,))


@pytest.mark.parametrize("value", [object(), {}, datetime(2025, 1, 1)])
def test_unsupported_bindings_are_rejected(storage, value):
    with pytest.raises(dbapi.ProgrammingError, match="Unsupported"):
        dbapi.connect(storage=storage).cursor().execute("SELECT ?", (value,))


def test_invalid_sql_maps_errors(engine):
    with engine.connect() as connection:
        with pytest.raises(ProgrammingError) as raised:
            connection.execute(text("SELECT * FROM absent"))
        assert isinstance(raised.value.orig, dbapi.ProgrammingError)
