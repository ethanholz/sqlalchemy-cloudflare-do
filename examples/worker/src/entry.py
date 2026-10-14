"""Counter demo and /check smoke test, executed inside a real Python Worker."""

from datetime import datetime
from urllib.parse import urlparse

from sqlalchemy import (
    JSON,
    Boolean,
    DateTime,
    Integer,
    LargeBinary,
    String,
    inspect,
    select,
)
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import DeclarativeBase, Session, mapped_column
from workers import DurableObject, Response, WorkerEntrypoint

from sqlalchemy_cloudflare_do import create_engine_from_storage, run_in_transaction


class Base(DeclarativeBase):
    pass


class User(Base):
    __tablename__ = "dialect_check_users"
    id = mapped_column(Integer, primary_key=True)
    name = mapped_column(String, unique=True)
    active = mapped_column(Boolean)
    stamp = mapped_column(DateTime)
    payload = mapped_column(JSON)
    blob = mapped_column(LargeBinary)


class Database(DurableObject):
    def __init__(self, ctx, env):
        super().__init__(ctx, env)
        self.engine = create_engine_from_storage(ctx.storage)

    async def fetch(self, request):
        if urlparse(request.url).path == "/check":
            return Response.json(self.check())

        def increment(connection):
            connection.exec_driver_sql(
                "CREATE TABLE IF NOT EXISTS counter(id INTEGER PRIMARY KEY, value INTEGER)"
            )
            return connection.exec_driver_sql(
                "INSERT INTO counter VALUES (1, 1) "
                "ON CONFLICT(id) DO UPDATE SET value = value + 1 RETURNING value"
            ).scalar_one()

        value = run_in_transaction(self.engine, increment)
        return Response.json({"count": value})

    def check(self):
        # This route resets ONLY the smoke-test table, not the counter.
        Base.metadata.drop_all(self.engine)
        Base.metadata.create_all(self.engine)
        stamp = datetime(2025, 1, 2, 3, 4, 5)

        def insert(connection):
            with Session(bind=connection) as session:
                user = User(
                    name="Alice's ?; -- name",
                    active=True,
                    stamp=stamp,
                    payload={"tags": ["python", None]},
                    blob=b"\x00\xff",
                )
                session.add(user)
                session.flush()
                return user.id

        assert run_in_transaction(self.engine, insert) == 1
        with Session(self.engine) as session:
            user = session.get(User, 1)
            assert user.active is True and user.stamp == stamp
            assert user.blob == b"\x00\xff" and user.payload == {
                "tags": ["python", None]
            }

        def abort(connection):
            connection.execute(User.__table__.insert().values(name="rolled back"))
            raise ValueError("abort")

        try:
            run_in_transaction(self.engine, abort)
        except ValueError as error:
            assert str(error) == "abort"
        else:
            raise AssertionError("callback exceptions must escape")

        with self.engine.connect() as connection:
            assert connection.execute(select(User.name)).scalars().all() == [
                "Alice's ?; -- name"
            ]
            empty = connection.execute(select(User).where(User.id == -1))
            assert "name" in list(empty.keys()) and empty.all() == []
            assert connection.exec_driver_sql("SELECT 1 AS v, 2 AS v").one() == (1, 2)
            assert (
                connection.execute(
                    User.__table__.update().where(User.id == 1).values(active=False)
                ).rowcount
                == 1
            )
            try:
                connection.execute(
                    User.__table__.insert(),
                    [{"name": "new"}, {"name": "Alice's ?; -- name"}],
                )
            except IntegrityError:
                pass
            else:
                raise AssertionError("unique constraints must raise IntegrityError")
            assert connection.execute(select(User.id)).scalars().all() == [1]
            connection.execute(
                User.__table__.insert().values(
                    name="nulls",
                    active=None,
                    stamp=None,
                    payload=None,
                    blob=None,
                )
            )
            row = connection.execute(select(User).where(User.name == "nulls")).one()
            assert row.active is None and row.stamp is None and row.blob is None
            connection.execute(
                User.__table__.insert().values(name="empty-blob", blob=b"")
            )
            assert (
                connection.execute(
                    select(User.blob).where(User.name == "empty-blob")
                ).scalar_one()
                == b""
            )

        # Nested transactionSync (executemany inside the outer callback).
        def nested_batch(connection):
            return connection.execute(
                User.__table__.insert(), [{"name": "nested-1"}, {"name": "nested-2"}]
            ).rowcount

        nested_count = run_in_transaction(self.engine, nested_batch)
        assert nested_count == 2, f"executemany rowcount: {nested_count}"

        def bulk(connection):
            return (
                connection.execute(
                    User.__table__.insert().returning(User.id),
                    [{"name": f"bulk-{i}"} for i in range(201)],
                )
                .scalars()
                .all()
            )

        assert len(set(run_in_transaction(self.engine, bulk))) == 201

        def failing_bulk(connection):
            connection.execute(
                User.__table__.insert().returning(User.id),
                [{"name": f"rollback-{i}"} for i in range(100)] + [{"name": "nulls"}],
            )

        try:
            run_in_transaction(self.engine, failing_bulk)
        except IntegrityError:
            pass
        else:
            raise AssertionError("bulk RETURNING failures must roll back earlier pages")
        with self.engine.connect() as connection:
            assert (
                connection.execute(
                    select(User.id).where(User.name.like("rollback-%"))
                ).all()
                == []
            )

        assert "dialect_check_users" in inspect(self.engine).get_table_names()
        assert any(
            c["name"] == "blob"
            for c in inspect(self.engine).get_columns("dialect_check_users")
        )
        return {"ok": True}


class Default(WorkerEntrypoint):
    async def fetch(self, request):
        # A namespace/stub routes requests; only Database owns its storage.
        identifier = self.env.DATABASE.idFromName("example")
        return await self.env.DATABASE.get(identifier).fetch(request)
