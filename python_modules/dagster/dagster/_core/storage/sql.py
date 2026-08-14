import dataclasses
import threading
import time
from contextlib import AbstractContextManager
from dataclasses import dataclass
from functools import lru_cache
from typing import Any, ClassVar, TypeAlias

import sqlalchemy as db
from alembic.command import downgrade, stamp, upgrade
from alembic.config import Config
from alembic.runtime.environment import EnvironmentContext
from alembic.runtime.migration import MigrationContext
from alembic.script import ScriptDirectory
from sqlalchemy.engine import URL, Connection
from sqlalchemy.ext.compiler import compiles

from dagster._serdes import ConfigurableClass, ConfigurableClassData
from dagster._utils import file_relative_path

create_engine = db.create_engine  # exported


ALEMBIC_SCRIPTS_LOCATION = "dagster:_core/storage/alembic"

# Stand-in for a typed query object, which is only available in sqlalchemy 2+
SqlAlchemyQuery: TypeAlias = Any

# Stand-in for a typed row object, which is only available in sqlalchemy 2+
SqlAlchemyRow: TypeAlias = Any

AlembicVersion: TypeAlias = tuple[str | None, str | tuple[str, ...] | None]

ConnectionContextManager: TypeAlias = AbstractContextManager[Connection]


@lru_cache(maxsize=3)  # run, event, and schedule storages
def get_alembic_config(
    dunder_file: str,
    config_path: str = "alembic/alembic.ini",
    script_location: str | None = None,
) -> Config:
    if not script_location:
        script_location = ALEMBIC_SCRIPTS_LOCATION

    alembic_config = Config(file_relative_path(dunder_file, config_path))
    alembic_config.set_main_option("script_location", script_location)
    return alembic_config


def run_alembic_upgrade(
    alembic_config: Config, conn: Connection, run_id: str | None = None, rev: str = "head"
) -> None:
    alembic_config.attributes["connection"] = conn
    alembic_config.attributes["run_id"] = run_id
    upgrade(alembic_config, rev)
    with global_cache.lock:
        Cache.global_alembic_counter += 1


def run_alembic_downgrade(
    alembic_config: Config, conn: Connection, rev: str, run_id: str | None = None
) -> None:
    alembic_config.attributes["connection"] = conn
    alembic_config.attributes["run_id"] = run_id
    downgrade(alembic_config, rev)
    with global_cache.lock:
        Cache.global_alembic_counter += 1


# Ensure that at most one thread can be stamping alembic revisions at once
_alembic_lock = threading.Lock()


@dataclass(kw_only=True)
class CacheData:
    key: tuple[str, URL, ConfigurableClassData | None] | None
    monotonic_timestamp: float = dataclasses.field(default_factory=time.monotonic)
    alembic_counter: int = dataclasses.field(
        default_factory=lambda: int(Cache.global_alembic_counter)
    )
    table_names: list[str] | None = None
    columns: dict[str, list[str]] = dataclasses.field(default_factory=dict)
    indexes: dict[str, list[str]] = dataclasses.field(default_factory=dict)
    _lock: threading.Lock = dataclasses.field(default_factory=threading.Lock)

    def get_table_names(self, connect: ConnectionContextManager) -> list[str]:
        if self.table_names:
            return self.table_names
        with connect as conn:
            del connect
            table_names = db.inspect(conn).get_table_names()
        if self.key is None:
            return table_names
        with self._lock:
            if not self.table_names:
                self.table_names = table_names
        return self.table_names

    def has_table(self, table_name: str, connect: ConnectionContextManager) -> bool:
        if self.key is None:
            with connect as conn:
                del connect
                return db.inspect(conn).has_table(table_name)
        return table_name in self.get_table_names(connect)

    def get_columns(self, table_name: str, connect: ConnectionContextManager) -> list[str]:
        if table_name in self.columns:
            return self.columns[table_name]
        with connect as conn:
            del connect
            columns = [x.get("name") for x in db.inspect(conn).get_columns(table_name)]
        if self.key is None:
            return columns
        with self._lock:
            return self.columns.setdefault(table_name, columns)

    def get_indexes(self, table_name: str, connect: ConnectionContextManager) -> list[str]:
        if table_name in self.indexes:
            return self.indexes[table_name]
        with connect as conn:
            del connect
            indexes = [
                name for x in db.inspect(conn).get_indexes(table_name) if (name := x.get("name"))
            ]
        if self.key is None:
            return indexes
        with self._lock:
            return self.indexes.setdefault(table_name, indexes)

    def has_column(
        self, *, table_name: str, column_name: str, connect: ConnectionContextManager
    ) -> bool:
        return column_name in self.get_columns(table_name, connect)

    def has_index(
        self, *, table_name: str, index_name: str, connect: ConnectionContextManager
    ) -> bool:
        if self.key is None:
            with connect as conn:
                del connect
                return db.inspect(conn).has_index(table_name=table_name, index_name=index_name)
        return index_name in self.get_indexes(table_name, connect)


class Cache:
    global_alembic_counter: ClassVar[int] = 0
    _cache: dict[tuple[str, URL, ConfigurableClassData | None], CacheData]
    _uncached: CacheData
    lock: threading.Lock

    def __init__(self) -> None:
        super().__init__()
        self._cache = {}
        self._uncached = CacheData(key=None)
        self.lock = threading.Lock()

    def get(self, url: URL, storage: ConfigurableClass | object) -> CacheData:
        if not isinstance(storage, ConfigurableClass):
            return self._uncached
        key = (storage.__class__.__name__, url, storage.inst_data)
        data = self._cache.get(key)
        if data is not None:
            if (
                data.alembic_counter != self.__class__.global_alembic_counter
                or time.monotonic() >= (data.monotonic_timestamp + 600)
            ):
                with self.lock:
                    self._cache.pop(key, None)
                data = None
        if data is None:
            data = CacheData(key=key)
            with self.lock:
                data = self._cache.setdefault(key, data)
        return data

    def get_table_names(
        self, url: URL, storage: ConfigurableClass | object, connect: ConnectionContextManager
    ) -> list[str]:
        return self.get(url, storage).get_table_names(connect)

    def has_table(
        self,
        table_name: str,
        url: URL,
        storage: ConfigurableClass | object,
        connect: ConnectionContextManager,
    ) -> bool:
        return self.get(url, storage).has_table(table_name, connect)

    def get_columns(
        self,
        table_name: str,
        url: URL,
        storage: ConfigurableClass | object,
        connect: ConnectionContextManager,
    ) -> list[str]:
        return self.get(url, storage).get_columns(table_name, connect)

    def get_indexes(
        self,
        table_name: str,
        url: URL,
        storage: ConfigurableClass | object,
        connect: ConnectionContextManager,
    ) -> list[str]:
        return self.get(url, storage).get_indexes(table_name, connect)

    def has_column(
        self,
        *,
        table_name: str,
        column_name: str,
        url: URL,
        storage: ConfigurableClass | object,
        connect: ConnectionContextManager,
    ) -> bool:
        return self.get(url, storage).has_column(
            table_name=table_name,
            column_name=column_name,
            connect=connect,
        )

    def has_index(
        self,
        *,
        table_name: str,
        index_name: str,
        url: URL,
        storage: ConfigurableClass | object,
        connect: ConnectionContextManager,
    ) -> bool:
        return self.get(url, storage).has_index(
            table_name=table_name, index_name=index_name, connect=connect
        )


global_cache = Cache()


get_table_names = global_cache.get_table_names
has_table = global_cache.has_table
get_columns = global_cache.get_columns
get_indexes = global_cache.get_indexes
has_column = global_cache.has_column
has_index = global_cache.has_index


def stamp_alembic_rev(alembic_config: Config, conn: Connection, rev: str = "head") -> None:
    with _alembic_lock:
        alembic_config.attributes["connection"] = conn
        stamp(alembic_config, rev)
    with global_cache.lock:
        Cache.global_alembic_counter += 1


def check_alembic_revision(alembic_config: Config, conn: Connection) -> AlembicVersion:
    with _alembic_lock:
        migration_context = MigrationContext.configure(conn)
        db_revision = migration_context.get_current_revision()
        script = ScriptDirectory.from_config(alembic_config)
        head_revision = script.as_revision_number("head")

    return (db_revision, head_revision)


def safe_commit(conn: Connection) -> None:
    """Commits to a connection if it is in a transaction. Supports compatibility across SQLAlchemy versions,
    since older versions (1.3) have autocommit on transactions, instead of explicit commits.
    """
    if not conn.in_transaction():
        return
    if hasattr(conn, "commit"):
        conn.commit()  # type: ignore


def run_migrations_offline(
    context: EnvironmentContext, config: Config, target_metadata: db.MetaData
) -> None:
    """Run migrations in 'offline' mode.

    This configures the context with just a URL
    and not an Engine, though an Engine is acceptable
    here as well.  By skipping the Engine creation
    we don't even need a DBAPI to be available.

    Calls to context.execute() here emit the given string to the
    script output.

    """
    from sqlite3 import DatabaseError

    connectable = config.attributes.get("connection", None)

    if connectable is None:
        raise Exception(
            "No connection set in alembic config. If you are trying to run this script from the "
            "command line, STOP and read the README."
        )

    try:
        context.configure(
            url=connectable.url,
            target_metadata=target_metadata,
            literal_binds=True,
            dialect_opts={"paramstyle": "named"},
        )

        with context.begin_transaction():
            context.run_migrations()
    except DatabaseError as exc:
        # This is to deal with concurrent execution -- if this table already exists thanks to a
        # race with another process, we are fine and can continue.
        if "table alembic_version already exists" not in str(exc):
            raise


def run_migrations_online(
    context: EnvironmentContext, config: Config, target_metadata: db.MetaData
) -> None:
    """Run migrations in 'online' mode.

    In this scenario we need to create an Engine
    and associate a connection with the context.

    """
    from sqlite3 import DatabaseError

    connection = config.attributes.get("connection", None)

    if connection is None:
        raise Exception(
            "No connection set in alembic config. If you are trying to run this script from the "
            "command line, STOP and read the README."
        )

    try:
        context.configure(connection=connection, target_metadata=target_metadata)

        with context.begin_transaction():
            context.run_migrations()

    except DatabaseError as exc:
        # This is to deal with concurrent execution -- if this table already exists thanks to a
        # race with another process, we are fine and can continue.
        if "table alembic_version already exists" not in str(exc):
            raise


# SQLAlchemy types, compiler directives, etc. to avoid pre-0.11.0 migrations
# as well as compiler directives to make cross-DB API semantics the same.

# 1: make MySQL dates equivalent to PG or Sqlite dates

MYSQL_DATE_PRECISION: int = 6
MYSQL_FLOAT_PRECISION: int = 32


# datetime issue fix from here: https://stackoverflow.com/questions/29711102/sqlalchemy-mysql-millisecond-or-microsecond-precision/29723278
@compiles(db.DateTime, "mysql")
def compile_datetime_and_add_precision_mysql(_element, _compiler, **_kw) -> str:
    return f"DATETIME({MYSQL_DATE_PRECISION})"


class get_sql_current_timestamp(db.sql.expression.FunctionElement):
    """Like CURRENT_TIMESTAMP, but has the same semantics on MySQL, Postgres, and Sqlite."""

    type = db.types.DateTime()


@compiles(get_sql_current_timestamp, "mysql")
def compiles_get_sql_current_timestamp_mysql(_element, _compiler, **_kw) -> str:
    return f"CURRENT_TIMESTAMP({MYSQL_DATE_PRECISION})"


@compiles(get_sql_current_timestamp)
def compiles_get_sql_current_timestamp_default(_element, _compiler, **_kw) -> str:
    return "CURRENT_TIMESTAMP"


@compiles(db.types.TIMESTAMP, "mysql")
def add_precision_to_mysql_timestamps(_element, _compiler, **_kw) -> str:
    return f"TIMESTAMP({MYSQL_DATE_PRECISION})"


@compiles(db.types.Float, "mysql")
def add_precision_to_mysql_floats(_element, _compiler, **_kw) -> str:
    """Forces floats to have minimum precision of 32, which converts the underlying type to be a
    double.  This is necessary because the default precision of floats is too low for some types,
    including unix timestamps, resulting in truncated values in MySQL.
    """
    return f"FLOAT({MYSQL_FLOAT_PRECISION})"


@compiles(db.types.FLOAT, "mysql")
def add_precision_to_mysql_FLOAT(_element, _compiler, **_kw) -> str:
    """Forces floats to have minimum precision of 32, which converts the underlying type to be a
    double.  This is necessary because the default precision of floats is too low for some types,
    including unix timestamps, resulting in truncated values in MySQL.
    """
    return f"FLOAT({MYSQL_FLOAT_PRECISION})"


class LongText(db.Text):
    """Allows customization of certain fields to map to LONGTEXT in MySQL.  For Postgres, all text
    fields are mapped to TEXT, which is unbounded in length, so the distinction is not neccessary.
    In MySQL, however, TEXT is limited to 64KB, so LONGTEXT (4GB) is required for certain fields.
    """

    pass


@compiles(LongText, "mysql")
def compile_longtext_mysql(_element, _compiler, **_kw) -> str:
    return "LONGTEXT"


class MySQLCompatabilityTypes:
    UniqueText = db.String(512)
    LongText = LongText
