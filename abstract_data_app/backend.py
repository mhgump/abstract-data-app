"""
Data backend implementations.

All backends share the same abstract interface so that the framework can
duplicate every write across all configured backends transparently.

Thread safety contract:
- LocalSqliteDataBackend: thread-safe via per-thread connections + WAL mode.
- PostgresDataBackend:    thread-safe via per-thread connections.
- RedisDataBackend:       thread-safe; redis-py connection pool handles concurrency.
"""

import json
import threading
from abc import ABC, abstractmethod
from typing import Any, Optional


# ---------------------------------------------------------------------------
# Abstract base
# ---------------------------------------------------------------------------

class DataBackend(ABC):
    """Abstract base class that every backend must implement."""

    @abstractmethod
    def upsert(self, type_name: str, key: str, data: dict[str, Any]) -> None:
        """Insert or update an item."""

    @abstractmethod
    def delete(self, type_name: str, key: str) -> bool:
        """Delete an item. Returns True if the key existed."""

    @abstractmethod
    def get(self, type_name: str, key: str) -> Optional[dict[str, Any]]:
        """Return the item's data dict, or None if the key does not exist."""

    @abstractmethod
    def list_all(self, type_name: str) -> list[dict[str, Any]]:
        """Return all items as ``[{"key": ..., "data": {...}}, ...]``."""

    @abstractmethod
    def dry_run_upsert(
        self, type_name: str, key: str, data: dict[str, Any]
    ) -> Optional[str]:
        """
        Attempt an upsert without committing it.

        Returns an error string if the operation would fail, or None on success.
        Used by the validation tool to surface backend-level constraints.
        """


# ---------------------------------------------------------------------------
# SQLite
# ---------------------------------------------------------------------------

class LocalSqliteDataBackend(DataBackend):
    """
    SQLite-backed store.  Suitable for development and single-process deployments.

    Uses a single shared connection (``check_same_thread=False``) protected by
    a ``threading.RLock``.  This works correctly for both ``:memory:`` and
    file-based databases.  WAL mode is enabled for file databases to allow
    concurrent reads without blocking the writer.
    """

    def __init__(self, db_path: str = "abstract_data_app.db") -> None:
        """
        Args:
            db_path: Path to the SQLite database file.
                     Use ``":memory:"`` for an in-process ephemeral store.
        """
        import sqlite3
        self.db_path = db_path
        self._lock = threading.RLock()
        self._conn_obj = sqlite3.connect(db_path, check_same_thread=False)
        if db_path != ":memory:":
            self._conn_obj.execute("PRAGMA journal_mode=WAL")
        self._conn_obj.execute("""
            CREATE TABLE IF NOT EXISTS items (
                type_name TEXT NOT NULL,
                key       TEXT NOT NULL,
                data      TEXT NOT NULL,
                PRIMARY KEY (type_name, key)
            )
        """)
        self._conn_obj.commit()

    def _conn(self):
        return self._conn_obj

    def upsert(self, type_name: str, key: str, data: dict[str, Any]) -> None:
        with self._lock:
            self._conn_obj.execute(
                "INSERT OR REPLACE INTO items (type_name, key, data) VALUES (?, ?, ?)",
                (type_name, key, json.dumps(data)),
            )
            self._conn_obj.commit()

    def delete(self, type_name: str, key: str) -> bool:
        with self._lock:
            cur = self._conn_obj.execute(
                "DELETE FROM items WHERE type_name = ? AND key = ?",
                (type_name, key),
            )
            self._conn_obj.commit()
            return cur.rowcount > 0

    def get(self, type_name: str, key: str) -> Optional[dict[str, Any]]:
        with self._lock:
            cur = self._conn_obj.execute(
                "SELECT data FROM items WHERE type_name = ? AND key = ?",
                (type_name, key),
            )
            row = cur.fetchone()
        return json.loads(row[0]) if row else None

    def list_all(self, type_name: str) -> list[dict[str, Any]]:
        with self._lock:
            cur = self._conn_obj.execute(
                "SELECT key, data FROM items WHERE type_name = ?",
                (type_name,),
            )
            rows = cur.fetchall()
        return [{"key": row[0], "data": json.loads(row[1])} for row in rows]

    def dry_run_upsert(
        self, type_name: str, key: str, data: dict[str, Any]
    ) -> Optional[str]:
        conn = self._conn_obj
        with self._lock:
            try:
                conn.execute("SAVEPOINT _ada_dry_run")
                conn.execute(
                    "INSERT OR REPLACE INTO items (type_name, key, data) VALUES (?, ?, ?)",
                    (type_name, key, json.dumps(data)),
                )
                conn.execute("ROLLBACK TO SAVEPOINT _ada_dry_run")
                conn.execute("RELEASE SAVEPOINT _ada_dry_run")
                return None
            except Exception as exc:
                try:
                    conn.execute("ROLLBACK TO SAVEPOINT _ada_dry_run")
                    conn.execute("RELEASE SAVEPOINT _ada_dry_run")
                except Exception:
                    pass
                return str(exc)


# ---------------------------------------------------------------------------
# PostgreSQL
# ---------------------------------------------------------------------------

class PostgresDataBackend(DataBackend):
    """
    PostgreSQL-backed store via psycopg2.

    Install the optional dependency::

        pip install "abstract-data-app[postgres]"

    Uses per-thread connections to avoid sharing a single psycopg2 connection
    across threads (psycopg2 connections are not thread-safe).
    """

    TABLE = "abstract_data_app_items"

    def __init__(self, dsn: str) -> None:
        """
        Args:
            dsn: libpq connection string, e.g.
                 ``"postgresql://user:pass@localhost/mydb"``
        """
        self.dsn = dsn
        self._local = threading.local()
        self._ensure_tables()

    def _psycopg2(self):
        try:
            import psycopg2
            return psycopg2
        except ImportError:
            raise ImportError(
                "psycopg2 is required for PostgresDataBackend.\n"
                'Install it with: pip install "abstract-data-app[postgres]"'
            )

    def _conn(self):
        pg = self._psycopg2()
        local = self._local
        if not hasattr(local, "conn") or local.conn is None or local.conn.closed:
            local.conn = pg.connect(self.dsn)
        return local.conn

    def _ensure_tables(self) -> None:
        pg = self._psycopg2()
        conn = pg.connect(self.dsn)
        try:
            with conn.cursor() as cur:
                cur.execute(f"""
                    CREATE TABLE IF NOT EXISTS {self.TABLE} (
                        type_name TEXT NOT NULL,
                        key       TEXT NOT NULL,
                        data      TEXT NOT NULL,
                        PRIMARY KEY (type_name, key)
                    )
                """)
            conn.commit()
        finally:
            conn.close()

    def upsert(self, type_name: str, key: str, data: dict[str, Any]) -> None:
        conn = self._conn()
        try:
            with conn.cursor() as cur:
                cur.execute(
                    f"""
                    INSERT INTO {self.TABLE} (type_name, key, data)
                    VALUES (%s, %s, %s)
                    ON CONFLICT (type_name, key) DO UPDATE SET data = EXCLUDED.data
                    """,
                    (type_name, key, json.dumps(data)),
                )
            conn.commit()
        except Exception:
            conn.rollback()
            raise

    def delete(self, type_name: str, key: str) -> bool:
        conn = self._conn()
        try:
            with conn.cursor() as cur:
                cur.execute(
                    f"DELETE FROM {self.TABLE} WHERE type_name = %s AND key = %s",
                    (type_name, key),
                )
                deleted = cur.rowcount > 0
            conn.commit()
            return deleted
        except Exception:
            conn.rollback()
            raise

    def get(self, type_name: str, key: str) -> Optional[dict[str, Any]]:
        conn = self._conn()
        with conn.cursor() as cur:
            cur.execute(
                f"SELECT data FROM {self.TABLE} WHERE type_name = %s AND key = %s",
                (type_name, key),
            )
            row = cur.fetchone()
        return json.loads(row[0]) if row else None

    def list_all(self, type_name: str) -> list[dict[str, Any]]:
        conn = self._conn()
        with conn.cursor() as cur:
            cur.execute(
                f"SELECT key, data FROM {self.TABLE} WHERE type_name = %s",
                (type_name,),
            )
            return [{"key": row[0], "data": json.loads(row[1])} for row in cur.fetchall()]

    def dry_run_upsert(
        self, type_name: str, key: str, data: dict[str, Any]
    ) -> Optional[str]:
        conn = self._conn()
        try:
            with conn.cursor() as cur:
                cur.execute("SAVEPOINT _ada_dry_run")
                cur.execute(
                    f"""
                    INSERT INTO {self.TABLE} (type_name, key, data)
                    VALUES (%s, %s, %s)
                    ON CONFLICT (type_name, key) DO UPDATE SET data = EXCLUDED.data
                    """,
                    (type_name, key, json.dumps(data)),
                )
                cur.execute("ROLLBACK TO SAVEPOINT _ada_dry_run")
                cur.execute("RELEASE SAVEPOINT _ada_dry_run")
            return None
        except Exception as exc:
            try:
                with conn.cursor() as cur:
                    cur.execute("ROLLBACK TO SAVEPOINT _ada_dry_run")
                    cur.execute("RELEASE SAVEPOINT _ada_dry_run")
            except Exception:
                pass
            return str(exc)


# ---------------------------------------------------------------------------
# Redis
# ---------------------------------------------------------------------------

class RedisDataBackend(DataBackend):
    """
    Redis-backed store.

    Install the optional dependency::

        pip install "abstract-data-app[redis]"

    Data is stored as JSON strings under keys of the form::

        abstract_data_app:data:<type_name>:<key>

    A Redis Set per type tracks known keys::

        abstract_data_app:index:<type_name>

    redis-py's connection pool is thread-safe, so a single client instance is
    shared across all threads.
    """

    def __init__(self, host: str = "localhost", port: int = 6379, db: int = 0, **kwargs) -> None:
        try:
            import redis
        except ImportError:
            raise ImportError(
                "redis is required for RedisDataBackend.\n"
                'Install it with: pip install "abstract-data-app[redis]"'
            )
        import redis as redis_module
        self._r = redis_module.Redis(host=host, port=port, db=db, **kwargs)

    def _data_key(self, type_name: str, key: str) -> str:
        return f"abstract_data_app:data:{type_name}:{key}"

    def _index_key(self, type_name: str) -> str:
        return f"abstract_data_app:index:{type_name}"

    def upsert(self, type_name: str, key: str, data: dict[str, Any]) -> None:
        pipe = self._r.pipeline()
        pipe.set(self._data_key(type_name, key), json.dumps(data))
        pipe.sadd(self._index_key(type_name), key)
        pipe.execute()

    def delete(self, type_name: str, key: str) -> bool:
        pipe = self._r.pipeline()
        pipe.delete(self._data_key(type_name, key))
        pipe.srem(self._index_key(type_name), key)
        results = pipe.execute()
        return results[0] > 0

    def get(self, type_name: str, key: str) -> Optional[dict[str, Any]]:
        raw = self._r.get(self._data_key(type_name, key))
        return json.loads(raw) if raw is not None else None

    def list_all(self, type_name: str) -> list[dict[str, Any]]:
        raw_keys = self._r.smembers(self._index_key(type_name))
        result = []
        for raw_key in raw_keys:
            key = raw_key.decode() if isinstance(raw_key, bytes) else raw_key
            raw = self._r.get(self._data_key(type_name, key))
            if raw is not None:
                result.append({"key": key, "data": json.loads(raw)})
        return result

    def dry_run_upsert(
        self, type_name: str, key: str, data: dict[str, Any]
    ) -> Optional[str]:
        # Redis has no native transaction rollback; validate serializability only.
        try:
            json.dumps(data)
            return None
        except (TypeError, ValueError) as exc:
            return f"Data is not JSON-serialisable: {exc}"
