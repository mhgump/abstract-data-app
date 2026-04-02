import json
import threading
from typing import Any, Optional

from .base import DataBackend


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
