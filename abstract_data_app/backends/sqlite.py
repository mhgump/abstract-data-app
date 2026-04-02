import json
import threading
from typing import Any, Optional

from .base import DataBackend


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
