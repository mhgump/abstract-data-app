import json
from typing import Any, Optional

from .base import DataBackend


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
