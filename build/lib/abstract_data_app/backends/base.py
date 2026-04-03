from abc import ABC, abstractmethod
from typing import Any, Optional


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
