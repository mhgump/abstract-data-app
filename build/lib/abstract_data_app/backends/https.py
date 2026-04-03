import json
from typing import Any, Optional

from .base import DataBackend


class HttpsDataBackend(DataBackend):
    """
    Data backend that proxies every operation to a running abstract-data-app
    instance over HTTP/HTTPS.

    Each :class:`DataBackend` method is translated to the corresponding CRUD
    route exposed by the remote server:

    =====================  =================================================
    ``upsert``             ``PUT  /data/<TypeName>/<key>``  (JSON body)
    ``delete``             ``DELETE /data/<TypeName>/<key>``
    ``get``                ``GET  /data/<TypeName>/<key>``
    ``list_all``           ``GET  /data/<TypeName>``
    ``dry_run_upsert``     not supported over HTTP — always returns ``None``
    =====================  =================================================

    Args:
        base_url: Root URL of the remote abstract-data-app instance, e.g.
                  ``"https://myserver.example.com"`` or
                  ``"http://localhost:8000"``.  Trailing slashes are stripped.

    Example::

        remote = HttpsDataBackend("https://myserver.example.com")
        app = abstract_data_app.init(data_backend=remote)
        app.add_data_type(Widget)
        # All reads/writes are forwarded to the remote server.
        app.serve_forever()
    """

    def __init__(self, base_url: str) -> None:
        self.base_url = base_url.rstrip("/")

    def _request(self, method: str, path: str, body: Optional[dict] = None) -> tuple[int, Any]:
        """Send an HTTP request and return ``(status_code, parsed_json_body)``."""
        import urllib.request
        import urllib.error

        url = self.base_url + path
        encoded = json.dumps(body).encode() if body is not None else None
        headers = {"Content-Type": "application/json"} if encoded is not None else {}
        req = urllib.request.Request(url, data=encoded, headers=headers, method=method)
        try:
            with urllib.request.urlopen(req) as resp:
                return resp.status, json.loads(resp.read())
        except urllib.error.HTTPError as exc:
            try:
                return exc.code, json.loads(exc.read())
            except Exception:
                return exc.code, {"error": str(exc)}

    def upsert(self, type_name: str, key: str, data: dict[str, Any]) -> None:
        status, resp = self._request("PUT", f"/data/{type_name}/{key}", body=data)
        if status not in (200, 201):
            raise RuntimeError(
                f"HttpsDataBackend upsert failed (HTTP {status}): {resp.get('error', resp)}"
            )

    def delete(self, type_name: str, key: str) -> bool:
        status, resp = self._request("DELETE", f"/data/{type_name}/{key}")
        if status not in (200, 404):
            raise RuntimeError(
                f"HttpsDataBackend delete failed (HTTP {status}): {resp.get('error', resp)}"
            )
        return resp.get("deleted", False)

    def get(self, type_name: str, key: str) -> Optional[dict[str, Any]]:
        status, resp = self._request("GET", f"/data/{type_name}/{key}")
        if status == 404:
            return None
        if status != 200:
            raise RuntimeError(
                f"HttpsDataBackend get failed (HTTP {status}): {resp.get('error', resp)}"
            )
        return resp.get("data")

    def list_all(self, type_name: str) -> list[dict[str, Any]]:
        status, resp = self._request("GET", f"/data/{type_name}")
        if status != 200:
            raise RuntimeError(
                f"HttpsDataBackend list failed (HTTP {status}): {resp.get('error', resp)}"
            )
        return resp.get("items", [])

    def dry_run_upsert(
        self, type_name: str, key: str, data: dict[str, Any]
    ) -> Optional[str]:
        # No dry-run HTTP route exists on the remote server; the actual upsert
        # will surface any backend-level errors at write time.
        return None
