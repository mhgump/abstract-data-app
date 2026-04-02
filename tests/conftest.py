"""Shared test utilities."""

import json
import socket
import threading
import time
import urllib.error
import urllib.request
from typing import Any, Optional


def find_free_port() -> int:
    """Bind to port 0 and let the OS pick an available port."""
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def start_server(app, port: int) -> None:
    """Start app._flask in a daemon thread on the given port and wait until it accepts connections."""
    t = threading.Thread(
        target=app._flask.run,
        kwargs={"host": "127.0.0.1", "port": port, "use_reloader": False},
        daemon=True,
    )
    t.start()
    _wait_for_server("127.0.0.1", port)


def _wait_for_server(host: str, port: int, timeout: float = 5.0) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        try:
            with socket.create_connection((host, port), timeout=0.1):
                return
        except OSError:
            time.sleep(0.05)
    raise RuntimeError(f"Server on {host}:{port} did not start within {timeout}s")


class Client:
    """Thin HTTP client wrapper for test assertions."""

    def __init__(self, base_url: str) -> None:
        self.base = base_url.rstrip("/")

    def request(
        self,
        method: str,
        path: str,
        body: Any = None,
    ) -> tuple[int, Optional[Any]]:
        url = self.base + path
        data = json.dumps(body).encode() if body is not None else None
        headers = {"Content-Type": "application/json"}
        req = urllib.request.Request(url, data=data, headers=headers, method=method)
        try:
            with urllib.request.urlopen(req) as resp:
                raw = resp.read()
                return resp.status, json.loads(raw) if raw else None
        except urllib.error.HTTPError as exc:
            raw = exc.read()
            return exc.code, json.loads(raw) if raw else None

    # Convenience shorthands
    def get(self, path: str) -> tuple[int, Any]:
        return self.request("GET", path)

    def put(self, path: str, body: Any) -> tuple[int, Any]:
        return self.request("PUT", path, body)

    def delete(self, path: str) -> tuple[int, Any]:
        return self.request("DELETE", path)

    def mcp(self, method: str, params: dict = None, req_id: int = 1) -> tuple[int, Any]:
        payload = {"jsonrpc": "2.0", "id": req_id, "method": method}
        if params is not None:
            payload["params"] = params
        return self.request("POST", "/mcp", payload)

    def tool(self, name: str, arguments: dict, req_id: int = 1) -> Any:
        """Call an MCP tool and return the parsed result dict (raises on isError)."""
        _, resp = self.mcp("tools/call", {"name": name, "arguments": arguments}, req_id)
        content = resp["result"]
        if content.get("isError"):
            raise AssertionError(f"MCP tool '{name}' returned error: {content['content']}")
        return json.loads(content["content"][0]["text"])
