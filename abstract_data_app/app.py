"""
Core App class and ``init()`` factory function.

Architecture
------------
- One Flask application handles both the CRUD HTTP routes and the MCP endpoint.
- A fixed-size thread pool (``_ThreadPoolWSGIServer``) handles concurrent
  requests, sized by ``Config.num_threads``.
- One ``threading.Lock`` per registered data type serialises *write* operations
  (upsert / delete) so that multi-backend fan-out is atomic at the app level.
  Read operations (get / list) do not acquire the lock.
- All request handlers wrap their body in a try/except so that a single bad
  request never brings down the server.
- ``serve_forever()`` catches server-level exceptions and restarts automatically
  so the process stays alive indefinitely.

MCP transport
-------------
Implements the *Streamable HTTP* transport (JSON-RPC 2.0 over HTTP POST).
Supported methods: ``initialize``, ``notifications/initialized``,
``tools/list``, ``tools/call``.
"""

import json
import sys
import threading
import time
import traceback
from concurrent.futures import ThreadPoolExecutor
from socketserver import TCPServer
from typing import Any, Optional
from wsgiref.simple_server import WSGIRequestHandler, WSGIServer

from flask import Flask, jsonify, request

from .backend import DataBackend
from .config import Config
from .operations import Operation
from .validation import dataclass_to_json_schema, validate_dataclass_dict


# ---------------------------------------------------------------------------
# Thread-pooled WSGI server
# ---------------------------------------------------------------------------

class _ThreadPoolWSGIServer(WSGIServer):
    """WSGI server backed by a fixed-size ``ThreadPoolExecutor``.

    Werkzeug's default ``threaded=True`` spawns an unbounded number of threads.
    This implementation caps concurrency at ``num_threads``.
    """

    allow_reuse_address = True
    daemon_threads = True

    def __init__(self, server_address: tuple, handler_class, num_threads: int) -> None:
        WSGIServer.__init__(self, server_address, handler_class)
        self._pool = ThreadPoolExecutor(max_workers=num_threads, thread_name_prefix="ada-worker")

    def process_request(self, request, client_address) -> None:  # type: ignore[override]
        self._pool.submit(self._process_in_thread, request, client_address)

    def _process_in_thread(self, request, client_address) -> None:
        try:
            self.finish_request(request, client_address)
        except Exception:
            self.handle_error(request, client_address)
        finally:
            self.shutdown_request(request)

    def server_close(self) -> None:
        self._pool.shutdown(wait=False)
        WSGIServer.server_close(self)


# ---------------------------------------------------------------------------
# App
# ---------------------------------------------------------------------------

class App:
    """
    The assembled HTTP + MCP application.

    Do not construct directly — use :func:`init`.
    """

    _MCP_PROTOCOL_VERSION = "2024-11-05"
    _SERVER_INFO = {"name": "abstract-data-app", "version": "0.1.0"}

    def __init__(
        self,
        data_backends: list[DataBackend],
        data_types: list[type],
        operations: list[type[Operation]],
        config: Config,
    ) -> None:
        if not data_backends:
            raise ValueError("At least one DataBackend must be provided.")

        self.backends: list[DataBackend] = data_backends
        self.config = config

        # Map type name → class
        self.data_types: dict[str, type] = {dt.__name__: dt for dt in data_types}

        # Instantiate operations; key by TOOL_SPEC["name"] for O(1) dispatch
        self.operations: dict[str, Operation] = {}
        for op_class in operations:
            instance = op_class()
            tool_name = instance.TOOL_SPEC.get("name") or op_class.__name__
            self.operations[tool_name] = instance

        # One write-lock per data type
        self._write_locks: dict[str, threading.Lock] = {
            name: threading.Lock() for name in self.data_types
        }

        # Pre-build MCP tool list once (it's static after init)
        self._mcp_tools: list[dict] = self._build_mcp_tools()

        self._flask = self._create_flask_app()

    # ------------------------------------------------------------------
    # Flask app construction
    # ------------------------------------------------------------------

    def _create_flask_app(self) -> Flask:
        app = Flask(__name__)
        app.json.sort_keys = False  # preserve field order

        for type_name in self.data_types:
            self._register_crud_routes(app, type_name)

        app.add_url_rule(
            self.config.mcp_path,
            endpoint="mcp",
            view_func=self._handle_mcp_request,
            methods=["POST"],
        )

        @app.errorhandler(Exception)
        def _unhandled(exc: Exception):
            self._log_error("Unhandled exception in request", exc)
            return jsonify({"error": str(exc)}), 500

        return app

    def _register_crud_routes(self, app: Flask, type_name: str) -> None:
        # /data/<TypeName>/<key>  →  GET / PUT / POST / DELETE
        app.add_url_rule(
            f"/data/{type_name}/<key>",
            endpoint=f"{type_name}_item",
            view_func=self._make_item_handler(type_name),
            methods=["GET", "PUT", "POST", "DELETE"],
        )
        # /data/<TypeName>        →  GET (list, optional ?filter=<jq>)
        app.add_url_rule(
            f"/data/{type_name}",
            endpoint=f"{type_name}_list",
            view_func=self._make_list_handler(type_name),
            methods=["GET"],
        )

    # ------------------------------------------------------------------
    # HTTP route handler factories
    # ------------------------------------------------------------------

    def _make_item_handler(self, type_name: str):
        def handler(key: str):
            try:
                method = request.method
                if method == "GET":
                    item = self._do_get(type_name, key)
                    if item is None:
                        return jsonify({"error": f"Key '{key}' not found"}), 404
                    return jsonify({"key": key, "data": item})

                if method in ("PUT", "POST"):
                    body = request.get_json(force=True, silent=True)
                    if body is None:
                        return jsonify({"error": "Request body must be valid JSON"}), 400
                    result = self._do_upsert(type_name, key, body)
                    return jsonify(result), 200

                if method == "DELETE":
                    result = self._do_delete(type_name, key)
                    status = 200 if result["deleted"] else 404
                    return jsonify(result), status

                return jsonify({"error": f"Method {method} not allowed"}), 405

            except Exception as exc:
                self._log_error(f"{type_name} item handler", exc)
                return jsonify({"error": str(exc)}), 500

        handler.__name__ = f"{type_name}_item_handler"
        return handler

    def _make_list_handler(self, type_name: str):
        def handler():
            jq_filter = request.args.get("filter")
            try:
                items = self._do_list(type_name, jq_filter)
                count = len(items) if isinstance(items, list) else None
                return jsonify({"items": items, "count": count})
            except ValueError as exc:
                return jsonify({"error": str(exc)}), 400
            except Exception as exc:
                self._log_error(f"{type_name} list handler", exc)
                return jsonify({"error": str(exc)}), 500

        handler.__name__ = f"{type_name}_list_handler"
        return handler

    # ------------------------------------------------------------------
    # MCP endpoint
    # ------------------------------------------------------------------

    def _handle_mcp_request(self):
        """Single HTTP POST endpoint that implements JSON-RPC 2.0 for MCP."""
        body = request.get_json(force=True, silent=True)
        if body is None:
            return jsonify(self._jsonrpc_error(None, -32700, "Parse error")), 400

        req_id = body.get("id")  # None for notifications
        method: str = body.get("method", "")
        params: dict = body.get("params") or {}

        # Notifications have no "id" and require no response body.
        if "id" not in body:
            return ("", 204)

        try:
            result = self._dispatch_mcp(method, params)
            return jsonify({"jsonrpc": "2.0", "id": req_id, "result": result})
        except _McpMethodNotFound as exc:
            return jsonify(self._jsonrpc_error(req_id, -32601, str(exc))), 404
        except _McpInvalidParams as exc:
            return jsonify(self._jsonrpc_error(req_id, -32602, str(exc))), 400
        except Exception as exc:
            self._log_error("MCP dispatch", exc)
            return jsonify(self._jsonrpc_error(req_id, -32603, str(exc))), 500

    def _dispatch_mcp(self, method: str, params: dict) -> Any:
        if method == "initialize":
            return {
                "protocolVersion": self._MCP_PROTOCOL_VERSION,
                "capabilities": {"tools": {}},
                "serverInfo": self._SERVER_INFO,
            }

        if method in ("notifications/initialized", "ping"):
            return {}

        if method == "tools/list":
            return {"tools": self._mcp_tools}

        if method == "tools/call":
            name = params.get("name")
            arguments = params.get("arguments") or {}
            if not name:
                raise _McpInvalidParams("tools/call requires 'name'")
            return self._execute_tool(name, arguments)

        raise _McpMethodNotFound(f"Unknown method: '{method}'")

    def _execute_tool(self, tool_name: str, arguments: dict) -> dict:
        """Run a tool and return an MCP-formatted result."""
        try:
            result = self._dispatch_tool(tool_name, arguments)
            text = json.dumps(result, indent=2, default=str)
            return {"content": [{"type": "text", "text": text}]}
        except Exception as exc:
            self._log_error(f"Tool '{tool_name}'", exc)
            return {
                "content": [{"type": "text", "text": str(exc)}],
                "isError": True,
            }

    def _dispatch_tool(self, tool_name: str, arguments: dict) -> Any:
        # Operation tools
        if tool_name in self.operations:
            return self.operations[tool_name].call(arguments)

        # Data type tools: <TypeName>_<action>
        for type_name in self.data_types:
            prefix = f"{type_name}_"
            if tool_name.startswith(prefix):
                action = tool_name[len(prefix):]
                return self._dispatch_data_tool(type_name, action, arguments)

        raise ValueError(f"Unknown tool: '{tool_name}'")

    def _dispatch_data_tool(self, type_name: str, action: str, args: dict) -> Any:
        if action == "upsert":
            _require(args, "key", "data")
            return self._do_upsert(type_name, args["key"], args["data"])
        if action == "delete":
            _require(args, "key")
            return self._do_delete(type_name, args["key"])
        if action == "get":
            _require(args, "key")
            item = self._do_get(type_name, args["key"])
            if item is None:
                return {"found": False, "key": args["key"]}
            return {"found": True, "key": args["key"], "data": item}
        if action == "list":
            return self._do_list(type_name, args.get("jq_filter"))
        if action == "validate":
            return self._do_validate(type_name, args.get("data") or {})
        raise ValueError(f"Unknown data action: '{action}' for type '{type_name}'")

    # ------------------------------------------------------------------
    # Core data operations  (shared by HTTP handlers and MCP tools)
    # ------------------------------------------------------------------

    def _do_upsert(self, type_name: str, key: str, data: dict) -> dict:
        """Write to all backends under the per-type write lock."""
        with self._write_locks[type_name]:
            errors: list[str] = []
            for backend in self.backends:
                try:
                    backend.upsert(type_name, key, data)
                except Exception as exc:
                    errors.append(f"{backend.__class__.__name__}: {exc}")
            if errors:
                raise RuntimeError("Upsert failed on one or more backends: " + "; ".join(errors))
        return {"key": key, "data": data}

    def _do_delete(self, type_name: str, key: str) -> dict:
        """Delete from all backends under the per-type write lock."""
        with self._write_locks[type_name]:
            existed = False
            errors: list[str] = []
            for backend in self.backends:
                try:
                    if backend.delete(type_name, key):
                        existed = True
                except Exception as exc:
                    errors.append(f"{backend.__class__.__name__}: {exc}")
            if errors:
                raise RuntimeError("Delete failed on one or more backends: " + "; ".join(errors))
        return {"deleted": existed, "key": key}

    def _do_get(self, type_name: str, key: str) -> Optional[dict]:
        """Read from the primary (first) backend. No lock needed for reads."""
        return self.backends[0].get(type_name, key)

    def _do_list(self, type_name: str, jq_filter: Optional[str]) -> list[dict]:
        """List from the primary backend, then apply an optional jq filter."""
        items = self.backends[0].list_all(type_name)
        if not jq_filter:
            return items
        try:
            import jq as jq_lib
            outputs = jq_lib.compile(jq_filter).input(items).all()
            # A filter like [.[] | select(...)] produces one array output; unwrap it.
            # A filter like .[] | select(...) produces N individual outputs; return as-is.
            if len(outputs) == 1 and isinstance(outputs[0], list):
                return outputs[0]
            return outputs
        except ImportError:
            raise RuntimeError(
                "jq filtering requires the 'jq' package. "
                "Install it with: pip install jq"
            )
        except Exception as exc:
            raise ValueError(f"Invalid jq filter '{jq_filter}': {exc}") from exc

    def _do_validate(self, type_name: str, data: dict) -> dict:
        """
        Validate *data* as an instance of *type_name* in two phases:

        1. Parse each field in parallel and collect type errors.
        2. If phase 1 is clean, perform a dry-run upsert on every backend to
           surface any backend-level constraints.
        """
        data_type = self.data_types[type_name]
        errors = validate_dataclass_dict(data_type, data)

        if not errors:
            for backend in self.backends:
                backend_error = backend.dry_run_upsert(
                    type_name, "__validation_dry_run__", data
                )
                if backend_error:
                    errors.append(
                        f"Backend {backend.__class__.__name__} dry-run: {backend_error}"
                    )

        return {"valid": not errors, "errors": errors}

    # ------------------------------------------------------------------
    # MCP tool list builder
    # ------------------------------------------------------------------

    def _build_mcp_tools(self) -> list[dict]:
        tools: list[dict] = []

        for type_name, data_type in self.data_types.items():
            schema = dataclass_to_json_schema(data_type)

            tools.append({
                "name": f"{type_name}_upsert",
                "description": f"Insert or update a {type_name} item by key.",
                "inputSchema": {
                    "type": "object",
                    "properties": {
                        "key": {"type": "string", "description": "Unique item key"},
                        "data": {**schema, "description": f"{type_name} payload"},
                    },
                    "required": ["key", "data"],
                },
            })

            tools.append({
                "name": f"{type_name}_delete",
                "description": f"Delete a {type_name} item by key.",
                "inputSchema": {
                    "type": "object",
                    "properties": {
                        "key": {"type": "string"},
                    },
                    "required": ["key"],
                },
            })

            tools.append({
                "name": f"{type_name}_get",
                "description": f"Retrieve a {type_name} item by key.",
                "inputSchema": {
                    "type": "object",
                    "properties": {
                        "key": {"type": "string"},
                    },
                    "required": ["key"],
                },
            })

            tools.append({
                "name": f"{type_name}_list",
                "description": (
                    f"List all {type_name} items. "
                    "Optionally filter results using a jq expression applied to the "
                    "array of {\"key\": ..., \"data\": {...}} objects."
                ),
                "inputSchema": {
                    "type": "object",
                    "properties": {
                        "jq_filter": {
                            "type": "string",
                            "description": (
                                "jq expression, e.g. '.[] | select(.data.active == true)'"
                            ),
                        },
                    },
                },
            })

            tools.append({
                "name": f"{type_name}_validate",
                "description": (
                    f"Validate a JSON object as a {type_name}. "
                    "Returns a list of errors (empty if valid). "
                    "Each field is checked in parallel; if all pass, a dry-run "
                    "upsert is attempted against all configured backends."
                ),
                "inputSchema": {
                    "type": "object",
                    "properties": {
                        "data": {
                            "type": "object",
                            "description": f"The {type_name} payload to validate",
                        },
                    },
                    "required": ["data"],
                },
            })

        for op_name, op in self.operations.items():
            tools.append(op.TOOL_SPEC)

        return tools

    # ------------------------------------------------------------------
    # Server lifecycle
    # ------------------------------------------------------------------

    def serve_forever(self) -> None:
        """
        Start the server and block until interrupted (Ctrl-C).

        If the server crashes for any reason other than ``KeyboardInterrupt``,
        the error is printed and the server restarts automatically after a
        one-second pause.
        """
        addr = (self.config.host, self.config.port)
        print(
            f"abstract-data-app listening on http://{self.config.host}:{self.config.port}",
            flush=True,
        )
        print(
            f"  MCP endpoint: http://{self.config.host}:{self.config.port}{self.config.mcp_path}",
            flush=True,
        )
        print(
            f"  Data types: {', '.join(self.data_types) or '(none)'}",
            flush=True,
        )
        print(
            f"  Operations: {', '.join(self.operations) or '(none)'}",
            flush=True,
        )
        print(
            f"  Thread pool: {self.config.num_threads} workers",
            flush=True,
        )

        while True:
            server: Optional[_ThreadPoolWSGIServer] = None
            try:
                server = _ThreadPoolWSGIServer(addr, WSGIRequestHandler, self.config.num_threads)
                server.set_app(self._flask)
                server.serve_forever()
                # serve_forever() only returns on an explicit server.shutdown() call.
                break
            except KeyboardInterrupt:
                print("\nShutting down.", flush=True)
                break
            except Exception as exc:
                self._log_error("Server crashed", exc)
                if server is not None:
                    try:
                        server.server_close()
                    except Exception:
                        pass
                print("Restarting server in 1 second...", file=sys.stderr, flush=True)
                time.sleep(1)

    # ------------------------------------------------------------------
    # Utilities
    # ------------------------------------------------------------------

    def _log_error(self, context: str, exc: Exception) -> None:
        if self.config.print_errors:
            print(f"[abstract-data-app] ERROR in {context}: {exc}", file=sys.stderr, flush=True)
            traceback.print_exc(file=sys.stderr)

    @staticmethod
    def _jsonrpc_error(req_id: Any, code: int, message: str) -> dict:
        return {
            "jsonrpc": "2.0",
            "id": req_id,
            "error": {"code": code, "message": message},
        }


# ---------------------------------------------------------------------------
# Sentinel exceptions for MCP dispatch
# ---------------------------------------------------------------------------

class _McpMethodNotFound(Exception):
    pass


class _McpInvalidParams(Exception):
    pass


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _require(args: dict, *keys: str) -> None:
    missing = [k for k in keys if k not in args]
    if missing:
        raise _McpInvalidParams(f"Missing required argument(s): {missing}")


# ---------------------------------------------------------------------------
# Public factory
# ---------------------------------------------------------------------------

def init(
    data_backend: list[DataBackend] | DataBackend,
    data_types: list[type],
    operations: list[type[Operation]] | None = None,
    config: Config | None = None,
) -> App:
    """
    Create and return an :class:`App` instance.

    Args:
        data_backend: One backend or a list of backends.  All write operations
                      are fanned out to every backend in the list.
        data_types:   List of Python *dataclass* classes to expose as CRUD
                      resources and MCP tools.
        operations:   List of :class:`~abstract_data_app.Operation` subclasses
                      to expose as MCP tools.
        config:       Optional :class:`~abstract_data_app.Config`; defaults are
                      used if not provided.

    Example::

        app = abstract_data_app.init(
            data_backend=[LocalSqliteDataBackend("store.db")],
            data_types=[MyDataType],
            operations=[MyOperation],
        )
        app.serve_forever()
    """
    if isinstance(data_backend, DataBackend):
        data_backend = [data_backend]

    return App(
        data_backends=data_backend,
        data_types=data_types,
        operations=list(operations or []),
        config=config or Config(),
    )
