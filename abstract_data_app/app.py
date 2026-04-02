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

Building an app
---------------
Create an :class:`App` via :func:`init`, then register data types and operations
one at a time before starting the server::

    app = abstract_data_app.init(data_backend=LocalSqliteDataBackend(":memory:"))
    app.add_data_type(MyType, MCP_SPEC={"field": {"description": "My field"}})
    app.add_operation(MyOperation)
    app.serve_forever()

MCP transport
-------------
Implements the *Streamable HTTP* transport (JSON-RPC 2.0 over HTTP POST).
Supported methods: ``initialize``, ``notifications/initialized``,
``tools/list``, ``tools/call``.
"""

from __future__ import annotations

import copy
import dataclasses
import inspect
import json
import sys
import threading
import time
import traceback
import uuid
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
from enum import Enum
from typing import Any, Optional
from wsgiref.simple_server import WSGIRequestHandler, WSGIServer

from flask import Flask, jsonify, request

from .backends import DataBackend
from .config import Config
from .operations import CancellationToken, Operation, OperationCancelledError
from .validation import dataclass_to_json_schema, validate_dataclass_dict


# ---------------------------------------------------------------------------
# MCPToolType enum
# ---------------------------------------------------------------------------

class MCPToolType(str, Enum):
    """
    The type of auto-generated MCP tool for a registered data type.

    Used with :meth:`App.get_mcp_spec` to retrieve the spec for a specific
    tool variant::

        spec = app.get_mcp_spec("Product", MCPToolType.UPSERT)
    """
    UPSERT = "upsert"
    DELETE = "delete"
    GET = "get"
    LIST = "list"
    VALIDATE = "validate"


# ---------------------------------------------------------------------------
# Async operation record
# ---------------------------------------------------------------------------

_OP_PENDING = "pending"
_OP_RUNNING = "running"
_OP_COMPLETED = "completed"
_OP_FAILED = "failed"
_OP_CANCELLED = "cancelled"
_OP_TERMINAL = {_OP_COMPLETED, _OP_FAILED, _OP_CANCELLED}


def _utcnow() -> str:
    return datetime.now(timezone.utc).isoformat()


class _OperationRecord:
    """Tracks the full lifecycle of one async operation invocation."""

    def __init__(self, operation_id: str, operation_name: str, tool_input: dict) -> None:
        self.operation_id = operation_id
        self.operation_name = operation_name
        self.status = _OP_PENDING
        self.created_at: str = _utcnow()
        self.started_at: Optional[str] = None
        self.completed_at: Optional[str] = None
        self.input = tool_input
        self.result: Any = None
        self.error: Optional[str] = None
        self.cancellation_token = CancellationToken()

    def to_dict(self) -> dict:
        return {
            "operation_id": self.operation_id,
            "operation_name": self.operation_name,
            "status": self.status,
            "created_at": self.created_at,
            "started_at": self.started_at,
            "completed_at": self.completed_at,
            "input": self.input,
            "result": self.result,
            "error": self.error,
        }


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

    Do not construct directly — use :func:`init`, then call
    :meth:`add_data_type` and :meth:`add_operation` before starting the server.
    """

    _MCP_PROTOCOL_VERSION = "2024-11-05"
    _SERVER_INFO = {"name": "abstract-data-app", "version": "0.1.0"}

    def __init__(
        self,
        data_backends: list[DataBackend],
        config: Config,
    ) -> None:
        if not data_backends:
            raise ValueError("At least one DataBackend must be provided.")

        self.backends: list[DataBackend] = data_backends
        self.config = config

        # Map type name → class
        self.data_types: dict[str, type] = {}

        # Map type name → MCP_SPEC field dict (field_name → {"description": ...})
        self._mcp_field_specs: dict[str, dict] = {}

        # Keyed by TOOL_SPEC["name"] for O(1) dispatch
        self.operations: dict[str, Operation] = {}

        # One write-lock per data type
        self._write_locks: dict[str, threading.Lock] = {}

        # Pre-built MCP tool list (rebuilt whenever add_data_type / add_operation is called)
        self._mcp_tools: list[dict] = []

        # Flask app built lazily on first access (or rebuilt after registration changes)
        self._flask_cached: Optional[Flask] = None

        # Async operation tracking
        self._op_records: dict[str, _OperationRecord] = {}
        self._op_records_lock = threading.Lock()

    # ------------------------------------------------------------------
    # Registration API
    # ------------------------------------------------------------------

    @property
    def _flask(self) -> Flask:
        """Lazily build (or rebuild) the Flask app on first access."""
        if self._flask_cached is None:
            self._flask_cached = self._create_flask_app()
        return self._flask_cached

    def add_data_type(self, data_type: type, MCP_SPEC: Optional[dict] = None) -> "App":
        """
        Register a dataclass as a CRUD resource and MCP tool set.

        This method must be called before :meth:`serve_forever`.  It can be
        called multiple times to register additional types.

        Args:
            data_type: A Python ``@dataclass`` class.
            MCP_SPEC:  Optional dict mapping field names to
                       ``{"description": "..."}`` dicts.  Every key in
                       *MCP_SPEC* must be a field name on *data_type*; the
                       framework raises ``ValueError`` if an unknown key is
                       found.  Fields omitted from *MCP_SPEC* will have no
                       description in the generated tool schema.

        Returns:
            ``self``, enabling optional method chaining.

        Raises:
            ValueError: If *data_type* is not a dataclass, or if *MCP_SPEC*
                        contains keys that are not field names of *data_type*.

        Example::

            app.add_data_type(
                Product,
                MCP_SPEC={
                    "name":     {"description": "Display name of the product"},
                    "price":    {"description": "Price in USD"},
                    "in_stock": {"description": "Whether the item is available"},
                },
            )
        """
        if not dataclasses.is_dataclass(data_type):
            raise ValueError(f"{data_type.__name__} is not a dataclass")
        if MCP_SPEC is not None:
            _assert_mcp_spec_matches(data_type, MCP_SPEC)

        type_name = data_type.__name__
        self.data_types[type_name] = data_type
        self._mcp_field_specs[type_name] = MCP_SPEC or {}
        self._write_locks[type_name] = threading.Lock()
        self._mcp_tools = self._build_mcp_tools()
        self._flask_cached = None  # force Flask rebuild on next access
        return self

    def add_operation(self, op_class: type[Operation]) -> "App":
        """
        Register an :class:`~abstract_data_app.Operation` subclass as an MCP tool.

        The operation is exposed under the name given in its ``TOOL_SPEC["name"]``
        field.  This method can be called multiple times to add additional
        operations.

        Args:
            op_class: A concrete subclass of :class:`~abstract_data_app.Operation`.

        Returns:
            ``self``, enabling optional method chaining.

        Example::

            app.add_operation(ClaimOp)
        """
        instance = op_class()
        tool_name = instance.TOOL_SPEC.get("name") or op_class.__name__
        self.operations[tool_name] = instance
        self._mcp_tools = self._build_mcp_tools()
        self._flask_cached = None  # force Flask rebuild on next access
        return self

    # ------------------------------------------------------------------
    # Programmatic MCP tool inspection
    # ------------------------------------------------------------------

    def list_mcp_tools(self) -> list[dict]:
        """
        Return the full list of registered MCP tool specs.

        Each entry is a dict with ``name``, ``description``, and
        ``inputSchema`` keys — the same structure served by the
        ``tools/list`` MCP method.

        Returns:
            A new list of tool spec dicts (shallow copy of the internal list).
        """
        return list(self._mcp_tools)

    def get_mcp_spec(
        self,
        *,
        data_type: Optional[type] = None,
        tool_type: Optional[MCPToolType] = None,
        operation=None,
    ) -> dict:
        """
        Return the MCP tool spec for a data-type tool or an operation.

        Pass either ``operation`` alone, or both ``data_type`` and ``tool_type``.

        Args:
            data_type: The registered dataclass (e.g. ``Product``).
            tool_type: A :class:`MCPToolType` value selecting which of the
                       five auto-generated tools to retrieve.
            operation: An :class:`~abstract_data_app.Operation` subclass (or
                       instance).  When provided, returns its ``TOOL_SPEC``
                       directly.  ``data_type`` and ``tool_type`` are ignored.

        Returns:
            The tool spec dict (``name``, ``description``, ``inputSchema``).
            For data-type tools, this is the pre-built spec that includes any
            field descriptions supplied via ``MCP_SPEC``.

        Raises:
            KeyError: If no matching data-type tool is found.
            ValueError: If neither ``operation`` nor both ``data_type`` +
                        ``tool_type`` are provided.

        Examples::

            # Data-type tool (includes MCP_SPEC descriptions if registered)
            spec = app.get_mcp_spec(data_type=Product, tool_type=MCPToolType.UPSERT)

            # Operation tool (passthrough to TOOL_SPEC)
            spec = app.get_mcp_spec(operation=ClaimOp)
        """
        if operation is not None:
            return operation.TOOL_SPEC
        if data_type is None or tool_type is None:
            raise ValueError(
                "Provide either `operation=` or both `data_type=` and `tool_type=`"
            )
        tool_name = f"{data_type.__name__}_{tool_type.value}"
        for tool in self._mcp_tools:
            if tool["name"] == tool_name:
                return tool
        available = [t["name"] for t in self._mcp_tools]
        raise KeyError(
            f"No MCP tool '{tool_name}' registered. Available tools: {available}"
        )

    # ------------------------------------------------------------------
    # Programmatic data access  (no HTTP server required)
    # ------------------------------------------------------------------

    def upsert(self, data_type: type, key: str, data: Any) -> dict:
        """
        Insert or update an item in all backends.

        Can be used without calling :meth:`serve_forever`.

        Args:
            data_type: The registered dataclass class (e.g. ``Widget``).
            key: The item's string key.
            data: The item's data — either a dataclass instance or a plain
                  ``dict``.  Dataclass instances are converted automatically
                  via :func:`dataclasses.asdict`.

        Returns:
            ``{"key": key, "data": <data dict>}``

        Raises:
            KeyError: If *data_type* has not been registered via
                      :meth:`add_data_type`.
        """
        type_name = data_type.__name__
        if type_name not in self.data_types:
            raise KeyError(f"Data type '{type_name}' is not registered. Call add_data_type() first.")
        if dataclasses.is_dataclass(data) and not isinstance(data, type):
            data = dataclasses.asdict(data)
        return self._do_upsert(type_name, key, data)

    def get(self, data_type: type, key: str) -> Optional[Any]:
        """
        Retrieve one item from the primary backend.

        Can be used without calling :meth:`serve_forever`.

        Args:
            data_type: The registered dataclass class.
            key: The item's string key.

        Returns:
            A dataclass instance constructed from the stored data, or ``None``
            if the key does not exist.

        Raises:
            KeyError: If *data_type* has not been registered.
        """
        type_name = data_type.__name__
        if type_name not in self.data_types:
            raise KeyError(f"Data type '{type_name}' is not registered. Call add_data_type() first.")
        raw = self._do_get(type_name, key)
        if raw is None:
            return None
        return data_type(**raw)

    def delete(self, data_type: type, key: str) -> bool:
        """
        Delete one item from all backends.

        Can be used without calling :meth:`serve_forever`.

        Args:
            data_type: The registered dataclass class.
            key: The item's string key.

        Returns:
            ``True`` if the key existed and was deleted, ``False`` if it was
            not found.

        Raises:
            KeyError: If *data_type* has not been registered.
        """
        type_name = data_type.__name__
        if type_name not in self.data_types:
            raise KeyError(f"Data type '{type_name}' is not registered. Call add_data_type() first.")
        return self._do_delete(type_name, key)["deleted"]

    def list(self, data_type: type) -> list:
        """
        List all items of a given type from the primary backend.

        Can be used without calling :meth:`serve_forever`.

        Args:
            data_type: The registered dataclass class.

        Returns:
            A list of ``{"key": str, "data": <dataclass instance>}`` dicts,
            one per stored item.

        Raises:
            KeyError: If *data_type* has not been registered.
        """
        type_name = data_type.__name__
        if type_name not in self.data_types:
            raise KeyError(f"Data type '{type_name}' is not registered. Call add_data_type() first.")
        raw_items = self._do_list(type_name, None)
        return [{"key": item["key"], "data": data_type(**item["data"])} for item in raw_items]

    def call(self, operation_name: str, tool_input: dict) -> Any:
        """
        Invoke a registered operation by name.

        Can be used without calling :meth:`serve_forever`.

        Args:
            operation_name: The name from the operation's ``TOOL_SPEC["name"]``
                            field.
            tool_input: The argument dict forwarded to the operation's
                        :meth:`~abstract_data_app.Operation.call` method.

        Returns:
            Whatever the operation's :meth:`~abstract_data_app.Operation.call`
            method returns.

        Raises:
            KeyError: If no operation with that name is registered.
        """
        if operation_name not in self.operations:
            raise KeyError(f"Operation '{operation_name}' is not registered. Call add_operation() first.")
        return self.operations[operation_name].call(tool_input)

    # ------------------------------------------------------------------
    # Flask app construction
    # ------------------------------------------------------------------

    def _create_flask_app(self) -> Flask:
        app = Flask(__name__)
        app.json.sort_keys = False  # preserve field order

        for type_name in self.data_types:
            self._register_crud_routes(app, type_name)

        self._register_operation_routes(app)

        app.add_url_rule(
            self.config.mcp_path,
            endpoint="mcp",
            view_func=self._handle_mcp_request,
            methods=["POST"],
        )

        if self.config.cors_origin:
            cors_origin = self.config.cors_origin

            @app.before_request
            def _handle_preflight():
                if request.method == "OPTIONS":
                    response = app.make_response("")
                    response.headers["Access-Control-Allow-Origin"] = cors_origin
                    response.headers["Access-Control-Allow-Methods"] = (
                        "GET, POST, PUT, DELETE, OPTIONS"
                    )
                    response.headers["Access-Control-Allow-Headers"] = (
                        "Content-Type, Authorization"
                    )
                    return response

            @app.after_request
            def _add_cors_headers(response):
                response.headers["Access-Control-Allow-Origin"] = cors_origin
                response.headers["Access-Control-Allow-Methods"] = (
                    "GET, POST, PUT, DELETE, OPTIONS"
                )
                response.headers["Access-Control-Allow-Headers"] = (
                    "Content-Type, Authorization"
                )
                return response

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
    # Async operation routes  (POST/GET/DELETE /operations/<name_or_id>)
    # ------------------------------------------------------------------

    def _register_operation_routes(self, app: Flask) -> None:
        app.add_url_rule(
            "/operations/<name_or_id>",
            endpoint="operations",
            view_func=self._make_operations_handler(),
            methods=["GET", "POST", "DELETE"],
        )

    def _make_operations_handler(self):
        def handler(name_or_id: str):
            try:
                method = request.method
                if method == "POST":
                    body = request.get_json(force=True, silent=True) or {}
                    return self._handle_invoke_operation(name_or_id, body)
                if method == "GET":
                    return self._handle_get_operation(name_or_id)
                if method == "DELETE":
                    return self._handle_cancel_operation(name_or_id)
                return jsonify({"error": f"Method {method} not allowed"}), 405
            except Exception as exc:
                self._log_error("operations handler", exc)
                return jsonify({"error": str(exc)}), 500

        handler.__name__ = "operations_handler"
        return handler

    def _handle_invoke_operation(self, op_name: str, tool_input: dict):
        """POST /operations/<op_name> — run operation asynchronously, return operation record."""
        if op_name not in self.operations:
            return jsonify({"error": f"Operation '{op_name}' not found"}), 404

        op = self.operations[op_name]
        op_id = str(uuid.uuid4())
        record = _OperationRecord(op_id, op_name, tool_input)

        with self._op_records_lock:
            self._op_records[op_id] = record

        t = threading.Thread(
            target=self._run_op_in_background,
            args=(record, op),
            daemon=True,
            name=f"ada-op-{op_id[:8]}",
        )
        t.start()

        with self._op_records_lock:
            data = record.to_dict()
        return jsonify(data), 202

    def _handle_get_operation(self, op_id: str):
        """GET /operations/<op_id> — return status and metadata for an operation."""
        with self._op_records_lock:
            record = self._op_records.get(op_id)
            if record is None:
                return jsonify({"error": f"Operation '{op_id}' not found"}), 404
            data = record.to_dict()
        return jsonify(data)

    def _handle_cancel_operation(self, op_id: str):
        """DELETE /operations/<op_id> — request cancellation of a running or pending operation."""
        with self._op_records_lock:
            record = self._op_records.get(op_id)
            if record is None:
                return jsonify({"error": f"Operation '{op_id}' not found"}), 404
            if record.status in _OP_TERMINAL:
                return jsonify({"error": f"Cannot cancel operation with status '{record.status}'"}), 409
            record.cancellation_token.cancel()
            if record.status == _OP_PENDING:
                # Never started — mark terminal immediately.
                record.status = _OP_CANCELLED
                record.completed_at = _utcnow()
            data = record.to_dict()
        return jsonify(data)

    def _run_op_in_background(self, record: _OperationRecord, op: Operation) -> None:
        """Execute *op* in the calling thread, updating *record* throughout."""
        with self._op_records_lock:
            # If cancellation was requested between POST and thread start, skip.
            if record.status == _OP_CANCELLED:
                return
            record.status = _OP_RUNNING
            record.started_at = _utcnow()

        try:
            # Forward the cancellation token only if the operation declares a
            # second parameter — preserves backward compatibility with existing
            # Operation subclasses whose call() only accepts tool_input.
            sig = inspect.signature(op.call)
            accepts_token = len(sig.parameters) >= 2
            token = record.cancellation_token

            result = op.call(record.input, token) if accepts_token else op.call(record.input)

            with self._op_records_lock:
                if token.is_cancelled:
                    record.status = _OP_CANCELLED
                else:
                    record.status = _OP_COMPLETED
                    record.result = result
        except OperationCancelledError as exc:
            with self._op_records_lock:
                record.status = _OP_CANCELLED
                record.error = str(exc)
        except Exception as exc:
            self._log_error(f"Async operation '{record.operation_name}'", exc)
            with self._op_records_lock:
                record.status = _OP_FAILED
                record.error = str(exc)
        finally:
            with self._op_records_lock:
                if record.completed_at is None:
                    record.completed_at = _utcnow()

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
            field_specs = self._mcp_field_specs.get(type_name, {})
            for tool_type in MCPToolType:
                tools.append(_compute_data_type_tool_spec(data_type, tool_type, field_specs))
        for op in self.operations.values():
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


def _compute_data_type_tool_spec(
    data_type: type,
    tool_type: MCPToolType,
    field_specs: dict,
) -> dict:
    """
    Build the MCP tool spec dict for one (data_type, tool_type) combination.

    *field_specs* maps field names to ``{"description": "..."}`` dicts
    (from ``MCP_SPEC``).  Pass an empty dict for no descriptions.
    """
    type_name = data_type.__name__
    raw_schema = dataclass_to_json_schema(data_type)
    schema = _apply_field_descriptions(raw_schema, field_specs) if field_specs else raw_schema

    if tool_type == MCPToolType.UPSERT:
        return {
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
        }
    if tool_type == MCPToolType.DELETE:
        return {
            "name": f"{type_name}_delete",
            "description": f"Delete a {type_name} item by key.",
            "inputSchema": {
                "type": "object",
                "properties": {"key": {"type": "string"}},
                "required": ["key"],
            },
        }
    if tool_type == MCPToolType.GET:
        return {
            "name": f"{type_name}_get",
            "description": f"Retrieve a {type_name} item by key.",
            "inputSchema": {
                "type": "object",
                "properties": {"key": {"type": "string"}},
                "required": ["key"],
            },
        }
    if tool_type == MCPToolType.LIST:
        return {
            "name": f"{type_name}_list",
            "description": (
                f"List all {type_name} items. "
                "Optionally filter results using a jq expression applied to the "
                'array of {"key": ..., "data": {...}} objects.'
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
        }
    # MCPToolType.VALIDATE
    return {
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
                    **schema,
                    "description": f"The {type_name} payload to validate",
                },
            },
            "required": ["data"],
        },
    }


def _assert_mcp_spec_matches(data_type: type, mcp_spec: dict) -> None:
    """
    Assert that every key in *mcp_spec* is a valid field name on *data_type*.

    Raises:
        ValueError: If *mcp_spec* contains keys not present as fields.
    """
    field_names = {f.name for f in dataclasses.fields(data_type)}
    unknown = set(mcp_spec.keys()) - field_names
    if unknown:
        raise ValueError(
            f"MCP_SPEC contains fields not present in {data_type.__name__}: "
            f"{sorted(unknown)}. Valid fields: {sorted(field_names)}"
        )


def _apply_field_descriptions(schema: dict, field_specs: dict) -> dict:
    """
    Return a deep copy of *schema* with ``description`` values from
    *field_specs* applied to the matching ``properties`` entries.

    Only entries that include a ``"description"`` key in *field_specs* are
    modified; other properties are left untouched.
    """
    schema = copy.deepcopy(schema)
    properties = schema.get("properties", {})
    for field_name, spec in field_specs.items():
        if field_name in properties and "description" in spec:
            properties[field_name]["description"] = spec["description"]
    return schema


# ---------------------------------------------------------------------------
# Public factory
# ---------------------------------------------------------------------------

def init(
    data_backend: list[DataBackend] | DataBackend,
    config: Config | None = None,
) -> App:
    """
    Create and return an :class:`App` instance.

    After calling ``init``, register data types and operations by calling
    :meth:`App.add_data_type` and :meth:`App.add_operation` on the returned
    object before starting the server.

    Args:
        data_backend: One backend or a list of backends.  All write operations
                      are fanned out to every backend in the list; the first
                      backend is used for all reads.
        config:       Optional :class:`~abstract_data_app.Config`; defaults are
                      used if not provided.

    Returns:
        A configured :class:`App` instance ready for registration and serving.

    Example::

        app = abstract_data_app.init(
            data_backend=LocalSqliteDataBackend("store.db"),
        )
        app.add_data_type(MyDataType, MCP_SPEC={"name": {"description": "Item name"}})
        app.add_operation(MyOperation)
        app.serve_forever()
    """
    if isinstance(data_backend, DataBackend):
        data_backend = [data_backend]

    return App(
        data_backends=data_backend,
        config=config or Config(),
    )


def get_mcp_spec(
    *,
    data_type: Optional[type] = None,
    tool_type: Optional[MCPToolType] = None,
    operation=None,
) -> dict:
    """
    Compute the MCP tool spec for a data-type tool or an operation.

    This is a **module-level** function that works without an :class:`App`
    instance.  It produces the same result as :meth:`App.get_mcp_spec` when
    no ``MCP_SPEC`` field descriptions have been registered on the app.

    Pass either ``operation`` alone, or both ``data_type`` and ``tool_type``.

    Args:
        data_type: A Python ``@dataclass`` class (e.g. ``Product``).
        tool_type: A :class:`MCPToolType` value selecting which of the five
                   auto-generated tools to compute.
        operation: An :class:`~abstract_data_app.Operation` subclass (or
                   instance).  Returns its ``TOOL_SPEC`` directly; ``data_type``
                   and ``tool_type`` are ignored.

    Returns:
        The tool spec dict (``name``, ``description``, ``inputSchema``).

    Raises:
        ValueError: If neither ``operation`` nor both ``data_type`` +
                    ``tool_type`` are provided.

    Examples::

        # Compute a data-type tool spec without an app instance
        spec = abstract_data_app.get_mcp_spec(
            data_type=Product,
            tool_type=MCPToolType.UPSERT,
        )

        # Get an operation's TOOL_SPEC
        spec = abstract_data_app.get_mcp_spec(operation=ClaimOp)
    """
    if operation is not None:
        return operation.TOOL_SPEC
    if data_type is None or tool_type is None:
        raise ValueError(
            "Provide either `operation=` or both `data_type=` and `tool_type=`"
        )
    return _compute_data_type_tool_spec(data_type, tool_type, field_specs={})
