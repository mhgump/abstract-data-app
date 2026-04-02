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

import copy
import dataclasses
import json
import sys
import threading
import time
import traceback
from concurrent.futures import ThreadPoolExecutor
from enum import Enum
from typing import Any, Optional
from wsgiref.simple_server import WSGIRequestHandler, WSGIServer

from flask import Flask, jsonify, request

from .backend import DataBackend
from .config import Config
from .operations import Operation
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

    def get_mcp_spec(self, type_name: str, tool_type: MCPToolType) -> dict:
        """
        Return the MCP tool spec for one auto-generated data-type tool.

        Args:
            type_name: The ``__name__`` of a registered dataclass
                       (e.g. ``Product.__name__`` or ``"Product"``).
            tool_type: A :class:`MCPToolType` value selecting which of the
                       five auto-generated tools to retrieve.

        Returns:
            The tool spec dict (``name``, ``description``, ``inputSchema``).

        Raises:
            KeyError: If no matching tool is found.

        Example::

            spec = app.get_mcp_spec(Product.__name__, MCPToolType.UPSERT)
            print(spec["inputSchema"]["properties"]["data"]["properties"])
        """
        tool_name = f"{type_name}_{tool_type.value}"
        for tool in self._mcp_tools:
            if tool["name"] == tool_name:
                return tool
        available = [t["name"] for t in self._mcp_tools]
        raise KeyError(
            f"No MCP tool '{tool_name}' registered. Available tools: {available}"
        )

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
            raw_schema = dataclass_to_json_schema(data_type)
            field_specs = self._mcp_field_specs.get(type_name, {})
            schema = (
                _apply_field_descriptions(raw_schema, field_specs)
                if field_specs
                else raw_schema
            )

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
                            **schema,
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
