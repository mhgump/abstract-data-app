"""
abstract_data_app
=================

Build an HTTP CRUD API and an MCP server from Python dataclasses and
operation classes, backed by one or more pluggable data backends.

Quickstart::

    from dataclasses import dataclass
    from typing import List, Dict, Any
    import abstract_data_app
    from abstract_data_app import (
        Operation,
        LocalSqliteDataBackend,
        Config,
        MCPToolType,
    )


    @dataclass
    class Widget:
        name: str
        tags: List[str]
        meta: Dict[str, bool]


    class EchoOp(Operation):
        TOOL_SPEC = {
            "name": "echo",
            "description": "Echo the input back",
            "inputSchema": {
                "type": "object",
                "properties": {"message": {"type": "string"}},
                "required": ["message"],
            },
        }

        def call(self, tool_input: Any) -> Any:
            return {"echo": tool_input["message"]}


    app = abstract_data_app.init(
        data_backend=LocalSqliteDataBackend("widgets.db"),
    )
    app.add_data_type(Widget, MCP_SPEC={"name": {"description": "Widget display name"}})
    app.add_operation(EchoOp)
    app.serve_forever()

HTTP routes (per data type)
---------------------------
- ``GET    /data/<TypeName>/<key>``             — get item
- ``PUT    /data/<TypeName>/<key>``  (+ body)   — upsert item
- ``POST   /data/<TypeName>/<key>``  (+ body)   — upsert item
- ``DELETE /data/<TypeName>/<key>``             — delete item
- ``GET    /data/<TypeName>?filter=<jq>``       — list items (optional jq filter)

MCP endpoint
------------
``POST /mcp`` — JSON-RPC 2.0 (Streamable HTTP transport).

MCP tools (per data type)
-------------------------
- ``<TypeName>_upsert``   — upsert
- ``<TypeName>_delete``   — delete
- ``<TypeName>_get``      — get
- ``<TypeName>_list``     — list (optional jq_filter argument)
- ``<TypeName>_validate`` — validate a JSON payload; runs parallel field checks
                            then a dry-run upsert on every backend

MCP tools (per operation)
-------------------------
Each ``Operation.TOOL_SPEC["name"]`` is exposed as an MCP tool.

Programmatic tool inspection
-----------------------------
- ``app.list_mcp_tools()``                      — all registered tool specs
- ``app.get_mcp_spec(name, MCPToolType.UPSERT)`` — spec for one tool variant
"""

from .app import App, MCPToolType, init
from .backend import (
    DataBackend,
    LocalSqliteDataBackend,
    PostgresDataBackend,
    RedisDataBackend,
)
from .config import Config
from .operations import Operation

__all__ = [
    # Factory + App
    "init",
    "App",
    # MCP tool type enum
    "MCPToolType",
    # Config
    "Config",
    # Operation base
    "Operation",
    # Backends
    "DataBackend",
    "LocalSqliteDataBackend",
    "PostgresDataBackend",
    "RedisDataBackend",
]
