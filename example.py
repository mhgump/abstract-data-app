"""
Runnable example matching the target usage in the spec.

Start with:
    pip install flask jq
    python example.py

Then try:
    # Upsert
    curl -X PUT http://localhost:8000/data/DataType/item1 \
         -H 'Content-Type: application/json' \
         -d '{"field": "hello", "keys": ["a", "b"], "flags": {"active": true}}'

    # Get
    curl http://localhost:8000/data/DataType/item1

    # List with jq filter
    curl 'http://localhost:8000/data/DataType?filter=.[]|select(.data.flags.active==true)'

    # Delete
    curl -X DELETE http://localhost:8000/data/DataType/item1

    # MCP – list tools
    curl -X POST http://localhost:8000/mcp \
         -H 'Content-Type: application/json' \
         -d '{"jsonrpc":"2.0","id":1,"method":"tools/list"}'

    # MCP – validate
    curl -X POST http://localhost:8000/mcp \
         -H 'Content-Type: application/json' \
         -d '{"jsonrpc":"2.0","id":2,"method":"tools/call",
              "params":{"name":"DataType_validate",
                        "arguments":{"data":{"field":123,"keys":[],"flags":{}}}}}'
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any, Dict, List

import abstract_data_app
from abstract_data_app import LocalSqliteDataBackend, Operation


@dataclass
class DataType:
    field: str
    keys: List[str]
    flags: Dict[str, bool]


class Operation1(Operation):

    TOOL_SPEC = {
        "name": "operation1",
        "description": "Example operation that echoes its input",
        "inputSchema": {
            "type": "object",
            "properties": {
                "message": {"type": "string", "description": "Text to echo"},
            },
            "required": ["message"],
        },
    }

    def call(self, tool_input: Any) -> Any:
        # Operations may have side effects on data objects managed by the app.
        # Operations may return JSON data.
        return {"echoed": tool_input.get("message", "")}


app = abstract_data_app.init(
    data_backend=LocalSqliteDataBackend(":memory:"),
)
app.add_data_type(
    DataType,
    MCP_SPEC={
        "field": {"description": "A string value for this item"},
        "keys":  {"description": "List of string keys associated with the item"},
        "flags": {"description": "Boolean flags keyed by name"},
    },
)
app.add_operation(Operation1)

if __name__ == "__main__":
    app.serve_forever()
