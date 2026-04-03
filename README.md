# abstract-data-app

A Python library that turns a set of dataclass types and operation classes into a running HTTP CRUD API and an MCP server — with zero boilerplate routing code.

You declare your data shapes and custom logic. The framework generates the server, all HTTP routes, all MCP tools, and schema-aware validation automatically.

---

## Table of contents

1. [Installation](#installation)
2. [Quickstart](#quickstart)
3. [Core concepts](#core-concepts)
4. [Defining data types](#defining-data-types)
5. [Annotating MCP fields with MCP_SPEC](#annotating-mcp-fields-with-mcp_spec)
6. [Defining operations](#defining-operations)
7. [HTTP API reference](#http-api-reference)
8. [jq filtering](#jq-filtering)
9. [MCP server reference](#mcp-server-reference)
10. [Programmatic tool inspection](#programmatic-tool-inspection)
11. [Programmatic data access](#programmatic-data-access)
12. [on_write callbacks](#on_write-callbacks)
13. [Validation tool](#validation-tool)
14. [Data backends](#data-backends)
15. [Multiple backends](#multiple-backends)
16. [Writing a custom backend](#writing-a-custom-backend)
17. [Configuration reference](#configuration-reference)
18. [Concurrency and thread safety](#concurrency-and-thread-safety)
19. [Error handling and reliability](#error-handling-and-reliability)
20. [Running tests](#running-tests)

---

## Installation

**Core dependencies** (Flask + jq):

```bash
pip install abstract-data-app
```

**With PostgreSQL support:**

```bash
pip install "abstract-data-app[postgres]"
```

**With Redis support:**

```bash
pip install "abstract-data-app[redis]"
```

**Everything:**

```bash
pip install "abstract-data-app[all]"
```

Requires Python 3.10+.

---

## Quickstart

```python
from dataclasses import dataclass
from typing import Any, Dict, List

import abstract_data_app
from abstract_data_app import LocalSqliteDataBackend, Operation


@dataclass
class Product:
    name: str
    price: float
    tags: List[str]
    in_stock: bool


class DiscountOp(Operation):
    TOOL_SPEC = {
        "name": "apply_discount",
        "description": "Apply a percentage discount to a product's price",
        "inputSchema": {
            "type": "object",
            "properties": {
                "key":      {"type": "string"},
                "percent":  {"type": "number"},
            },
            "required": ["key", "percent"],
        },
    }

    def call(self, tool_input: Any) -> Any:
        # Operations can close over the backend for side effects — see below.
        return {"message": f"Would discount {tool_input['key']} by {tool_input['percent']}%"}


app = abstract_data_app.init(
    data_backend=LocalSqliteDataBackend("store.db"),
)
app.add_data_type(
    Product,
    MCP_SPEC={
        "name":     {"description": "Display name of the product"},
        "price":    {"description": "Price in USD"},
        "in_stock": {"description": "Whether the item is available"},
    },
)
app.add_operation(DiscountOp)
app.serve_forever()
```

Once running, the server exposes:

- HTTP CRUD routes at `http://localhost:8000/data/Product/<key>`
- An MCP endpoint at `http://localhost:8000/mcp`

---

## Core concepts

| Concept | What it is |
|---|---|
| **Data type** | A Python `@dataclass`. The framework uses its field names and type annotations to generate HTTP routes, MCP tools, and a validator. |
| **Key** | A user-provided string that uniquely identifies one instance of a data type. Keys are not part of the dataclass itself; they are supplied separately in the URL or MCP tool call. |
| **Operation** | A subclass of `Operation` with a `TOOL_SPEC` dict and a `call()` method. Exposed only as an MCP tool, not as an HTTP route. |
| **Backend** | A storage implementation (`LocalSqliteDataBackend`, `PostgresDataBackend`, `RedisDataBackend`, or `HttpsDataBackend`). Multiple backends can be used simultaneously; all writes are fanned out to every backend. |
| **App** | The assembled server object, obtained from `abstract_data_app.init(...)`. Call `add_data_type()` and `add_operation()` to register types and operations, then `.serve_forever()` to start it. |
| **MCP_SPEC** | An optional dict passed to `add_data_type()` that maps field names to `{"description": "..."}` entries, enriching the auto-generated MCP tool schemas with human-readable field descriptions. |

---

## Defining data types

Register a `@dataclass` with `app.add_data_type()`. The framework reads its annotations to generate JSON Schemas for MCP tools and to drive the validation tool. Call `add_data_type` once per type, before starting the server.

```python
app.add_data_type(Product)
```

See [Annotating MCP fields with MCP_SPEC](#annotating-mcp-fields-with-mcp_spec) to enrich the generated schemas with field descriptions.

### Supported field types

| Python annotation | JSON Schema type | Validated as |
|---|---|---|
| `str` | `string` | `isinstance(v, str)` |
| `int` | `integer` | `isinstance(v, int)` (bools rejected) |
| `float` | `number` | `int` or `float` accepted (bools rejected) |
| `bool` | `boolean` | `isinstance(v, bool)` |
| `List[X]` | `array` with `items` | list, each element checked as `X` |
| `Dict[str, V]` | `object` with `additionalProperties` | dict, each value checked as `V` |
| `Optional[X]` / `X \| None` | type + `"null"` | value may be `None` or `X` |
| `Union[X, Y]` | `oneOf` | value must match at least one type |
| Nested `@dataclass` | nested `object` | recursively validated |
| `Any` | `{}` (unconstrained) | always passes |

### Example with all supported constructs

```python
from __future__ import annotations
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional

@dataclass
class Address:
    street: str
    city: str
    zip_code: Optional[str] = None   # optional field — omitting it is fine

@dataclass
class Person:
    name: str
    age: int
    email: Optional[str]
    addresses: List[Address]          # list of nested dataclasses
    scores: List[int]
    metadata: Dict[str, Any]          # open-ended dict
    active: bool = True               # has a default — optional in JSON payloads
```

### Fields with defaults

Fields that have a `default` or `default_factory` are **optional** in JSON payloads sent to the upsert route or validation tool. All other fields are required.

```python
@dataclass
class Config:
    key: str                           # required
    value: str                         # required
    ttl: int = 3600                    # optional — defaults to 3600
    tags: List[str] = field(default_factory=list)  # optional
```

---

## Annotating MCP fields with MCP_SPEC

Pass an optional `MCP_SPEC` dict to `add_data_type` to attach human-readable descriptions to the fields of the auto-generated MCP tool schemas.

```python
app.add_data_type(
    Product,
    MCP_SPEC={
        "name":     {"description": "Display name shown to customers"},
        "price":    {"description": "Unit price in USD"},
        "in_stock": {"description": "Set to false when inventory is zero"},
        # "tags" omitted — will have no description in the generated schema
    },
)
```

### Rules

- Every key in `MCP_SPEC` must be a field name on the dataclass. Passing an unknown key raises `ValueError` immediately.
- Fields omitted from `MCP_SPEC` are still included in the schema; they just have no description.
- Descriptions are applied to the `data` parameter of both the `_upsert` and `_validate` tool schemas.

### Inspecting the result

Use `get_mcp_spec` to retrieve the generated spec and confirm the descriptions:

```python
from abstract_data_app import MCPToolType

spec = app.get_mcp_spec(Product.__name__, MCPToolType.UPSERT)
props = spec["inputSchema"]["properties"]["data"]["properties"]
print(props["name"]["description"])   # "Display name shown to customers"
print(props["price"]["description"])  # "Unit price in USD"
print("description" in props["tags"]) # False — omitted from MCP_SPEC
```

---

## Defining operations

Register an operation with `app.add_operation()`. An operation is any subclass of `abstract_data_app.Operation` that:

1. Sets the `TOOL_SPEC` class variable to a valid MCP tool definition dict.
2. Implements the `call(self, tool_input)` method.

```python
from abstract_data_app import Operation
from typing import Any

class SearchOp(Operation):
    TOOL_SPEC = {
        "name": "search",
        "description": "Search products by name prefix",
        "inputSchema": {
            "type": "object",
            "properties": {
                "prefix": {"type": "string", "description": "Name prefix to search for"},
            },
            "required": ["prefix"],
        },
    }

    def call(self, tool_input: Any) -> Any:
        prefix = tool_input["prefix"].lower()
        # ... custom search logic ...
        return {"results": []}
```

`TOOL_SPEC` must be a dict with:

| Key | Required | Description |
|---|---|---|
| `name` | Yes | Unique tool name. Used as the MCP tool identifier. |
| `description` | Yes | Human-readable description shown to MCP clients. |
| `inputSchema` | Yes | JSON Schema for the arguments dict passed to `call()`. |

`call()` receives the `arguments` dict from the MCP `tools/call` request and must return any JSON-serialisable value. The framework wraps the return value in the MCP `content` array automatically.

### Giving operations access to the data store

Operations receive only their `tool_input` argument — there is no injected `app` or `backend` reference. The intended pattern is to **close over the backend** when defining the operation class:

```python
backend = LocalSqliteDataBackend("store.db")

class ClaimOp(Operation):
    """Atomically return and delete an item."""
    TOOL_SPEC = {
        "name": "claim",
        "description": "Return and delete an item by key",
        "inputSchema": {
            "type": "object",
            "properties": {"key": {"type": "string"}},
            "required": ["key"],
        },
    }

    def call(self, tool_input: Any) -> Any:
        key = tool_input["key"]
        data = backend.get("Order", key)     # read from the shared backend
        if data is not None:
            backend.delete("Order", key)     # side-effect: remove it
        return {"found": data is not None, "data": data}

app = abstract_data_app.init(data_backend=backend)  # same instance
app.add_data_type(Order)
app.add_operation(ClaimOp)
```

Pass the **same backend instance** to both `init()` and close over it in the operation so they share the same store.

---

## HTTP API reference

For every registered data type `TypeName`, the framework creates four routes.

### Upsert — create or update an item

```
PUT  /data/<TypeName>/<key>
POST /data/<TypeName>/<key>
```

**Request body:** JSON object whose fields match the dataclass. Required fields must be present; fields with defaults may be omitted.

**Response `200 OK`:**

```json
{
  "key": "item1",
  "data": { "name": "Widget", "price": 9.99, "tags": ["sale"], "in_stock": true }
}
```

**Example:**

```bash
curl -X PUT http://localhost:8000/data/Product/widget-1 \
     -H 'Content-Type: application/json' \
     -d '{"name": "Widget", "price": 9.99, "tags": ["sale"], "in_stock": true}'
```

---

### Get — retrieve a single item

```
GET /data/<TypeName>/<key>
```

**Response `200 OK`:**

```json
{
  "key": "widget-1",
  "data": { "name": "Widget", "price": 9.99, "tags": ["sale"], "in_stock": true }
}
```

**Response `404 Not Found`** when the key does not exist:

```json
{ "error": "Key 'widget-1' not found" }
```

---

### Delete — remove an item

```
DELETE /data/<TypeName>/<key>
```

**Response `200 OK`** when the key existed and was deleted:

```json
{ "deleted": true, "key": "widget-1" }
```

**Response `404 Not Found`** when the key did not exist:

```json
{ "deleted": false, "key": "widget-1" }
```

---

### List — all items of a type

```
GET /data/<TypeName>
GET /data/<TypeName>?filter=<jq-expression>
```

**Response `200 OK`:**

```json
{
  "items": [
    { "key": "widget-1", "data": { "name": "Widget", "price": 9.99, "tags": ["sale"], "in_stock": true } },
    { "key": "gadget-2", "data": { "name": "Gadget", "price": 24.99, "tags": [],      "in_stock": false } }
  ],
  "count": 2
}
```

When a `filter` query parameter is present, jq is applied before returning — see [jq filtering](#jq-filtering).

---

## jq filtering

The list endpoint accepts an optional `?filter=<jq-expression>` query parameter. The expression is applied to the full items array — each element is a `{"key": "...", "data": {...}}` object.

### Filter styles

Both styles produce the same result:

**Stream style** (`.[] | select(...)`): the expression emits matching items one by one.

```bash
# Products that are in stock
curl 'http://localhost:8000/data/Product?filter=.[]%20|%20select(.data.in_stock%20==%20true)'
```

**Array style** (`[.[] | select(...)]`): the expression wraps results in an array. Either style is fine; the framework normalises both to a flat list.

```bash
curl 'http://localhost:8000/data/Product?filter=[.[]|select(.data.price<10)]'
```

### Common filter examples

```bash
# Items where a boolean field is true
.[] | select(.data.in_stock == true)

# Items matching a specific string field
.[] | select(.data.category == "electronics")

# Items where the maximum value in a list field is ≥ 90
.[] | select(.data.scores | max >= 90)

# Items that have a specific tag in a list field
.[] | select(.data.tags | any(. == "sale"))

# Items where a nested object's field matches
.[] | select(.data.address.city == "London")

# Compound: in stock AND price under $20
.[] | select(.data.in_stock == true and .data.price < 20)

# Extract only the key and one field (transforms, not just filters)
.[] | {key: .key, name: .data.name}
```

The jq input is always the full `[{"key": ..., "data": {...}}, ...]` array. The result is whatever the jq expression emits — it does not have to be a list of the original items; it can be any jq transformation.

The `?filter=` value must be URL-encoded in practice. In Python:

```python
import urllib.parse
filter_expr = '.[] | select(.data.active == true)'
url = f'http://localhost:8000/data/Widget?filter={urllib.parse.quote(filter_expr)}'
```

---

## MCP server reference

The framework exposes a single HTTP endpoint that implements the [Model Context Protocol](https://modelcontextprotocol.io) Streamable HTTP transport (JSON-RPC 2.0 over HTTP POST).

**Default endpoint:** `POST http://localhost:8000/mcp`

The path is configurable via `Config.mcp_path`.

### Connecting an MCP client

Point any MCP client at the endpoint URL. The server supports the standard MCP handshake:

```json
// Client → Server
{ "jsonrpc": "2.0", "id": 1, "method": "initialize",
  "params": { "protocolVersion": "2024-11-05", "capabilities": {}, "clientInfo": {"name": "my-client"} } }

// Server → Client
{ "jsonrpc": "2.0", "id": 1, "result": {
    "protocolVersion": "2024-11-05",
    "capabilities": { "tools": {} },
    "serverInfo": { "name": "abstract-data-app", "version": "0.1.0" }
}}

// Client → Server (notification, no response expected)
{ "jsonrpc": "2.0", "method": "notifications/initialized" }
```

### Listing available tools

```json
// Request
{ "jsonrpc": "2.0", "id": 2, "method": "tools/list" }

// Response — tools array contains all auto-generated + operation tools
{ "jsonrpc": "2.0", "id": 2, "result": { "tools": [ ... ] } }
```

### Calling a tool

```json
// Request
{ "jsonrpc": "2.0", "id": 3, "method": "tools/call",
  "params": { "name": "Product_upsert", "arguments": { "key": "w1", "data": { ... } } } }

// Response
{ "jsonrpc": "2.0", "id": 3, "result": {
    "content": [ { "type": "text", "text": "{ \"key\": \"w1\", ... }" } ]
}}
```

The `content[0].text` field is always a JSON string of the operation's return value. On error, the result also contains `"isError": true`.

---

### Auto-generated data type tools

For every registered data type `TypeName`, five MCP tools are generated automatically.

#### `TypeName_upsert`

Insert or update an item.

```json
{
  "name": "Product_upsert",
  "arguments": {
    "key": "widget-1",
    "data": { "name": "Widget", "price": 9.99, "tags": ["sale"], "in_stock": true }
  }
}
```

Returns: `{"key": "...", "data": {...}}`

---

#### `TypeName_get`

Retrieve a single item by key.

```json
{ "name": "Product_get", "arguments": { "key": "widget-1" } }
```

Returns `{"found": true, "key": "...", "data": {...}}` when the key exists, or `{"found": false, "key": "..."}` when it does not.

---

#### `TypeName_delete`

Delete an item by key.

```json
{ "name": "Product_delete", "arguments": { "key": "widget-1" } }
```

Returns `{"deleted": true, "key": "..."}` when the key existed, or `{"deleted": false, "key": "..."}` when it did not.

---

#### `TypeName_list`

List all items, with an optional jq filter.

```json
{ "name": "Product_list", "arguments": {} }
// or with a filter:
{ "name": "Product_list", "arguments": { "jq_filter": ".[] | select(.data.in_stock == true)" } }
```

Returns: a JSON array of `{"key": "...", "data": {...}}` objects (filtered if `jq_filter` was provided). The same jq rules as the HTTP list endpoint apply.

---

#### `TypeName_validate`

Validate a JSON payload as this type without storing it. See [Validation tool](#validation-tool).

---

### Operation tools

Every `Operation` subclass is exposed as an MCP tool using the name and schema from its `TOOL_SPEC`. The tool name is `TOOL_SPEC["name"]`.

```json
{ "name": "apply_discount", "arguments": { "key": "widget-1", "percent": 10 } }
```

The return value of `call()` is JSON-serialised into `content[0].text`.

---

## Programmatic tool inspection

After registering data types and operations you can inspect the generated MCP tool specs without starting the server.

### `app.list_mcp_tools()`

Returns a list of all registered MCP tool spec dicts — the same payload served by the `tools/list` MCP method.

```python
tools = app.list_mcp_tools()
for tool in tools:
    print(tool["name"], "—", tool["description"])
```

### `app.get_mcp_spec(type_name, tool_type)`

Returns the spec for one specific auto-generated tool.

```python
from abstract_data_app import MCPToolType

spec = app.get_mcp_spec(Product.__name__, MCPToolType.UPSERT)
# spec is a dict: {"name": "Product_upsert", "description": "...", "inputSchema": {...}}
```

`MCPToolType` values:

| Value | Tool name suffix | Description |
|---|---|---|
| `MCPToolType.UPSERT` | `_upsert` | Insert or update by key |
| `MCPToolType.DELETE` | `_delete` | Delete by key |
| `MCPToolType.GET` | `_get` | Retrieve by key |
| `MCPToolType.LIST` | `_list` | List all items (optional jq filter) |
| `MCPToolType.VALIDATE` | `_validate` | Validate a payload without storing it |

Raises `KeyError` if no matching tool is found.

---

## Programmatic data access

The `App` object exposes a full CRUD API that works **without starting an HTTP server**. This is useful for scripts, tests, and any code that wants to use the storage layer directly from Python.

All five methods can be called as soon as `add_data_type()` has been called — no call to `serve_forever()` is required.

### `app.upsert(data_type, key, data)`

Insert or update an item. `data` may be a dataclass instance or a plain `dict`.

```python
from dataclasses import dataclass
import abstract_data_app
from abstract_data_app import LocalSqliteDataBackend

@dataclass
class Widget:
    name: str
    price: float

app = abstract_data_app.init(data_backend=LocalSqliteDataBackend(":memory:"))
app.add_data_type(Widget)

# From a dataclass instance
result = app.upsert(Widget, "w1", Widget(name="Cog", price=4.99))
# result == {"key": "w1", "data": {"name": "Cog", "price": 4.99}}

# From a plain dict
result = app.upsert(Widget, "w2", {"name": "Sprocket", "price": 2.49})
```

Returns `{"key": key, "data": <data dict>}`.

---

### `app.get(data_type, key)`

Retrieve one item. Returns a **dataclass instance**, or `None` if the key does not exist.

```python
widget = app.get(Widget, "w1")
# widget is a Widget instance (not a dict)
print(widget.name)   # "Cog"
print(widget.price)  # 4.99

missing = app.get(Widget, "no-such-key")
# missing is None
```

---

### `app.delete(data_type, key)`

Delete one item. Returns `True` if the key existed, `False` if it did not.

```python
existed = app.delete(Widget, "w1")  # True
existed = app.delete(Widget, "w1")  # False — already gone
```

---

### `app.list(data_type)`

List all items of a type. Returns a list of `{"key": str, "data": <dataclass instance>}` dicts.

```python
items = app.list(Widget)
# [
#   {"key": "w2", "data": Widget(name="Sprocket", price=2.49)},
# ]
for item in items:
    print(item["key"], item["data"].price)
```

---

### `app.call(operation_name, tool_input)`

Invoke a registered operation by its `TOOL_SPEC["name"]`. This is the programmatic equivalent of an MCP `tools/call` request.

```python
class DiscountOp(Operation):
    TOOL_SPEC = {
        "name": "apply_discount",
        "description": "Apply a percentage discount",
        "inputSchema": {
            "type": "object",
            "properties": {
                "key":     {"type": "string"},
                "percent": {"type": "number"},
            },
            "required": ["key", "percent"],
        },
    }

    def call(self, tool_input):
        return {"discounted_price": 4.99 * (1 - tool_input["percent"] / 100)}

app.add_operation(DiscountOp)
result = app.call("apply_discount", {"key": "w1", "percent": 10})
# result == {"discounted_price": 4.491}
```

Raises `KeyError` for any method if the data type or operation has not been registered.

---

## on_write callbacks

Register a function to be called every time a write (upsert or delete) succeeds for a given data type.  Callbacks fire from **every** write path — the programmatic API, the HTTP routes, and the MCP tools — because they all share the same internal write logic.

### `app.add_on_write_callback(data_type, on_write)`

```python
def on_write(operation: str, key: str, data: dict | None) -> None:
    ...
```

| Argument | Type | Value |
|---|---|---|
| `operation` | `str` | `"upsert"` or `"delete"` |
| `key` | `str` | The item key |
| `data` | `dict \| None` | Written data dict for upserts; `None` for deletes |

The type must already be registered with `add_data_type()` before a callback can be attached.  The method returns `self` for chaining.

### Basic example — audit log

```python
from dataclasses import dataclass
import abstract_data_app
from abstract_data_app import LocalSqliteDataBackend

@dataclass
class Order:
    product: str
    quantity: int
    shipped: bool

def audit_log(operation: str, key: str, data: dict | None) -> None:
    if operation == "upsert":
        print(f"[upsert] {key} → {data}")
    else:
        print(f"[delete] {key}")

app = abstract_data_app.init(data_backend=LocalSqliteDataBackend("orders.db"))
app.add_data_type(Order)
app.add_on_write_callback(Order, audit_log)
app.serve_forever()
```

### Multiple callbacks

Multiple callbacks can be registered for the same type.  They are called in registration order.

```python
app.add_on_write_callback(Order, send_to_audit_log)
app.add_on_write_callback(Order, invalidate_cache)
app.add_on_write_callback(Order, publish_event)
```

### Type isolation

Callbacks are scoped to the type they are registered on.  Writes to `Order` never fire `Invoice` callbacks, and vice-versa.

```python
app.add_on_write_callback(Order, on_order_write)
app.add_on_write_callback(Invoice, on_invoice_write)
```

### Method chaining

`add_on_write_callback` returns `self`, so it can be chained with other registration methods:

```python
app = (
    abstract_data_app.init(data_backend=LocalSqliteDataBackend(":memory:"))
    .add_data_type(Order)
    .add_on_write_callback(Order, audit_log)
    .add_data_type(Invoice)
    .add_on_write_callback(Invoice, audit_log)
)
```

### Error handling

If a callback raises an exception, the error is logged to `stderr` (respecting `Config.print_errors`) and the remaining callbacks in the chain still run.  The write is considered successful regardless — a misbehaving callback never causes the write to appear to fail from the caller's perspective.

### Thread safety

Callbacks are invoked **after** the per-type write lock is released, so they may safely call back into the app (e.g. to read related data) without risking a deadlock.  If multiple threads upsert concurrently, each write fires its callbacks independently.

---

## Validation tool

Each data type gets a `TypeName_validate` MCP tool that checks a payload without writing it to the store. It runs two phases:

**Phase 1 — parallel field validation.** Each field is checked against its type annotation in a separate thread. All errors are collected before returning, so a single call surfaces all problems at once.

**Phase 2 — backend dry-run (only if phase 1 passes).** The payload is attempted against every configured backend using a rolled-back transaction (or equivalent). This catches backend-level constraints such as serialisation failures.

**Request:**

```json
{
  "name": "Product_validate",
  "arguments": {
    "data": { "name": "Widget", "price": "not-a-number", "tags": [1, 2], "in_stock": true }
  }
}
```

**Response (errors found):**

```json
{
  "valid": false,
  "errors": [
    "Field 'price': expected float, got str",
    "Field 'tags': item[0]: expected str, got int"
  ]
}
```

**Response (clean):**

```json
{ "valid": true, "errors": [] }
```

Validation rules by type:

- `str` — value must be a string.
- `int` — value must be an integer; booleans are **rejected** (Python's `bool` is a subclass of `int`, but the validator treats them as distinct).
- `float` — integers are accepted (widening); booleans are rejected.
- `bool` — value must be exactly `true` or `false`.
- `List[X]` — value must be a JSON array; each element is validated as `X`. The error message includes the failing index, e.g. `item[2]: expected str, got int`.
- `Dict[str, V]` — value must be a JSON object; each value is validated as `V`. The error includes the key name.
- Nested dataclass — value must be a JSON object and is recursively validated. Errors are prefixed with the field name.
- `Optional[X]` — `null` is always accepted; non-null values are validated as `X`.
- Fields with unknown or unrecognised type hints pass without error (lenient mode).

---

## Data backends

A backend is any class that implements the `DataBackend` abstract base class. Four implementations are included.

### `LocalSqliteDataBackend`

SQLite file or in-memory database. Best for development, testing, and single-process deployments.

```python
from abstract_data_app import LocalSqliteDataBackend

# Persistent file
backend = LocalSqliteDataBackend("myapp.db")

# Ephemeral in-memory (resets on process restart; useful for tests)
backend = LocalSqliteDataBackend(":memory:")
```

All data for every registered type is stored in one table:

```sql
CREATE TABLE items (
    type_name TEXT NOT NULL,
    key       TEXT NOT NULL,
    data      TEXT NOT NULL,   -- JSON-serialised payload
    PRIMARY KEY (type_name, key)
)
```

Thread safety: a single connection is shared across all threads and protected by a `threading.RLock`. WAL journal mode is enabled for file-based databases.

---

### `PostgresDataBackend`

PostgreSQL via `psycopg2`. Requires the optional dependency:

```bash
pip install "abstract-data-app[postgres]"
```

```python
from abstract_data_app import PostgresDataBackend

backend = PostgresDataBackend("postgresql://user:password@localhost:5432/mydb")
```

The DSN is any [libpq connection string](https://www.postgresql.org/docs/current/libpq-connect.html#LIBPQ-CONNSTRING). The table `abstract_data_app_items` is created automatically on first use.

Thread safety: a separate psycopg2 connection is opened per thread using `threading.local`.

---

### `RedisDataBackend`

Redis via `redis-py`. Requires the optional dependency:

```bash
pip install "abstract-data-app[redis]"
```

```python
from abstract_data_app import RedisDataBackend

# Default local Redis
backend = RedisDataBackend()

# Custom host/port/db
backend = RedisDataBackend(host="redis.example.com", port=6380, db=1)

# Any keyword argument accepted by redis.Redis (password, ssl, etc.)
backend = RedisDataBackend(host="redis.example.com", password="secret", ssl=True)
```

Data is stored under keys with the prefix `abstract_data_app:data:<TypeName>:<key>`. A Redis Set at `abstract_data_app:index:<TypeName>` tracks which keys exist for each type. The redis-py client uses a built-in connection pool, which is thread-safe.

**Note:** The Redis dry-run validation only checks JSON serialisability, not a real transaction rollback, because Redis does not support nested transactions.

---

### `HttpsDataBackend`

Proxies every operation to a **remote abstract-data-app instance** over HTTP or HTTPS. No extra dependencies — uses Python's standard library `urllib`.

```python
from abstract_data_app import HttpsDataBackend

backend = HttpsDataBackend("https://myserver.example.com")
# or for local development:
backend = HttpsDataBackend("http://localhost:8000")
```

Each `DataBackend` method maps to the corresponding remote HTTP route:

| Method | Remote route |
|---|---|
| `upsert` | `PUT /data/<TypeName>/<key>` |
| `delete` | `DELETE /data/<TypeName>/<key>` |
| `get` | `GET /data/<TypeName>/<key>` |
| `list_all` | `GET /data/<TypeName>` |
| `dry_run_upsert` | not supported — always returns `None` |

**Typical use case:** run a lightweight local app that delegates all storage to a shared remote server.

```python
remote = HttpsDataBackend("https://shared-store.example.com")
app = abstract_data_app.init(data_backend=remote)
app.add_data_type(Widget)
app.serve_forever()   # all reads/writes go to the remote server
```

`HttpsDataBackend` can also be combined with local backends in a multi-backend setup — for example, to keep a local SQLite cache while writing through to a remote server (see [Multiple backends](#multiple-backends)).

---

## Multiple backends

Pass a list to `data_backend`. Every **write** (upsert and delete) is fanned out to all backends in order. The first backend in the list is used as the **primary** for all reads (get and list).

```python
from abstract_data_app import LocalSqliteDataBackend, PostgresDataBackend

sqlite  = LocalSqliteDataBackend("local_cache.db")
postgres = PostgresDataBackend("postgresql://user:pass@db-host/mydb")

app = abstract_data_app.init(
    data_backend=[sqlite, postgres],   # sqlite is primary (reads); both get writes
)
app.add_data_type(Product)
```

If any backend raises an exception during a write, the error is collected and a `RuntimeError` is raised after all backends have been attempted. The error message identifies which backends failed.

---

## Writing a custom backend

Subclass `DataBackend` and implement the five abstract methods:

```python
from abstract_data_app import DataBackend
from typing import Any, Optional

class MyBackend(DataBackend):

    def upsert(self, type_name: str, key: str, data: dict[str, Any]) -> None:
        """Store data under (type_name, key). Overwrite if already present."""
        ...

    def delete(self, type_name: str, key: str) -> bool:
        """Remove (type_name, key). Return True if it existed, False otherwise."""
        ...

    def get(self, type_name: str, key: str) -> Optional[dict[str, Any]]:
        """Return the data dict for (type_name, key), or None if not found."""
        ...

    def list_all(self, type_name: str) -> list[dict[str, Any]]:
        """Return all items of type_name as [{"key": ..., "data": {...}}, ...]."""
        ...

    def dry_run_upsert(
        self, type_name: str, key: str, data: dict[str, Any]
    ) -> Optional[str]:
        """
        Attempt an upsert without committing it.
        Return an error string if it would fail, or None if it would succeed.
        Called by the validation tool (phase 2) only when field validation passes.
        """
        ...
```

Then pass an instance to `init()` and register your data types:

```python
app = abstract_data_app.init(data_backend=MyBackend(...))
app.add_data_type(MyType)
```

---

## Configuration reference

Pass a `Config` instance to `init()` to customise server behaviour. All fields have defaults.

```python
from abstract_data_app import Config

app = abstract_data_app.init(
    data_backend=...,
    config=Config(
        host="0.0.0.0",
        port=8000,
        num_threads=8,
        debug=False,
        print_errors=True,
        mcp_path="/mcp",
    ),
)
```

| Field | Type | Default | Description |
|---|---|---|---|
| `host` | `str` | `"0.0.0.0"` | Network interface to bind. Use `"127.0.0.1"` to accept only local connections. |
| `port` | `int` | `8000` | TCP port to listen on. |
| `num_threads` | `int` | `8` | Maximum number of concurrent requests. Each request is handled by a thread from a fixed-size pool. |
| `debug` | `bool` | `False` | Enable Flask debug mode. **Do not use in production** — it disables the thread pool and enables the Werkzeug debugger. |
| `print_errors` | `bool` | `True` | Print full tracebacks to `stderr` for every unhandled exception. Set to `False` to suppress noise in production if you have external log aggregation. |
| `mcp_path` | `str` | `"/mcp"` | URL path for the MCP JSON-RPC endpoint. Change this if the default conflicts with another route. |

---

## Concurrency and thread safety

### Request handling

`serve_forever()` starts a WSGI server backed by a `ThreadPoolExecutor` sized to `Config.num_threads`. Each incoming HTTP request is handled by one thread from the pool. Requests beyond the pool capacity are queued by the OS.

### Write serialisation

For every registered data type, the framework holds one `threading.Lock`. All write operations (upsert and delete) acquire this lock before fanning out to backends. This means:

- Concurrent writes to the **same type** are serialised.
- Concurrent writes to **different types** proceed in parallel.
- Reads (get and list) never acquire a lock.

This prevents interleaved multi-backend writes where backend A gets a new value but backend B still has the old one midway through a fan-out.

### Backend thread safety

Each built-in backend is independently thread-safe:

| Backend | Mechanism |
|---|---|
| `LocalSqliteDataBackend` | Single connection + `threading.RLock` |
| `PostgresDataBackend` | `threading.local` — one psycopg2 connection per thread |
| `RedisDataBackend` | redis-py's built-in connection pool |

---

## Error handling and reliability

### Per-request errors

Every HTTP route handler and every MCP tool call wraps its body in a `try/except`. An unhandled exception in application code returns an HTTP 500 or an MCP result with `"isError": true` rather than crashing the server process. The error message is included in the response body.

With `Config.print_errors=True` (the default), a full Python traceback is printed to `stderr` alongside the error.

### Server-level errors

`serve_forever()` runs in an infinite loop. If the WSGI server itself raises an unexpected exception (e.g. port already in use, OS-level socket error), the error is printed and the loop sleeps for one second before attempting to restart. The process only exits on `KeyboardInterrupt` (Ctrl-C).

This means the process stays alive even under unusual failure conditions, and will resume handling requests as soon as the underlying problem clears.

---

## Running tests

The test suite uses pytest with four user-journey test files. Each test file starts its own server on a free port using a module-scoped fixture, so all suites can run in parallel without conflicts.

```bash
poetry run pytest tests/ -v
```

Example output:

```
tests/test_journey_1_crud_list_delete.py::test_step_3_upsert_book1            PASSED
...
tests/test_journey_4_mcp_spec.py::test_step_10_crud_round_trip                PASSED
34 passed in 0.41s
```

To test your own code the same way, use `abstract_data_app.init()` as normal, then access `app._flask` for the Flask test client or start a real server on a free port:

```python
import socket, threading, time
import abstract_data_app
from abstract_data_app import LocalSqliteDataBackend, Config

def find_free_port():
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]

port = find_free_port()
app = abstract_data_app.init(
    data_backend=LocalSqliteDataBackend(":memory:"),
    config=Config(host="127.0.0.1", port=port),
)
app.add_data_type(MyType)

thread = threading.Thread(
    target=app._flask.run,
    kwargs={"host": "127.0.0.1", "port": port, "use_reloader": False},
    daemon=True,
)
thread.start()
time.sleep(0.3)  # wait for Flask to bind

# now make HTTP requests to http://127.0.0.1:<port>
```
