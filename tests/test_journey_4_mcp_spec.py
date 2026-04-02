"""
User journey 4 — Register a data type with an MCP_SPEC and inspect the
generated tool specs programmatically.

Story
-----
A developer wants field-level descriptions in the auto-generated MCP tools for
their ``Product`` type.  They supply an ``MCP_SPEC`` when calling
``add_data_type``, then use ``get_mcp_spec`` and ``list_mcp_tools`` to confirm
that the generated specs reflect their annotations exactly.  Finally they start
the server and verify a normal CRUD round-trip still works.

Steps
-----
1.  Define a ``Product`` dataclass with four fields:
      name (str), price (float), tags (List[str]), in_stock (bool).
2.  Create the app and call add_data_type with an MCP_SPEC covering three of
    the four fields (``name``, ``price``, ``in_stock`` — ``tags`` is omitted).
3.  Assert that passing an MCP_SPEC with an unknown field raises ValueError.
4.  Call get_mcp_spec(Product.__name__, MCPToolType.UPSERT).
5.  Assert the tool name is "Product_upsert".
6.  Assert the inputSchema "data" property has the expected required fields.
7.  Assert each MCP_SPEC field carries the provided description.
8.  Assert the field omitted from MCP_SPEC (``tags``) has no description.
9.  Call list_mcp_tools() and confirm all five Product tools are present.
10. Start the server, upsert a product via HTTP, and confirm a GET returns it.
"""

from dataclasses import dataclass
from typing import List

import pytest

import abstract_data_app
from abstract_data_app import Config, LocalSqliteDataBackend, MCPToolType

from conftest import Client, find_free_port, start_server


# ---------------------------------------------------------------------------
# Data type
# ---------------------------------------------------------------------------

@dataclass
class Product:
    name: str
    price: float
    tags: List[str]
    in_stock: bool


# ---------------------------------------------------------------------------
# MCP_SPEC used throughout this journey
# ---------------------------------------------------------------------------

PRODUCT_MCP_SPEC = {
    "name":     {"description": "Display name of the product"},
    "price":    {"description": "Price in USD"},
    "in_stock": {"description": "Whether the item is currently available"},
    # "tags" is intentionally omitted — should have no description
}


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture(scope="module")
def app_and_client():
    port = find_free_port()
    app = abstract_data_app.init(
        data_backend=LocalSqliteDataBackend(":memory:"),
        config=Config(host="127.0.0.1", port=port, print_errors=True),
    )
    app.add_data_type(Product, MCP_SPEC=PRODUCT_MCP_SPEC)
    start_server(app, port)
    return app, Client(f"http://127.0.0.1:{port}")


# ---------------------------------------------------------------------------
# Journey
# ---------------------------------------------------------------------------

def test_step_3_unknown_mcp_spec_field_raises():
    """MCP_SPEC with a key that is not a dataclass field must raise ValueError."""
    app = abstract_data_app.init(data_backend=LocalSqliteDataBackend(":memory:"))
    with pytest.raises(ValueError, match="not present in Product"):
        app.add_data_type(
            Product,
            MCP_SPEC={"nonexistent_field": {"description": "oops"}},
        )


def test_step_4_get_mcp_spec_returns_upsert_tool(app_and_client):
    app, _ = app_and_client
    spec = app.get_mcp_spec(Product.__name__, MCPToolType.UPSERT)
    assert spec is not None


def test_step_5_upsert_tool_name(app_and_client):
    app, _ = app_and_client
    spec = app.get_mcp_spec(Product.__name__, MCPToolType.UPSERT)
    assert spec["name"] == "Product_upsert"


def test_step_6_upsert_tool_required_fields(app_and_client):
    """The upsert inputSchema must require both 'key' and 'data'."""
    app, _ = app_and_client
    spec = app.get_mcp_spec(Product.__name__, MCPToolType.UPSERT)
    input_schema = spec["inputSchema"]
    assert "key" in input_schema["required"]
    assert "data" in input_schema["required"]

    data_schema = input_schema["properties"]["data"]
    # All four Product fields are required (none have defaults)
    assert set(data_schema["required"]) == {"name", "price", "tags", "in_stock"}


def test_step_7_mcp_spec_descriptions_are_set(app_and_client):
    """Fields listed in MCP_SPEC must carry the provided descriptions."""
    app, _ = app_and_client
    spec = app.get_mcp_spec(Product.__name__, MCPToolType.UPSERT)
    props = spec["inputSchema"]["properties"]["data"]["properties"]

    assert props["name"]["description"] == PRODUCT_MCP_SPEC["name"]["description"]
    assert props["price"]["description"] == PRODUCT_MCP_SPEC["price"]["description"]
    assert props["in_stock"]["description"] == PRODUCT_MCP_SPEC["in_stock"]["description"]


def test_step_8_omitted_field_has_no_description(app_and_client):
    """``tags`` was not in MCP_SPEC so its schema entry must not have a description."""
    app, _ = app_and_client
    spec = app.get_mcp_spec(Product.__name__, MCPToolType.UPSERT)
    props = spec["inputSchema"]["properties"]["data"]["properties"]
    assert "description" not in props["tags"]


def test_step_9_list_mcp_tools_contains_all_five_product_tools(app_and_client):
    app, _ = app_and_client
    tools = app.list_mcp_tools()
    tool_names = {t["name"] for t in tools}
    expected = {
        "Product_upsert",
        "Product_delete",
        "Product_get",
        "Product_list",
        "Product_validate",
    }
    assert expected.issubset(tool_names)


def test_step_10_crud_round_trip(app_and_client):
    """Upsert a product via HTTP and confirm GET returns it unchanged."""
    _, client = app_and_client
    product = {"name": "Widget", "price": 9.99, "tags": ["sale", "new"], "in_stock": True}

    status, body = client.put("/data/Product/widget-1", product)
    assert status == 200
    assert body["key"] == "widget-1"

    status, body = client.get("/data/Product/widget-1")
    assert status == 200
    assert body["data"]["name"] == product["name"]
    assert body["data"]["price"] == product["price"]
    assert body["data"]["tags"] == product["tags"]
    assert body["data"]["in_stock"] == product["in_stock"]
