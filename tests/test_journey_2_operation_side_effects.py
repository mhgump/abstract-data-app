"""
User journey 2 — An operation that reads and deletes a data item as a side-effect.

Story
-----
A developer needs a "claim" operation: given a key, return the stored record and
immediately remove it so no other caller can claim the same item.  The operation
is called via MCP tool invocation.

Steps
-----
1. Define a ``Record`` dataclass.
2. Define a ``ClaimOp`` operation that:
     a. Looks up the given key in the shared backend.
     b. If found, deletes the item and returns ``{found: true, data: {...}}``.
     c. If not found, returns ``{found: false, data: null}``.
   The operation captures the backend from the enclosing scope so it can reach
   the store directly — this is the intended pattern for operations with
   data-layer side effects.
3. Initialise the app with the same backend and start the server.
4. PUT /data/Record/rec1  — create a record.
5. Call the ``claim`` MCP tool with key ``rec1``:
   → expect found=True and the record's data returned.
6. Call the ``claim`` MCP tool with key ``rec1`` again:
   → the item was deleted in step 5; expect found=False.
7. Call the ``claim`` MCP tool with key ``nonexistent``:
   → expect found=False.
8. Confirm via GET /data/Record that the store is now empty.
"""

from dataclasses import dataclass
from typing import Any

import pytest

import abstract_data_app
from abstract_data_app import Config, LocalSqliteDataBackend, Operation

from conftest import Client, find_free_port, start_server


# ---------------------------------------------------------------------------
# Data type
# ---------------------------------------------------------------------------

@dataclass
class Record:
    value: str
    priority: int
    tags: list[str]


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture(scope="module")
def backend_and_client():
    """
    Return (backend, client) so that tests can inspect the backend directly
    and the ClaimOp closure can reference it.
    """
    backend = LocalSqliteDataBackend(":memory:")
    port = find_free_port()

    # The operation is defined here so it closes over ``backend``.
    class ClaimOp(Operation):
        TOOL_SPEC = {
            "name": "claim",
            "description": (
                "Return the Record stored under ``key`` and delete it atomically. "
                "If the key does not exist, returns {found: false, data: null}."
            ),
            "inputSchema": {
                "type": "object",
                "properties": {
                    "key": {"type": "string", "description": "Key of the record to claim"},
                },
                "required": ["key"],
            },
        }

        def call(self, tool_input: Any) -> Any:
            key = tool_input["key"]
            data = backend.get("Record", key)
            if data is not None:
                backend.delete("Record", key)
                return {"found": True, "data": data}
            return {"found": False, "data": None}

    app = abstract_data_app.init(
        data_backend=backend,
        config=Config(host="127.0.0.1", port=port, print_errors=True),
    )
    app.add_data_type(Record)
    app.add_operation(ClaimOp)
    start_server(app, port)
    return backend, Client(f"http://127.0.0.1:{port}")


# ---------------------------------------------------------------------------
# Journey
# ---------------------------------------------------------------------------

REC1_KEY = "rec1"
REC1 = {"value": "top-secret", "priority": 9, "tags": ["urgent", "confidential"]}


def test_step_4_create_record(backend_and_client):
    _, client = backend_and_client
    status, body = client.put(f"/data/Record/{REC1_KEY}", REC1)
    assert status == 200
    assert body["data"]["value"] == REC1["value"]


def test_step_5_claim_existing_key_returns_data_and_deletes(backend_and_client):
    backend, client = backend_and_client
    result = client.tool("claim", {"key": REC1_KEY})
    assert result["found"] is True
    assert result["data"]["value"] == REC1["value"]
    assert result["data"]["priority"] == REC1["priority"]
    assert result["data"]["tags"] == REC1["tags"]
    # Side-effect: the item should be gone from the backend
    assert backend.get("Record", REC1_KEY) is None


def test_step_6_claim_same_key_again_returns_not_found(backend_and_client):
    """Claiming a key a second time after it was deleted returns found=False."""
    _, client = backend_and_client
    result = client.tool("claim", {"key": REC1_KEY})
    assert result["found"] is False
    assert result["data"] is None


def test_step_7_claim_nonexistent_key_returns_not_found(backend_and_client):
    _, client = backend_and_client
    result = client.tool("claim", {"key": "nonexistent-key-xyz"})
    assert result["found"] is False
    assert result["data"] is None


def test_step_8_store_is_empty(backend_and_client):
    """After all claims the listing should be empty."""
    _, client = backend_and_client
    status, body = client.get("/data/Record")
    assert status == 200
    assert body["count"] == 0
