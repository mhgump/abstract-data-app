"""
User journey 5 — Cancelable async operations.

Story
-----
A developer needs to run long-running operations via HTTP, monitor their
progress, and be able to cancel them before they finish.  The new
``POST /operations/<name>`` route starts an operation asynchronously and
returns an operation ID immediately.  ``GET /operations/<id>`` checks
status, and ``DELETE /operations/<id>`` cancels a pending or running op.

Steps
-----
1. Define a ``SlowOp`` that sleeps in a loop and checks a cancellation token
   cooperatively, and a ``FastOp`` that returns immediately.
2. POST /operations/fast_op → 202, get operation_id; poll until completed,
   verify result is present.
3. POST /operations/slow_op → 202, get operation_id; status is pending or
   running (not completed yet).
4. GET /operations/<slow_id> → status is pending or running.
5. DELETE /operations/<slow_id> → cancels the op; response status is
   "pending" or "cancelled" (depending on race), cancellation_token is set.
6. Poll GET /operations/<slow_id> until terminal; assert final status is
   "cancelled".
7. DELETE /operations/<slow_id> again → 409 (already cancelled).
8. POST /operations/nonexistent → 404.
9. GET /operations/nonexistent-id → 404.
"""

import time
from typing import Any

import pytest

import abstract_data_app
from abstract_data_app import (
    CancellationToken,
    Config,
    LocalSqliteDataBackend,
    Operation,
    OperationCancelledError,
)

from conftest import Client, find_free_port, start_server


# ---------------------------------------------------------------------------
# Operations
# ---------------------------------------------------------------------------

class FastOp(Operation):
    TOOL_SPEC = {
        "name": "fast_op",
        "description": "Returns immediately with a simple result.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "value": {"type": "integer"},
            },
            "required": ["value"],
        },
    }

    def call(self, tool_input: Any) -> Any:
        return {"doubled": tool_input.get("value", 0) * 2}


class SlowOp(Operation):
    """Sleeps in small increments and checks the cancellation token each time."""

    TOOL_SPEC = {
        "name": "slow_op",
        "description": "Sleeps for a long time, but supports cooperative cancellation.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "duration_s": {"type": "number"},
            },
        },
    }

    def call(self, tool_input: Any, cancellation_token: CancellationToken = None) -> Any:
        duration = tool_input.get("duration_s", 30)
        deadline = time.monotonic() + duration
        while time.monotonic() < deadline:
            if cancellation_token:
                cancellation_token.raise_if_cancelled()
            time.sleep(0.05)
        return {"completed": True}


# ---------------------------------------------------------------------------
# Fixture
# ---------------------------------------------------------------------------

@pytest.fixture(scope="module")
def client():
    backend = LocalSqliteDataBackend(":memory:")
    port = find_free_port()
    app = abstract_data_app.init(
        data_backend=backend,
        config=Config(host="127.0.0.1", port=port, print_errors=False),
    )
    app.add_operation(FastOp)
    app.add_operation(SlowOp)
    start_server(app, port)
    return Client(f"http://127.0.0.1:{port}")


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _poll_until_terminal(client: Client, op_id: str, timeout: float = 5.0) -> dict:
    """Poll GET /operations/<op_id> until status is a terminal state."""
    terminal = {"completed", "failed", "cancelled"}
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        status, body = client.get(f"/operations/{op_id}")
        assert status == 200, f"Unexpected status {status}: {body}"
        if body["status"] in terminal:
            return body
        time.sleep(0.05)
    raise TimeoutError(f"Operation {op_id} did not reach terminal state within {timeout}s")


# ---------------------------------------------------------------------------
# Journey
# ---------------------------------------------------------------------------

def test_step_2_fast_op_completes_with_result(client):
    """POST a fast operation, poll until complete, verify result."""
    status, body = client.request("POST", "/operations/fast_op", {"value": 21})
    assert status == 202
    assert "operation_id" in body
    assert body["operation_name"] == "fast_op"
    assert body["status"] in ("pending", "running", "completed")

    final = _poll_until_terminal(client, body["operation_id"])
    assert final["status"] == "completed"
    assert final["result"] == {"doubled": 42}
    assert final["completed_at"] is not None


def test_step_3_slow_op_starts_and_is_not_immediately_done(client):
    """POST a slow operation and confirm it hasn't finished right away."""
    status, body = client.request("POST", "/operations/slow_op", {"duration_s": 30})
    assert status == 202
    assert body["status"] in ("pending", "running")
    # Store the id for later tests via a module-level dict
    _slow_op_id_store["id"] = body["operation_id"]


_slow_op_id_store: dict = {}


def test_step_4_get_status_of_running_op(client):
    """GET /operations/<id> returns status while the operation is running."""
    op_id = _slow_op_id_store["id"]
    status, body = client.get(f"/operations/{op_id}")
    assert status == 200
    assert body["operation_id"] == op_id
    assert body["operation_name"] == "slow_op"
    assert body["status"] in ("pending", "running")
    assert body["created_at"] is not None


def test_step_5_cancel_running_op(client):
    """DELETE /operations/<id> cancels the operation; response reflects the request."""
    op_id = _slow_op_id_store["id"]
    status, body = client.request("DELETE", f"/operations/{op_id}", None)
    assert status == 200
    # The op may have been in pending or running — either way, cancellation was accepted.
    assert body["operation_id"] == op_id
    assert body["status"] in ("pending", "cancelled", "running")


def test_step_6_cancelled_op_reaches_terminal_cancelled(client):
    """After cancellation, polling eventually shows status == 'cancelled'."""
    op_id = _slow_op_id_store["id"]
    final = _poll_until_terminal(client, op_id)
    assert final["status"] == "cancelled"
    assert final["completed_at"] is not None


def test_step_7_cancel_already_cancelled_returns_409(client):
    """Cancelling a terminal operation returns 409 Conflict."""
    op_id = _slow_op_id_store["id"]
    status, body = client.request("DELETE", f"/operations/{op_id}", None)
    assert status == 409
    assert "Cannot cancel" in body["error"]


def test_step_8_invoke_unknown_operation_returns_404(client):
    """POST to an unregistered operation name returns 404."""
    status, body = client.request("POST", "/operations/no_such_op", {})
    assert status == 404
    assert "not found" in body["error"].lower()


def test_step_9_get_unknown_operation_id_returns_404(client):
    """GET with an unknown operation ID returns 404."""
    status, body = client.get("/operations/00000000-0000-0000-0000-000000000000")
    assert status == 404
    assert "not found" in body["error"].lower()
