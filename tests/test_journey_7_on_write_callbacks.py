"""
User journey 7 — on_write callbacks.

Story
-----
A developer wants to react to every successful write (upsert or delete) on a
data type without polling the store.  Common uses: audit logging, cache
invalidation, event streaming, and real-time notifications.

They register one or more callbacks via ``app.add_on_write_callback(DataType,
fn)``.  Each callback receives three arguments:

* ``operation`` — ``"upsert"`` or ``"delete"``
* ``key``       — the item key
* ``data``      — the written data dict for upserts; ``None`` for deletes

Steps
-----
1. Define ``Order`` and ``Invoice`` dataclasses.
2. Initialise the app with an in-memory SQLite backend.
3. Register a single callback on ``Order`` — verify it fires on upsert and
   delete via the programmatic API.
4. Register a second callback on ``Order`` — verify both fire in order.
5. Register a callback on ``Invoice`` — verify callbacks are type-isolated
   (Order writes do not fire Invoice callbacks and vice-versa).
6. Verify that a callback raising an exception does not cause the write to fail
   and that the remaining callbacks in the chain still execute.
7. Verify callbacks fire when writes are made through the HTTP routes
   (PUT / DELETE).
8. Verify that registering a callback for an unregistered type raises KeyError.
"""

import threading
from dataclasses import dataclass
from typing import List

import pytest

import abstract_data_app
from abstract_data_app import LocalSqliteDataBackend
from conftest import Client, find_free_port, start_server


# ---------------------------------------------------------------------------
# Data types
# ---------------------------------------------------------------------------

@dataclass
class Order:
    product: str
    quantity: int
    shipped: bool


@dataclass
class Invoice:
    order_key: str
    amount: float


# ---------------------------------------------------------------------------
# Shared app (no server started yet for programmatic steps)
# ---------------------------------------------------------------------------

app = abstract_data_app.init(data_backend=LocalSqliteDataBackend(":memory:"))
app.add_data_type(Order)
app.add_data_type(Invoice)


# ---------------------------------------------------------------------------
# Step 3 — single callback fires on upsert and delete
# ---------------------------------------------------------------------------

def test_step_3a_callback_fires_on_upsert():
    events = []
    app.add_on_write_callback(Order, lambda op, key, data: events.append((op, key, data)))

    app.upsert(Order, "o1", Order(product="Widget", quantity=3, shipped=False))

    assert len(events) == 1
    op, key, data = events[0]
    assert op == "upsert"
    assert key == "o1"
    assert data["product"] == "Widget"
    assert data["quantity"] == 3
    assert data["shipped"] is False


def test_step_3b_callback_fires_on_delete():
    events = []
    app.add_on_write_callback(Order, lambda op, key, data: events.append((op, key, data)))

    # o1 was inserted in the previous test step
    app.delete(Order, "o1")

    # The newly-registered callback (and all previously registered ones) fire.
    delete_events = [e for e in events if e[0] == "delete"]
    assert len(delete_events) >= 1
    op, key, data = delete_events[0]
    assert op == "delete"
    assert key == "o1"
    assert data is None


# ---------------------------------------------------------------------------
# Step 4 — multiple callbacks fire in registration order
# ---------------------------------------------------------------------------

def test_step_4_multiple_callbacks_fire_in_order():
    call_order = []

    app_local = abstract_data_app.init(data_backend=LocalSqliteDataBackend(":memory:"))
    app_local.add_data_type(Order)
    app_local.add_on_write_callback(Order, lambda op, key, data: call_order.append("first"))
    app_local.add_on_write_callback(Order, lambda op, key, data: call_order.append("second"))
    app_local.add_on_write_callback(Order, lambda op, key, data: call_order.append("third"))

    app_local.upsert(Order, "o2", {"product": "Cog", "quantity": 1, "shipped": True})

    assert call_order == ["first", "second", "third"]


# ---------------------------------------------------------------------------
# Step 5 — callbacks are type-isolated
# ---------------------------------------------------------------------------

def test_step_5_callbacks_are_type_isolated():
    order_events: list = []
    invoice_events: list = []

    app_local = abstract_data_app.init(data_backend=LocalSqliteDataBackend(":memory:"))
    app_local.add_data_type(Order)
    app_local.add_data_type(Invoice)
    app_local.add_on_write_callback(Order, lambda op, key, data: order_events.append(op))
    app_local.add_on_write_callback(Invoice, lambda op, key, data: invoice_events.append(op))

    app_local.upsert(Order, "o3", {"product": "Bolt", "quantity": 10, "shipped": False})
    assert order_events == ["upsert"]
    assert invoice_events == []

    app_local.upsert(Invoice, "inv1", {"order_key": "o3", "amount": 49.99})
    assert order_events == ["upsert"]          # unchanged
    assert invoice_events == ["upsert"]

    app_local.delete(Order, "o3")
    assert order_events == ["upsert", "delete"]
    assert invoice_events == ["upsert"]        # unchanged


# ---------------------------------------------------------------------------
# Step 6 — a failing callback does not prevent other callbacks or the return
#           value from reaching the caller
# ---------------------------------------------------------------------------

def test_step_6_failing_callback_does_not_abort_write():
    good_events: list = []

    app_local = abstract_data_app.init(data_backend=LocalSqliteDataBackend(":memory:"))
    app_local.add_data_type(Order)

    def bad_callback(op, key, data):
        raise RuntimeError("intentional failure in callback")

    app_local.add_on_write_callback(Order, bad_callback)
    app_local.add_on_write_callback(Order, lambda op, key, data: good_events.append(op))

    # Write must succeed despite the bad callback
    result = app_local.upsert(Order, "o4", {"product": "Screw", "quantity": 5, "shipped": False})
    assert result["key"] == "o4"

    # The second (good) callback still fires
    assert good_events == ["upsert"]


# ---------------------------------------------------------------------------
# Step 7 — callbacks fire via HTTP routes
# ---------------------------------------------------------------------------

@pytest.fixture(scope="module")
def http_client_and_tracker():
    """Start a server; return (Client, list_of_captured_events)."""
    events: list = []

    http_app = abstract_data_app.init(data_backend=LocalSqliteDataBackend(":memory:"))
    http_app.add_data_type(Order)
    http_app.add_on_write_callback(
        Order, lambda op, key, data: events.append((op, key, data))
    )

    port = find_free_port()
    start_server(http_app, port)
    return Client(f"http://127.0.0.1:{port}"), events


def test_step_7a_http_put_fires_callback(http_client_and_tracker):
    client, events = http_client_and_tracker
    before = len(events)

    status, body = client.put("/data/Order/http-o1", {"product": "Nut", "quantity": 7, "shipped": False})
    assert status == 200

    assert len(events) == before + 1
    op, key, data = events[-1]
    assert op == "upsert"
    assert key == "http-o1"
    assert data["product"] == "Nut"


def test_step_7b_http_delete_fires_callback(http_client_and_tracker):
    client, events = http_client_and_tracker
    before = len(events)

    status, _ = client.delete("/data/Order/http-o1")
    assert status == 200

    assert len(events) == before + 1
    op, key, data = events[-1]
    assert op == "delete"
    assert key == "http-o1"
    assert data is None


# ---------------------------------------------------------------------------
# Step 8 — registering for an unregistered type raises KeyError
# ---------------------------------------------------------------------------

def test_step_8_unregistered_type_raises_key_error():
    @dataclass
    class Ghost:
        name: str

    app_local = abstract_data_app.init(data_backend=LocalSqliteDataBackend(":memory:"))

    with pytest.raises(KeyError):
        app_local.add_on_write_callback(Ghost, lambda op, key, data: None)


# ---------------------------------------------------------------------------
# Extra — callback receives a copy of the data, not the internal dict
# ---------------------------------------------------------------------------

def test_extra_callback_data_matches_upserted_payload():
    received: list = []

    app_local = abstract_data_app.init(data_backend=LocalSqliteDataBackend(":memory:"))
    app_local.add_data_type(Invoice)
    app_local.add_on_write_callback(
        Invoice, lambda op, key, data: received.append(dict(data) if data else None)
    )

    app_local.upsert(Invoice, "inv2", {"order_key": "o5", "amount": 99.0})
    assert received == [{"order_key": "o5", "amount": 99.0}]


# ---------------------------------------------------------------------------
# Extra — method chaining works
# ---------------------------------------------------------------------------

def test_extra_add_on_write_callback_supports_chaining():
    app_local = abstract_data_app.init(data_backend=LocalSqliteDataBackend(":memory:"))
    returned = (
        app_local
        .add_data_type(Order)
        .add_on_write_callback(Order, lambda op, key, data: None)
        .add_data_type(Invoice)
        .add_on_write_callback(Invoice, lambda op, key, data: None)
    )
    assert returned is app_local


# ---------------------------------------------------------------------------
# Extra — thread safety: concurrent upserts each fire exactly one callback
# ---------------------------------------------------------------------------

def test_extra_concurrent_upserts_each_fire_callback():
    lock = threading.Lock()
    events: list = []

    def record(op, key, data):
        with lock:
            events.append(key)

    app_local = abstract_data_app.init(data_backend=LocalSqliteDataBackend(":memory:"))
    app_local.add_data_type(Order)
    app_local.add_on_write_callback(Order, record)

    threads = [
        threading.Thread(
            target=app_local.upsert,
            args=(Order, f"concurrent-{i}", {"product": "Bolt", "quantity": i, "shipped": False}),
        )
        for i in range(20)
    ]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    assert sorted(events) == sorted(f"concurrent-{i}" for i in range(20))
