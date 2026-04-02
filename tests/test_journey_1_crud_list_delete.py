"""
User journey 1 — Create, list, and delete items via HTTP CRUD routes.

Story
-----
A developer defines a ``Book`` data type and starts the server.  They add two
books, confirm both appear in the listing, then delete them one at a time and
verify the list shrinks accordingly.

Steps
-----
1. Define ``Book`` dataclass.
2. Initialise the app with an in-memory SQLite backend and start the server.
3. PUT /data/Book/book1  — upsert first book.
4. PUT /data/Book/book2  — upsert second book.
5. GET /data/Book        — list: expect 2 items, both present by key.
6. GET /data/Book/book1  — get first book individually.
7. DELETE /data/Book/book1 — delete first book; expect deleted=true.
8. GET /data/Book        — list: expect 1 item (book2 only).
9. DELETE /data/Book/book2 — delete second book; expect deleted=true.
10. GET /data/Book       — list: expect 0 items.
"""

from dataclasses import dataclass

import pytest

import abstract_data_app
from abstract_data_app import Config, LocalSqliteDataBackend

from conftest import Client, find_free_port, start_server


# ---------------------------------------------------------------------------
# Data type
# ---------------------------------------------------------------------------

@dataclass
class Book:
    title: str
    author: str
    year: int
    available: bool


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture(scope="module")
def client() -> Client:
    port = find_free_port()
    app = abstract_data_app.init(
        data_backend=LocalSqliteDataBackend(":memory:"),
        config=Config(host="127.0.0.1", port=port, print_errors=True),
    )
    app.add_data_type(Book)
    start_server(app, port)
    return Client(f"http://127.0.0.1:{port}")


# ---------------------------------------------------------------------------
# Journey
# ---------------------------------------------------------------------------

BOOK1_KEY = "book1"
BOOK1 = {"title": "The Pragmatic Programmer", "author": "Hunt & Thomas", "year": 1999, "available": True}

BOOK2_KEY = "book2"
BOOK2 = {"title": "Clean Code", "author": "Robert C. Martin", "year": 2008, "available": False}


def test_step_3_upsert_book1(client):
    status, body = client.put(f"/data/Book/{BOOK1_KEY}", BOOK1)
    assert status == 200
    assert body["key"] == BOOK1_KEY
    assert body["data"]["title"] == BOOK1["title"]


def test_step_4_upsert_book2(client):
    status, body = client.put(f"/data/Book/{BOOK2_KEY}", BOOK2)
    assert status == 200
    assert body["key"] == BOOK2_KEY
    assert body["data"]["year"] == BOOK2["year"]


def test_step_5_list_returns_both_books(client):
    status, body = client.get("/data/Book")
    assert status == 200
    assert body["count"] == 2
    keys = {item["key"] for item in body["items"]}
    assert keys == {BOOK1_KEY, BOOK2_KEY}


def test_step_6_get_book1_individually(client):
    status, body = client.get(f"/data/Book/{BOOK1_KEY}")
    assert status == 200
    assert body["key"] == BOOK1_KEY
    assert body["data"] == BOOK1


def test_step_7_delete_book1(client):
    status, body = client.delete(f"/data/Book/{BOOK1_KEY}")
    assert status == 200
    assert body["deleted"] is True
    assert body["key"] == BOOK1_KEY


def test_step_8_list_after_first_delete_has_one_item(client):
    status, body = client.get("/data/Book")
    assert status == 200
    assert body["count"] == 1
    assert body["items"][0]["key"] == BOOK2_KEY


def test_step_9_delete_book2(client):
    status, body = client.delete(f"/data/Book/{BOOK2_KEY}")
    assert status == 200
    assert body["deleted"] is True


def test_step_10_list_after_both_deletes_is_empty(client):
    status, body = client.get("/data/Book")
    assert status == 200
    assert body["count"] == 0
    assert body["items"] == []


def test_step_extra_404_on_deleted_key(client):
    """Getting a key after deletion should return 404."""
    status, _ = client.get(f"/data/Book/{BOOK1_KEY}")
    assert status == 404


def test_step_extra_delete_nonexistent_key(client):
    """Deleting an already-gone key should report deleted=False."""
    status, body = client.delete(f"/data/Book/{BOOK1_KEY}")
    assert status == 404
    assert body["deleted"] is False
