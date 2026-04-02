"""
User journey 6 — Programmatic access without an HTTP server.

Story
-----
A developer wants to use the data types and operations from Python code
directly, without spinning up an HTTP server.  They create an App, register
a data type, and drive the full CRUD lifecycle through the programmatic API
(``app.upsert``, ``app.get``, ``app.list``, ``app.delete``).

Steps (mirrors journey 1, no HTTP involved)
-------------------------------------------
1. Define ``Book`` dataclass.
2. Initialise the app with an in-memory SQLite backend.  Do NOT call
   ``serve_forever()``.
3. ``app.upsert(Book, "book1", ...)``  — insert first book.
4. ``app.upsert(Book, "book2", ...)``  — insert second book.
5. ``app.list(Book)``                  — expect 2 items, both present by key.
6. ``app.get(Book, "book1")``          — get first book; returns Book instance.
7. ``app.delete(Book, "book1")``       — delete first book; returns True.
8. ``app.list(Book)``                  — expect 1 item (book2 only).
9. ``app.delete(Book, "book2")``       — delete second book; returns True.
10. ``app.list(Book)``                 — expect 0 items.
"""

from dataclasses import dataclass

import abstract_data_app
from abstract_data_app import LocalSqliteDataBackend


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
# Shared app (no server started)
# ---------------------------------------------------------------------------

app = abstract_data_app.init(data_backend=LocalSqliteDataBackend(":memory:"))
app.add_data_type(Book)

BOOK1_KEY = "book1"
BOOK1 = Book(title="The Pragmatic Programmer", author="Hunt & Thomas", year=1999, available=True)

BOOK2_KEY = "book2"
BOOK2 = Book(title="Clean Code", author="Robert C. Martin", year=2008, available=False)


# ---------------------------------------------------------------------------
# Journey
# ---------------------------------------------------------------------------

def test_step_3_upsert_book1():
    result = app.upsert(Book, BOOK1_KEY, BOOK1)
    assert result["key"] == BOOK1_KEY
    assert result["data"]["title"] == BOOK1.title


def test_step_4_upsert_book2():
    result = app.upsert(Book, BOOK2_KEY, BOOK2)
    assert result["key"] == BOOK2_KEY
    assert result["data"]["year"] == BOOK2.year


def test_step_5_list_returns_both_books():
    items = app.list(Book)
    assert len(items) == 2
    keys = {item["key"] for item in items}
    assert keys == {BOOK1_KEY, BOOK2_KEY}


def test_step_6_get_book1_returns_dataclass_instance():
    book = app.get(Book, BOOK1_KEY)
    assert book is not None
    assert isinstance(book, Book)
    assert book.title == BOOK1.title
    assert book.author == BOOK1.author
    assert book.year == BOOK1.year
    assert book.available == BOOK1.available


def test_step_6b_get_data_in_list_is_dataclass_instance():
    items = app.list(Book)
    book1_entry = next(i for i in items if i["key"] == BOOK1_KEY)
    assert isinstance(book1_entry["data"], Book)


def test_step_7_delete_book1_returns_true():
    existed = app.delete(Book, BOOK1_KEY)
    assert existed is True


def test_step_8_list_after_first_delete_has_one_item():
    items = app.list(Book)
    assert len(items) == 1
    assert items[0]["key"] == BOOK2_KEY


def test_step_9_delete_book2_returns_true():
    existed = app.delete(Book, BOOK2_KEY)
    assert existed is True


def test_step_10_list_after_both_deletes_is_empty():
    items = app.list(Book)
    assert items == []


def test_step_extra_get_missing_key_returns_none():
    result = app.get(Book, BOOK1_KEY)
    assert result is None


def test_step_extra_delete_nonexistent_key_returns_false():
    existed = app.delete(Book, BOOK1_KEY)
    assert existed is False


def test_step_extra_upsert_accepts_plain_dict():
    result = app.upsert(Book, "book3", {"title": "SICP", "author": "Abelson", "year": 1996, "available": True})
    assert result["key"] == "book3"
    book = app.get(Book, "book3")
    assert isinstance(book, Book)
    assert book.title == "SICP"
    app.delete(Book, "book3")


def test_step_extra_unregistered_type_raises():
    @dataclass
    class Unknown:
        x: int

    try:
        app.upsert(Unknown, "k", Unknown(x=1))
        assert False, "expected KeyError"
    except KeyError:
        pass
