"""
User journey 3 — Nested/recursive schema with jq filtering on list results.

Story
-----
A developer models ``Article`` items that contain a nested ``Tag`` dataclass,
a list of integer scores, and a metadata dict.  They insert several articles
with overlapping fields and then use jq filters on the list endpoint to slice
the data in different ways.

Schema
------
    @dataclass
    class Tag:
        name: str
        weight: float         # relevance score 0–1

    @dataclass
    class Article:
        title: str
        author: str
        published: bool
        tags: List[Tag]       # list of nested dataclass dicts
        scores: List[int]     # multiple numeric scores
        metadata: Dict[str, Any]

Steps
-----
1.  Define the types and start the server.
2.  Insert 5 articles with varied authors, published states, tags and scores.
3.  GET /data/Article          — unfiltered list returns all 5.
4.  Filter: published == true  — returns only published articles.
5.  Filter: author == "Alice"  — returns only Alice's articles.
6.  Filter: max score >= 90    — articles where any score is >= 90.
7.  Filter: tag name contains "python" — articles carrying that tag.
8.  Filter: compound (published AND author == "Alice").
9.  MCP validate tool — validate a well-formed Article: no errors.
10. MCP validate tool — validate a malformed Article (bad tag weight type,
    missing required field): errors reported for each bad field.
"""

from dataclasses import dataclass, field
from typing import Any, Dict, List

import pytest

import abstract_data_app
from abstract_data_app import Config, LocalSqliteDataBackend

from conftest import Client, find_free_port, start_server


# ---------------------------------------------------------------------------
# Data types
# ---------------------------------------------------------------------------

@dataclass
class Tag:
    name: str
    weight: float


@dataclass
class Article:
    title: str
    author: str
    published: bool
    tags: List[Tag]
    scores: List[int]
    metadata: Dict[str, Any]


# ---------------------------------------------------------------------------
# Test data
# ---------------------------------------------------------------------------

ARTICLES = {
    "art1": {
        "title": "Intro to Python",
        "author": "Alice",
        "published": True,
        "tags": [{"name": "python", "weight": 0.9}, {"name": "tutorial", "weight": 0.7}],
        "scores": [92, 88, 95],
        "metadata": {"lang": "en", "views": 1500},
    },
    "art2": {
        "title": "Advanced Rust",
        "author": "Bob",
        "published": True,
        "tags": [{"name": "rust", "weight": 0.95}, {"name": "systems", "weight": 0.8}],
        "scores": [78, 85],
        "metadata": {"lang": "en", "views": 800},
    },
    "art3": {
        "title": "Python Data Science",
        "author": "Alice",
        "published": False,
        "tags": [{"name": "python", "weight": 0.85}, {"name": "data", "weight": 0.9}],
        "scores": [91, 94, 87],
        "metadata": {"lang": "en", "views": 200},
    },
    "art4": {
        "title": "Go for Beginners",
        "author": "Carol",
        "published": True,
        "tags": [{"name": "go", "weight": 0.75}, {"name": "tutorial", "weight": 0.6}],
        "scores": [70, 75, 80],
        "metadata": {"lang": "en", "views": 650},
    },
    "art5": {
        "title": "Draft: Async Python",
        "author": "Alice",
        "published": False,
        "tags": [{"name": "python", "weight": 0.7}, {"name": "async", "weight": 0.8}],
        "scores": [60, 65],
        "metadata": {"lang": "en", "views": 50},
    },
}


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture(scope="module")
def client() -> Client:
    port = find_free_port()
    app = abstract_data_app.init(
        data_backend=[LocalSqliteDataBackend(":memory:")],
        data_types=[Article],
        config=Config(host="127.0.0.1", port=port, print_errors=True),
    )
    start_server(app, port)
    return Client(f"http://127.0.0.1:{port}")


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def keys_from(items: list) -> set:
    return {item["key"] for item in items}


# ---------------------------------------------------------------------------
# Journey
# ---------------------------------------------------------------------------

def test_step_2_insert_all_articles(client):
    for key, data in ARTICLES.items():
        status, body = client.put(f"/data/Article/{key}", data)
        assert status == 200, f"Upsert of {key} failed: {body}"
        assert body["key"] == key


def test_step_3_unfiltered_list_returns_all_five(client):
    status, body = client.get("/data/Article")
    assert status == 200
    assert body["count"] == 5
    assert keys_from(body["items"]) == set(ARTICLES.keys())


def test_step_4_filter_published_articles(client):
    """Only published articles should be returned."""
    published_keys = {k for k, v in ARTICLES.items() if v["published"]}
    # art1, art2, art4 are published
    assert published_keys == {"art1", "art2", "art4"}

    import urllib.parse
    filt = urllib.parse.quote(".[] | select(.data.published == true)")
    status, body = client.get(f"/data/Article?filter={filt}")
    assert status == 200
    assert keys_from(body["items"]) == published_keys


def test_step_5_filter_by_author_alice(client):
    """Filter articles authored by Alice."""
    alice_keys = {k for k, v in ARTICLES.items() if v["author"] == "Alice"}
    assert alice_keys == {"art1", "art3", "art5"}

    import urllib.parse
    filt = urllib.parse.quote('.[] | select(.data.author == "Alice")')
    status, body = client.get(f"/data/Article?filter={filt}")
    assert status == 200
    assert keys_from(body["items"]) == alice_keys


def test_step_6_filter_articles_with_max_score_at_least_90(client):
    """Articles where at least one score is >= 90."""
    high_score_keys = {
        k for k, v in ARTICLES.items() if any(s >= 90 for s in v["scores"])
    }
    assert high_score_keys == {"art1", "art3"}

    import urllib.parse
    filt = urllib.parse.quote(".[] | select(.data.scores | max >= 90)")
    status, body = client.get(f"/data/Article?filter={filt}")
    assert status == 200
    assert keys_from(body["items"]) == high_score_keys


def test_step_7_filter_articles_with_python_tag(client):
    """Articles that carry a tag named 'python'."""
    python_keys = {
        k for k, v in ARTICLES.items()
        if any(t["name"] == "python" for t in v["tags"])
    }
    assert python_keys == {"art1", "art3", "art5"}

    import urllib.parse
    filt = urllib.parse.quote(
        '.[] | select(.data.tags | map(.name) | any(. == "python"))'
    )
    status, body = client.get(f"/data/Article?filter={filt}")
    assert status == 200
    assert keys_from(body["items"]) == python_keys


def test_step_8_compound_filter_published_and_alice(client):
    """Published articles by Alice — the intersection."""
    expected = {
        k for k, v in ARTICLES.items()
        if v["published"] and v["author"] == "Alice"
    }
    assert expected == {"art1"}

    import urllib.parse
    filt = urllib.parse.quote(
        '.[] | select(.data.published == true and .data.author == "Alice")'
    )
    status, body = client.get(f"/data/Article?filter={filt}")
    assert status == 200
    assert keys_from(body["items"]) == expected


def test_step_9_mcp_validate_well_formed_article(client):
    """A correctly-typed Article payload should validate without errors."""
    valid_article = {
        "title": "Valid Article",
        "author": "Test",
        "published": True,
        "tags": [{"name": "test", "weight": 0.5}],
        "scores": [80, 85],
        "metadata": {"key": "value"},
    }
    result = client.tool("Article_validate", {"data": valid_article})
    assert result["valid"] is True
    assert result["errors"] == []


def test_step_10a_mcp_validate_missing_required_field(client):
    """An Article missing ``author`` should report that error."""
    missing_author = {
        "title": "No Author",
        # author is missing
        "published": True,
        "tags": [],
        "scores": [],
        "metadata": {},
    }
    result = client.tool("Article_validate", {"data": missing_author})
    assert result["valid"] is False
    assert any("author" in e for e in result["errors"]), result["errors"]


def test_step_10b_mcp_validate_wrong_type_in_nested_list(client):
    """
    An Article with a tag whose ``weight`` is a string (should be float)
    must surface a type error.
    """
    bad_tag_weight = {
        "title": "Bad Tags",
        "author": "Tester",
        "published": False,
        "tags": [{"name": "good", "weight": "not-a-float"}],  # weight is str, not float
        "scores": [50],
        "metadata": {},
    }
    result = client.tool("Article_validate", {"data": bad_tag_weight})
    assert result["valid"] is False
    # Error should mention the tags field
    assert any("tags" in e for e in result["errors"]), result["errors"]


def test_step_10c_mcp_validate_wrong_type_in_scores_list(client):
    """
    Scores must be a list of ints.  A list containing a string should fail.
    """
    bad_scores = {
        "title": "Bad Scores",
        "author": "Tester",
        "published": True,
        "tags": [],
        "scores": [90, "not-an-int", 85],  # mixed list
        "metadata": {},
    }
    result = client.tool("Article_validate", {"data": bad_scores})
    assert result["valid"] is False
    assert any("scores" in e for e in result["errors"]), result["errors"]
