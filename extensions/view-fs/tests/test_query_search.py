"""
Tests for FSViewQuery.search_files — glob and substring matching.
"""
import pytest
from fustor_view_fs.state import FSState
from fustor_view_fs.tree import TreeManager
from fustor_view_fs.query import FSViewQuery


@pytest.fixture
def fs_state():
    return FSState("test-view")


@pytest.fixture
def tree(fs_state):
    return TreeManager(fs_state)


@pytest.fixture
def query(fs_state):
    return FSViewQuery(fs_state)


async def _populate_search_tree(tree):
    """Helper to populate tree for search tests."""
    for name in ["report.csv", "data.csv", "readme.txt", "notes.md", "Report.CSV"]:
        await tree.update_node(
            {"size": 10, "modified_time": 100.0, "created_time": 100.0, "is_directory": False},
            f"/{name}"
        )


@pytest.mark.asyncio
async def test_search_substring(tree, query):
    """Substring (case-insensitive) match."""
    await _populate_search_tree(tree)
    results = query.search_files("report")
    paths = [r["path"] for r in results]
    assert "/report.csv" in paths
    assert "/Report.CSV" in paths


@pytest.mark.asyncio
async def test_search_glob_extension(tree, query):
    """Glob matching on file extension."""
    await _populate_search_tree(tree)
    results = query.search_files("*.csv")
    paths = [r["path"] for r in results]
    assert "/report.csv" in paths
    assert "/data.csv" in paths
    # Glob is case-sensitive, so Report.CSV should NOT match *.csv
    assert "/Report.CSV" not in paths


@pytest.mark.asyncio
async def test_search_no_results(tree, query):
    """Search with no matches returns empty list."""
    await _populate_search_tree(tree)
    results = query.search_files("nonexistent_file")
    assert results == []
