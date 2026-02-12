"""
Tests for FSViewQuery.get_directory_tree — depth limiting and path lookup.
"""
import pytest
from fustor_view_fs.state import FSState
from fustor_view_fs.tree import TreeManager
from fustor_view_fs.query import FSViewQuery


@pytest.fixture
def fs_state():
    state = FSState("test-view")
    state.logical_clock.reset(5000.0)
    return state


@pytest.fixture
def tree(fs_state):
    return TreeManager(fs_state)


@pytest.fixture
def query(fs_state):
    return FSViewQuery(fs_state)


async def _build_tree(tree):
    """Build: /data/archive/old.log + /data/report.csv"""
    await tree.update_node(
        {"size": 0, "modified_time": 1000.0, "created_time": 1000.0, "is_directory": True},
        "/data"
    )
    await tree.update_node(
        {"size": 100, "modified_time": 2000.0, "created_time": 2000.0, "is_directory": False},
        "/data/report.csv"
    )
    await tree.update_node(
        {"size": 0, "modified_time": 1500.0, "created_time": 1500.0, "is_directory": True},
        "/data/archive"
    )
    await tree.update_node(
        {"size": 50, "modified_time": 1500.0, "created_time": 1500.0, "is_directory": False},
        "/data/archive/old.log"
    )


@pytest.mark.asyncio
async def test_get_root_recursive(tree, query):
    """Get full tree from root."""
    await _build_tree(tree)
    result = query.get_directory_tree("/", recursive=True)
    assert result is not None
    assert result["content_type"] == "directory"
    # root has 1 child: /data
    assert len(result["children"]) == 1


@pytest.mark.asyncio
async def test_get_subdirectory(tree, query):
    """Get tree from a subdirectory."""
    await _build_tree(tree)
    result = query.get_directory_tree("/data", recursive=True)
    assert result is not None
    assert result["name"] == "data"
    child_names = [c["name"] for c in result["children"]]
    assert "report.csv" in child_names
    assert "archive" in child_names


@pytest.mark.asyncio
async def test_max_depth_zero(tree, query):
    """max_depth=0 returns only the node itself with no children."""
    await _build_tree(tree)
    result = query.get_directory_tree("/data", recursive=True, max_depth=0)
    assert result is not None
    assert "children" not in result or result.get("children") == []


@pytest.mark.asyncio
async def test_max_depth_one(tree, query):
    """max_depth=1 returns direct children but not grandchildren."""
    await _build_tree(tree)
    result = query.get_directory_tree("/data", recursive=True, max_depth=1)
    assert result is not None
    child_names = [c["name"] for c in result["children"]]
    assert "report.csv" in child_names
    assert "archive" in child_names
    # archive should have no children at depth 1
    archive = [c for c in result["children"] if c["name"] == "archive"][0]
    assert archive.get("children", []) == []


@pytest.mark.asyncio
async def test_nonexistent_path(tree, query):
    """Querying a nonexistent path returns None."""
    await _build_tree(tree)
    result = query.get_directory_tree("/nonexistent")
    assert result is None


@pytest.mark.asyncio
async def test_only_path_mode(tree, query):
    """only_path=True omits size/mtime fields."""
    await _build_tree(tree)
    result = query.get_directory_tree("/data", recursive=True, only_path=True)
    assert result is not None
    assert "size" not in result
    assert "modified_time" not in result
    assert "path" in result


@pytest.mark.asyncio
async def test_non_recursive(tree, query):
    """recursive=False returns only the node with empty children."""
    await _build_tree(tree)
    result = query.get_directory_tree("/data", recursive=False)
    assert result is not None
    assert result["children"] == []
