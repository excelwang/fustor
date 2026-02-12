"""
Tests for FSViewQuery.get_stats — boundary conditions.
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


def test_stats_empty_tree(query):
    """Stats on an empty tree (only root)."""
    stats = query.get_stats()
    assert stats["total_files"] == 0
    assert stats["total_directories"] == 1  # root
    assert stats["total_size"] == 0
    assert stats["suspect_file_count"] == 0
    assert stats["has_blind_spot"] is False


@pytest.mark.asyncio
async def test_stats_with_files(tree, query, fs_state):
    """Stats correctly count files, dirs, and total size."""
    await tree.update_node(
        {"size": 0, "modified_time": 1000.0, "created_time": 1000.0, "is_directory": True},
        "/data"
    )
    await tree.update_node(
        {"size": 100, "modified_time": 2000.0, "created_time": 2000.0, "is_directory": False},
        "/data/a.txt"
    )
    await tree.update_node(
        {"size": 200, "modified_time": 3000.0, "created_time": 3000.0, "is_directory": False},
        "/data/b.txt"
    )

    stats = query.get_stats()
    assert stats["total_files"] == 2
    assert stats["total_directories"] == 2  # root + /data
    assert stats["total_size"] == 300
    assert stats["item_count"] == 4


@pytest.mark.asyncio
async def test_stats_suspect_count(tree, query, fs_state):
    """Stats counts suspect files."""
    await tree.update_node(
        {"size": 10, "modified_time": 100.0, "created_time": 100.0, "is_directory": False},
        "/suspect.txt"
    )
    fs_state.file_path_map["/suspect.txt"].integrity_suspect = True

    stats = query.get_stats()
    assert stats["suspect_file_count"] == 1


def test_stats_blind_spot_flag(query, fs_state):
    """Stats has_blind_spot reflects additions/deletions."""
    assert query.get_stats()["has_blind_spot"] is False
    fs_state.blind_spot_additions.add("/new.txt")
    assert query.get_stats()["has_blind_spot"] is True
