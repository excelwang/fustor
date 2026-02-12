"""
Tests for TreeManager.update_node — basic insert, update, and path normalization.
"""
import pytest
from fustor_view_fs.state import FSState
from fustor_view_fs.tree import TreeManager


@pytest.fixture
def fs_state():
    return FSState("test-view")


@pytest.fixture
def tree(fs_state):
    return TreeManager(fs_state)


@pytest.mark.asyncio
async def test_insert_file(tree, fs_state):
    """Insert a new file node and verify it appears in file_path_map."""
    await tree.update_node(
        {"size": 100, "modified_time": 1000.0, "created_time": 999.0, "is_directory": False},
        "/test.txt"
    )
    node = fs_state.file_path_map.get("/test.txt")
    assert node is not None
    assert node.name == "test.txt"
    assert node.size == 100
    assert node.modified_time == 1000.0


@pytest.mark.asyncio
async def test_insert_directory(tree, fs_state):
    """Insert a new directory node."""
    await tree.update_node(
        {"size": 0, "modified_time": 500.0, "created_time": 500.0, "is_directory": True},
        "/mydir"
    )
    node = fs_state.directory_path_map.get("/mydir")
    assert node is not None
    assert node.name == "mydir"
    assert node.path == "/mydir"


@pytest.mark.asyncio
async def test_update_existing_file(tree, fs_state):
    """Updating an existing file modifies it in place."""
    await tree.update_node(
        {"size": 100, "modified_time": 1000.0, "created_time": 999.0, "is_directory": False},
        "/data.bin"
    )
    await tree.update_node(
        {"size": 200, "modified_time": 2000.0, "created_time": 999.0, "is_directory": False},
        "/data.bin"
    )
    node = fs_state.file_path_map["/data.bin"]
    assert node.size == 200
    assert node.modified_time == 2000.0
    # Should still be one entry
    assert len([k for k in fs_state.file_path_map if k == "/data.bin"]) == 1


@pytest.mark.asyncio
async def test_path_normalization(tree, fs_state):
    """Paths with redundant slashes or no leading slash are normalized."""
    await tree.update_node(
        {"size": 10, "modified_time": 100.0, "created_time": 100.0, "is_directory": False},
        "no_leading_slash.txt"
    )
    assert "/no_leading_slash.txt" in fs_state.file_path_map

    await tree.update_node(
        {"size": 10, "modified_time": 100.0, "created_time": 100.0, "is_directory": False},
        "/a//b///c.txt"
    )
    assert "/a/b/c.txt" in fs_state.file_path_map


@pytest.mark.asyncio
async def test_parent_child_relationship(tree, fs_state):
    """File node is attached as child of its parent directory."""
    await tree.update_node(
        {"size": 0, "modified_time": 100.0, "created_time": 100.0, "is_directory": True},
        "/parent"
    )
    await tree.update_node(
        {"size": 50, "modified_time": 200.0, "created_time": 200.0, "is_directory": False},
        "/parent/child.txt"
    )
    parent = fs_state.directory_path_map["/parent"]
    assert "child.txt" in parent.children
    assert parent.children["child.txt"].size == 50
