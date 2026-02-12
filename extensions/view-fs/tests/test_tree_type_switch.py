"""
Tests for TreeManager.update_node — type switch protection (file→dir, dir→file).
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
async def test_file_to_directory_switch(tree, fs_state):
    """When a path changes from file to directory, old file node is removed."""
    await tree.update_node(
        {"size": 100, "modified_time": 1000.0, "created_time": 999.0, "is_directory": False},
        "/shared"
    )
    assert "/shared" in fs_state.file_path_map
    assert "/shared" not in fs_state.directory_path_map

    # Now same path becomes a directory
    await tree.update_node(
        {"size": 0, "modified_time": 2000.0, "created_time": 2000.0, "is_directory": True},
        "/shared"
    )
    assert "/shared" not in fs_state.file_path_map
    assert "/shared" in fs_state.directory_path_map


@pytest.mark.asyncio
async def test_directory_to_file_switch(tree, fs_state):
    """When a path changes from directory to file, old dir node is removed."""
    await tree.update_node(
        {"size": 0, "modified_time": 1000.0, "created_time": 1000.0, "is_directory": True},
        "/shared"
    )
    # Add a child to the directory
    await tree.update_node(
        {"size": 10, "modified_time": 1000.0, "created_time": 1000.0, "is_directory": False},
        "/shared/child.txt"
    )
    assert "/shared" in fs_state.directory_path_map
    assert "/shared/child.txt" in fs_state.file_path_map

    # Now same path becomes a file — should clean up children
    await tree.update_node(
        {"size": 500, "modified_time": 2000.0, "created_time": 2000.0, "is_directory": False},
        "/shared"
    )
    assert "/shared" in fs_state.file_path_map
    assert "/shared" not in fs_state.directory_path_map
    # Children should be cleaned up by delete_node
    assert "/shared/child.txt" not in fs_state.file_path_map


@pytest.mark.asyncio
async def test_double_type_switch(tree, fs_state):
    """File → dir → file transitions work correctly."""
    # Start as file
    await tree.update_node(
        {"size": 10, "modified_time": 100.0, "created_time": 100.0, "is_directory": False},
        "/flip"
    )
    assert "/flip" in fs_state.file_path_map

    # Change to dir
    await tree.update_node(
        {"size": 0, "modified_time": 200.0, "created_time": 200.0, "is_directory": True},
        "/flip"
    )
    assert "/flip" in fs_state.directory_path_map
    assert "/flip" not in fs_state.file_path_map

    # Change back to file
    await tree.update_node(
        {"size": 20, "modified_time": 300.0, "created_time": 300.0, "is_directory": False},
        "/flip"
    )
    assert "/flip" in fs_state.file_path_map
    assert "/flip" not in fs_state.directory_path_map
