"""
Tests for TreeManager.delete_node — recursive cleanup of maps, suspects, and blind spots.
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
async def test_delete_file(tree, fs_state):
    """Deleting a file removes it from file_path_map and parent's children."""
    await tree.update_node(
        {"size": 100, "modified_time": 1000.0, "created_time": 999.0, "is_directory": False},
        "/file.txt"
    )
    assert "/file.txt" in fs_state.file_path_map
    root = fs_state.directory_path_map["/"]
    assert "file.txt" in root.children

    await tree.delete_node("/file.txt")
    assert "/file.txt" not in fs_state.file_path_map
    assert "file.txt" not in root.children


@pytest.mark.asyncio
async def test_delete_directory_recursive(tree, fs_state):
    """Deleting a directory recursively removes all children from maps."""
    # Create structure: /dir/sub/file.txt + /dir/other.txt
    await tree.update_node(
        {"size": 0, "modified_time": 100.0, "created_time": 100.0, "is_directory": True},
        "/dir"
    )
    await tree.update_node(
        {"size": 0, "modified_time": 100.0, "created_time": 100.0, "is_directory": True},
        "/dir/sub"
    )
    await tree.update_node(
        {"size": 10, "modified_time": 100.0, "created_time": 100.0, "is_directory": False},
        "/dir/sub/file.txt"
    )
    await tree.update_node(
        {"size": 20, "modified_time": 100.0, "created_time": 100.0, "is_directory": False},
        "/dir/other.txt"
    )

    await tree.delete_node("/dir")

    assert "/dir" not in fs_state.directory_path_map
    assert "/dir/sub" not in fs_state.directory_path_map
    assert "/dir/sub/file.txt" not in fs_state.file_path_map
    assert "/dir/other.txt" not in fs_state.file_path_map
    assert "dir" not in fs_state.directory_path_map["/"].children


@pytest.mark.asyncio
async def test_delete_clears_suspect_list(tree, fs_state):
    """Deleting a node also clears it from suspect_list."""
    await tree.update_node(
        {"size": 10, "modified_time": 100.0, "created_time": 100.0, "is_directory": False},
        "/suspect.txt"
    )
    # Manually add to suspect list
    import time
    fs_state.suspect_list["/suspect.txt"] = (time.monotonic() + 300, 100.0)

    await tree.delete_node("/suspect.txt")
    assert "/suspect.txt" not in fs_state.suspect_list


@pytest.mark.asyncio
async def test_delete_clears_blind_spot_additions(tree, fs_state):
    """Deleting a node also clears it from blind_spot_additions."""
    await tree.update_node(
        {"size": 10, "modified_time": 100.0, "created_time": 100.0, "is_directory": False},
        "/bspot.txt"
    )
    fs_state.blind_spot_additions.add("/bspot.txt")

    await tree.delete_node("/bspot.txt")
    assert "/bspot.txt" not in fs_state.blind_spot_additions


@pytest.mark.asyncio
async def test_delete_root_blocked(tree, fs_state):
    """Deleting root directory is blocked by safety check."""
    await tree.delete_node("/")
    # Root should still exist
    assert "/" in fs_state.directory_path_map


@pytest.mark.asyncio
async def test_delete_nonexistent_noop(tree, fs_state):
    """Deleting a path that doesn't exist is a no-op."""
    # Should not raise
    await tree.delete_node("/nonexistent.txt")
