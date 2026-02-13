"""
Robustness tests for TreeManager deletion logic, ensuring no NameErrors or inconsistent states.
"""
import pytest
import os
from fustor_view_fs.state import FSState
from fustor_view_fs.tree import TreeManager

@pytest.fixture
def fs_state():
    return FSState("test-view")

@pytest.fixture
def tree(fs_state):
    return TreeManager(fs_state)

@pytest.mark.asyncio
async def test_deep_path_deletion_consistency(tree, fs_state):
    """
    Test deleting a deeply nested file path and verify parent children consistency.
    This specifically checks the fix for the 'parent' NameError in delete_node.
    """
    deep_path = "/a/b/c/d/file.txt"
    await tree.update_node(
        {"size": 100, "modified_time": 1000.0, "created_time": 999.0, "is_directory": False},
        deep_path
    )
    
    # Verify all parents created
    assert "/a" in fs_state.directory_path_map
    assert "/a/b" in fs_state.directory_path_map
    assert "/a/b/c" in fs_state.directory_path_map
    assert "/a/b/c/d" in fs_state.directory_path_map
    assert deep_path in fs_state.file_path_map
    
    # Verify parent links
    d_node = fs_state.directory_path_map["/a/b/c/d"]
    assert "file.txt" in d_node.children
    
    # DELETE
    await tree.delete_node(deep_path)
    
    # Verify file is gone from map and parent children
    assert deep_path not in fs_state.file_path_map
    assert "file.txt" not in d_node.children
    
    # Verify parents still exist
    assert "/a/b/c/d" in fs_state.directory_path_map

@pytest.mark.asyncio
async def test_delete_nonexistent_deep_path(tree, fs_state):
    """Deleting a non-existent deep path should not crash."""
    await tree.delete_node("/non/existent/path/file.txt")

@pytest.mark.asyncio
async def test_delete_directory_with_children_consistency(tree, fs_state):
    """Verify recursive deletion cleanup."""
    await tree.update_node({"is_dir": True}, "/dir")
    await tree.update_node({"is_dir": False}, "/dir/f1.txt")
    await tree.update_node({"is_dir": True}, "/dir/sub")
    await tree.update_node({"is_dir": False}, "/dir/sub/f2.txt")
    
    assert "/dir/sub/f2.txt" in fs_state.file_path_map
    
    await tree.delete_node("/dir")
    
    assert "/dir" not in fs_state.directory_path_map
    assert "/dir/sub" not in fs_state.directory_path_map
    assert "/dir/f1.txt" not in fs_state.file_path_map
    assert "/dir/sub/f2.txt" not in fs_state.file_path_map
    assert "dir" not in fs_state.directory_path_map["/"].children
