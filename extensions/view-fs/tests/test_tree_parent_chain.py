"""
Tests for TreeManager._ensure_parent_chain — recursive directory creation.
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
async def test_auto_create_single_level_parent(tree, fs_state):
    """Creating a file in /a/file.txt auto-creates /a."""
    await tree.update_node(
        {"size": 10, "modified_time": 100.0, "created_time": 100.0, "is_directory": False},
        "/a/file.txt"
    )
    assert "/a" in fs_state.directory_path_map
    assert "/a/file.txt" in fs_state.file_path_map
    # Auto-created parent should have 0.0 last_updated_at
    assert fs_state.directory_path_map["/a"].last_updated_at == 0.0


@pytest.mark.asyncio
async def test_auto_create_deep_parent_chain(tree, fs_state):
    """Creating /a/b/c/d/file.txt auto-creates all intermediate dirs."""
    await tree.update_node(
        {"size": 5, "modified_time": 100.0, "created_time": 100.0, "is_directory": False},
        "/a/b/c/d/file.txt"
    )
    for path in ["/a", "/a/b", "/a/b/c", "/a/b/c/d"]:
        assert path in fs_state.directory_path_map, f"Missing auto-created dir: {path}"
    
    # Verify chain relationships: root → a → b → c → d → file
    root = fs_state.directory_path_map["/"]
    assert "a" in root.children
    a = fs_state.directory_path_map["/a"]
    assert "b" in a.children
    d = fs_state.directory_path_map["/a/b/c/d"]
    assert "file.txt" in d.children


@pytest.mark.asyncio
async def test_reuse_existing_parent(tree, fs_state):
    """If a parent already exists, it is reused (not replaced)."""
    await tree.update_node(
        {"size": 0, "modified_time": 500.0, "created_time": 500.0, "is_directory": True},
        "/existing"
    )
    original_node = fs_state.directory_path_map["/existing"]
    original_node.modified_time = 999.0  # Mark it

    await tree.update_node(
        {"size": 10, "modified_time": 100.0, "created_time": 100.0, "is_directory": False},
        "/existing/child.txt"
    )
    # Parent should be the same object, not replaced
    assert fs_state.directory_path_map["/existing"] is original_node
    assert fs_state.directory_path_map["/existing"].modified_time == 999.0


@pytest.mark.asyncio
async def test_multiple_files_share_auto_created_parent(tree, fs_state):
    """Multiple files under the same auto-created parent share it."""
    await tree.update_node(
        {"size": 10, "modified_time": 100.0, "created_time": 100.0, "is_directory": False},
        "/shared/a.txt"
    )
    await tree.update_node(
        {"size": 20, "modified_time": 200.0, "created_time": 200.0, "is_directory": False},
        "/shared/b.txt"
    )
    parent = fs_state.directory_path_map["/shared"]
    assert "a.txt" in parent.children
    assert "b.txt" in parent.children
    assert len(parent.children) == 2
