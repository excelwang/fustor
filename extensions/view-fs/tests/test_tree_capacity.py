import pytest
import asyncio
from fustor_view_fs.state import FSState
from fustor_view_fs.tree import TreeManager

class TestTreeCapacity:
    """
    Verify OOM Protection mechanism.
    """

    @pytest.mark.asyncio
    async def test_max_nodes_limit(self):
        # Configure limit = 5 (Root + 4 nodes)
        config = {"limits": {"max_nodes": 5}}
        state = FSState("test-view", config)
        tree = TreeManager(state)
        
        # 1. Fill to capacity
        # Root already exists (1)
        await tree.update_node({"is_dir": True}, "/a") # 2
        await tree.update_node({"is_dir": True}, "/b") # 3
        await tree.update_node({"is_dir": True}, "/c") # 4
        await tree.update_node({"is_dir": True}, "/d") # 5 (Limit Reached)
        
        assert "/d" in state.directory_path_map
        
        # 2. Try to add 6th node (Should be blocked)
        await tree.update_node({"is_dir": True}, "/e")
        
        assert "/e" not in state.directory_path_map
        assert len(state.directory_path_map) + len(state.file_path_map) == 5

    @pytest.mark.asyncio
    async def test_recursive_creation_limit(self):
        """Verify _ensure_parent_chain stops mid-way if limit hit."""
        config = {"limits": {"max_nodes": 4}}
        state = FSState("test-view", config)
        tree = TreeManager(state)
        
        # Root (1)
        
        # Attempt to create /a/b/c/d
        # Creates /a (2), /a/b (3), /a/b/c (4 - Limit), /a/b/c/d (Blocked)
        
        await tree.update_node({}, "/a/b/c/d")
        
        assert "/a/b/c" in state.directory_path_map
        assert "/a/b/c/d" not in state.directory_path_map
