import pytest
import time
from unittest.mock import MagicMock
from fustor_view_fs.tree import TreeManager
from fustor_view_fs.state import FSState
from fustor_view_fs.nodes import DirectoryNode, FileNode
from fustor_schema_fs.models import FSSchemaFields

class TestTreeManager:
    """
    P0 Coverage Tests for TreeManager.
    Focus: Data integrity, recursive operations, and memory leak prevention.
    """

    @pytest.fixture
    def tree_mgr(self):
        state = FSState("test-view", MagicMock()) # Mock clock
        state.max_nodes = 1000 # Allow enough nodes for tests
        return TreeManager(state)

    @pytest.mark.asyncio
    async def test_update_node_create_file(self, tree_mgr):
        """Test creating a new file node."""
        payload = {
            FSSchemaFields.SIZE: 1024,
            FSSchemaFields.MODIFIED_TIME: 100.0,
            FSSchemaFields.CREATED_TIME: 90.0
        }
        await tree_mgr.update_node(payload, "/foo/bar.txt", last_updated_at=200.0)

        assert "/foo/bar.txt" in tree_mgr.state.file_path_map
        node = tree_mgr.state.file_path_map["/foo/bar.txt"]
        assert node.size == 1024
        assert node.modified_time == 100.0
        assert node.last_updated_at == 200.0

        # Verify parent chain creation
        assert "/foo" in tree_mgr.state.directory_path_map
        parent = tree_mgr.state.directory_path_map["/foo"]
        assert "bar.txt" in parent.children
        assert parent.children["bar.txt"] is node

    @pytest.mark.asyncio
    async def test_update_node_existing_file(self, tree_mgr):
        """Test updating an existing file."""
        # Initial creation
        await tree_mgr.update_node({}, "/file.txt")
        node = tree_mgr.state.file_path_map["/file.txt"]
        
        # Update
        payload = {FSSchemaFields.SIZE: 2048, FSSchemaFields.MODIFIED_TIME: 300.0}
        await tree_mgr.update_node(payload, "/file.txt")

        assert node.size == 2048
        assert node.modified_time == 300.0

    @pytest.mark.asyncio
    async def test_ensure_parent_chain(self, tree_mgr):
        """Test recursive parent creation."""
        # Create deep path
        await tree_mgr.update_node({}, "/a/b/c/d.txt")

        assert "/" in tree_mgr.state.directory_path_map
        assert "/a" in tree_mgr.state.directory_path_map
        assert "/a/b" in tree_mgr.state.directory_path_map
        assert "/a/b/c" in tree_mgr.state.directory_path_map

        # Verify linkage
        root = tree_mgr.state.directory_path_map["/"]
        assert "a" in root.children
        assert root.children["a"] is tree_mgr.state.directory_path_map["/a"]
        
        node_a = tree_mgr.state.directory_path_map["/a"]
        assert "b" in node_a.children
        assert node_a.children["b"] is tree_mgr.state.directory_path_map["/a/b"]

    @pytest.mark.asyncio
    async def test_type_switch_file_to_dir(self, tree_mgr):
        """Test switching a path from File to Directory."""
        # 1. Create file
        await tree_mgr.update_node({FSSchemaFields.IS_DIRECTORY: False}, "/path")
        assert "/path" in tree_mgr.state.file_path_map
        assert "/path" not in tree_mgr.state.directory_path_map
        
        # 2. Update as Directory
        await tree_mgr.update_node({FSSchemaFields.IS_DIRECTORY: True}, "/path")
        
        # Verify File gone, Dir exists
        assert "/path" not in tree_mgr.state.file_path_map
        assert "/path" in tree_mgr.state.directory_path_map
        assert isinstance(tree_mgr.state.directory_path_map["/path"], DirectoryNode)
        
        # Verify parent linkage updated
        root = tree_mgr.state.directory_path_map["/"]
        assert isinstance(root.children["path"], DirectoryNode)

    @pytest.mark.asyncio
    async def test_type_switch_dir_to_file(self, tree_mgr):
        """Test switching a path from Directory to File."""
        # 1. Create dir
        await tree_mgr.update_node({FSSchemaFields.IS_DIRECTORY: True}, "/path")
        assert "/path" in tree_mgr.state.directory_path_map
        
        # 2. Update as File
        await tree_mgr.update_node({FSSchemaFields.IS_DIRECTORY: False}, "/path")
        
        # Verify Dir gone, File exists
        assert "/path" not in tree_mgr.state.directory_path_map
        assert "/path" in tree_mgr.state.file_path_map
        assert isinstance(tree_mgr.state.file_path_map["/path"], FileNode)

    @pytest.mark.asyncio
    async def test_delete_root_protection(self, tree_mgr):
        """Ensure root cannot be deleted."""
        await tree_mgr.delete_node("/")
        assert "/" in tree_mgr.state.directory_path_map

    @pytest.mark.asyncio
    async def test_delete_recursive_cleanup(self, tree_mgr):
        """
        Critical: Verify deleting a directory recursively removes all descendants
        from global maps and auxiliary lists (suspects, blind_spots).
        """
        # Setup tree: /dir/subdir/file.txt
        await tree_mgr.update_node({FSSchemaFields.IS_DIRECTORY: True}, "/dir")
        await tree_mgr.update_node({FSSchemaFields.IS_DIRECTORY: True}, "/dir/subdir")
        await tree_mgr.update_node({FSSchemaFields.IS_DIRECTORY: False}, "/dir/subdir/file.txt")
        
        # Populate auxiliary lists to test cleanup
        tree_mgr.state.suspect_list["/dir"] = 1.0
        tree_mgr.state.suspect_list["/dir/subdir"] = 1.0
        tree_mgr.state.suspect_list["/dir/subdir/file.txt"] = 1.0
        
        if not hasattr(tree_mgr.state, 'blind_spot_additions'):
             tree_mgr.state.blind_spot_additions = set()
        tree_mgr.state.blind_spot_additions.add("/dir/subdir")
        
        # Delete /dir
        await tree_mgr.delete_node("/dir")

        # Verify /dir is gone
        assert "/dir" not in tree_mgr.state.directory_path_map
        assert "dir" not in tree_mgr.state.directory_path_map["/"].children
        
        # Verify descendants map cleanup
        assert "/dir/subdir" not in tree_mgr.state.directory_path_map
        assert "/dir/subdir/file.txt" not in tree_mgr.state.file_path_map
        
        # Verify suspect list cleanup
        assert "/dir" not in tree_mgr.state.suspect_list
        assert "/dir/subdir" not in tree_mgr.state.suspect_list
        assert "/dir/subdir/file.txt" not in tree_mgr.state.suspect_list
        
        # Verify blind spot cleanup
        assert "/dir/subdir" not in tree_mgr.state.blind_spot_additions
