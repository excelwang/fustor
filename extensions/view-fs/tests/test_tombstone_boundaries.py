import pytest
import time
import asyncio
from unittest.mock import MagicMock
from fustor_view_fs.arbitrator import FSArbitrator
from fustor_view_fs.audit import AuditManager
from fustor_view_fs.state import FSState
from fustor_view_fs.tree import TreeManager
from fustor_core.event import EventBase, EventType, MessageSource

class TestTombstoneBoundaries:
    """
    P0 Coverage Tests for Tombstone Lifecycle.
    Focus: Boundary conditions for Protection, Resurrection, and Expiry.
    """

    @pytest.fixture
    def setup(self):
        state = FSState("test-view", {"consistency": {"tombstone_ttl_seconds": 60.0}})
        tree = TreeManager(state)
        # Mock logical clock to control time
        state.logical_clock.get_watermark = MagicMock(return_value=1000.0)
        
        arbitrator = FSArbitrator(state, tree, hot_file_threshold=5.0)
        audit = AuditManager(state, tree)
        return state, arbitrator, audit

    @pytest.mark.asyncio
    async def test_tombstone_creation(self, setup):
        """Verify Realtime Delete creates a tombstone."""
        state, arbitrator, _ = setup
        
        # Setup existing node
        await arbitrator.tree_manager.update_node({}, "/file.txt")
        assert "/file.txt" in state.file_path_map
        
        # Trigger Realtime Delete
        event = MagicMock()
        event.event_type = EventType.DELETE
        event.rows = [{"path": "/file.txt"}]
        event.message_source = MessageSource.REALTIME
        
        await arbitrator.process_event(event)
        
        # Verify Node GONE
        assert "/file.txt" not in state.file_path_map
        # Verify Tombstone CREATED
        assert "/file.txt" in state.tombstone_list
        l_ts, p_ts = state.tombstone_list["/file.txt"]
        assert l_ts == 1000.0 # From mocked watermark
        assert p_ts > 0

    @pytest.mark.asyncio
    async def test_tombstone_protection(self, setup):
        """Verify Tombstone protects against older/equal updates."""
        state, arbitrator, _ = setup
        
        # Plant Tombstone (Logical TS = 1000)
        state.tombstone_list["/file.txt"] = (1000.0, time.time())
        
        # Attempt Update with mtime = 900 (Older)
        event = MagicMock()
        event.event_type = EventType.UPDATE
        event.rows = [{"path": "/file.txt", "modified_time": 900.0}]
        event.message_source = MessageSource.SNAPSHOT # Compensatory source
        
        await arbitrator.process_event(event)
        
        # Should be IGNORED (Not in tree)
        assert "/file.txt" not in state.file_path_map
        assert "/file.txt" in state.tombstone_list

    @pytest.mark.asyncio
    async def test_tombstone_resurrection(self, setup):
        """Verify updates NEWER than tombstone cause resurrection."""
        state, arbitrator, _ = setup
        
        # Plant Tombstone (Logical TS = 1000)
        state.tombstone_list["/file.txt"] = (1000.0, time.time())
        
        # Attempt Update with mtime = 1001 (Newer)
        event = MagicMock()
        event.event_type = EventType.UPDATE
        event.rows = [{"path": "/file.txt", "modified_time": 1001.0}]
        event.message_source = MessageSource.SNAPSHOT
        
        await arbitrator.process_event(event)
        
        # Should be ACCEPTED
        assert "/file.txt" in state.file_path_map
        # Tombstone should be REMOVED
        assert "/file.txt" not in state.tombstone_list

    @pytest.mark.asyncio
    async def test_tombstone_cleanup_expiry(self, setup):
        """Verify expired tombstones are cleaned up at audit end."""
        state, _, audit = setup
        
        # TTL is 60s
        now = time.time()
        
        # 1. Expired Tombstone (70s old)
        state.tombstone_list["/expired"] = (1000.0, now - 70.0)
        
        # 2. Valid Tombstone (50s old)
        state.tombstone_list["/valid"] = (1000.0, now - 50.0)
        
        # Run Audit Cleanup
        state.last_audit_start = now # Pretend audit ran
        await audit.handle_end()
        
        # Verify /expired is GONE
        assert "/expired" not in state.tombstone_list
        # Verify /valid REMAINS
        assert "/valid" in state.tombstone_list

    @pytest.mark.asyncio
    async def test_tombstone_cleanup_boundary(self, setup):
        """Verify boundary of TTL cleanup with safe margins."""
        state, _, audit = setup
        
        # TTL is 60s
        now = time.time()
        
        # Test safely within retention window (e.g. 59.5s old)
        state.tombstone_list["/boundary_retained"] = (1000.0, now - 59.5)
        # Test safely outside retention window (e.g. 60.5s old)
        state.tombstone_list["/boundary_expired"] = (1000.0, now - 60.5)
        
        state.last_audit_start = now
        await audit.handle_end()
        
        # Verify retention
        assert "/boundary_retained" in state.tombstone_list
        # Verify expiration
        assert "/boundary_expired" not in state.tombstone_list
