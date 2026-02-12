"""
Test: Large File Writing Suspect Maintenance (Recovers deleted test_c6).

Validates that a file being actively written (non-atomic partial writes)
remains suspect throughout the write process even as multiple MODIFY events
arrive, and is only cleared when the final CLOSE_WRITE atomic event arrives.

Spec Reference: §4.3 Stability-based Model
"""
import pytest
import time
from unittest.mock import MagicMock, patch

from fustor_view_fs.arbitrator import FSArbitrator
from fustor_view_fs.state import FSState
from fustor_view_fs.tree import TreeManager
from fustor_core.event import EventType, MessageSource


@pytest.fixture
def setup():
    state = FSState("test_view")
    tree = TreeManager(state)
    arb = FSArbitrator(state, tree, hot_file_threshold=30.0)
    return state, arb


@pytest.mark.asyncio
async def test_partial_writes_keep_suspect_flag(setup):
    """
    Simulate a large file being written in chunks:
    Multiple MODIFY (is_atomic_write=False) events arrive.
    File should remain suspect throughout.
    """
    state, arb = setup
    path = "/data/large_upload.bin"

    # Simulate 5 partial writes (each one advances mtime slightly)
    for i in range(5):
        event = MagicMock()
        event.message_source = MessageSource.REALTIME
        event.event_type = EventType.UPDATE
        event.rows = [{
            "path": path,
            "file_name": "large_upload.bin",
            "size": (i + 1) * 1024,  # Growing file
            "modified_time": 1000.0 + i,
            "is_directory": False,
            "is_atomic_write": False,  # Partial write (MODIFY)
        }]
        await arb.process_event(event)

        node = state.get_node(path)
        assert node is not None
        assert node.integrity_suspect is True, (
            f"Partial write #{i+1} should keep file suspect"
        )
        assert path in state.suspect_list


@pytest.mark.asyncio
async def test_close_write_after_partial_clears_suspect(setup):
    """
    After partial writes, a final CLOSE_WRITE (is_atomic_write=True)
    should clear the suspect flag.
    """
    state, arb = setup
    path = "/data/large_upload.bin"

    # Partial write
    partial_event = MagicMock()
    partial_event.message_source = MessageSource.REALTIME
    partial_event.event_type = EventType.UPDATE
    partial_event.rows = [{
        "path": path,
        "file_name": "large_upload.bin",
        "size": 1024,
        "modified_time": 1000.0,
        "is_directory": False,
        "is_atomic_write": False,
    }]
    await arb.process_event(partial_event)
    assert state.get_node(path).integrity_suspect is True

    # Final atomic write (CLOSE_WRITE)
    close_event = MagicMock()
    close_event.message_source = MessageSource.REALTIME
    close_event.event_type = EventType.UPDATE
    close_event.rows = [{
        "path": path,
        "file_name": "large_upload.bin",
        "size": 5120,
        "modified_time": 1005.0,
        "is_directory": False,
        "is_atomic_write": True,  # CLOSE_WRITE
    }]
    await arb.process_event(close_event)

    node = state.get_node(path)
    assert node.integrity_suspect is False, "CLOSE_WRITE should clear suspect"
    assert path not in state.suspect_list


@pytest.mark.asyncio
async def test_size_grows_during_write(setup):
    """
    Verify that the node's size is progressively updated during partial writes,
    reflecting the growing file.
    """
    state, arb = setup
    path = "/data/growing.bin"

    sizes = [100, 500, 2000, 8000, 32000]
    for i, sz in enumerate(sizes):
        event = MagicMock()
        event.message_source = MessageSource.REALTIME
        event.event_type = EventType.UPDATE
        event.rows = [{
            "path": path,
            "file_name": "growing.bin",
            "size": sz,
            "modified_time": 1000.0 + i,
            "is_directory": False,
            "is_atomic_write": False,
        }]
        await arb.process_event(event)

        node = state.get_node(path)
        assert node.size == sz, f"Size should be {sz} after write #{i+1}"
        assert node.integrity_suspect is True
