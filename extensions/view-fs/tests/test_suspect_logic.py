import pytest
import time
import asyncio
from unittest.mock import MagicMock
import heapq
from fustor_view_fs import FSViewDriver
from fustor_core.event import UpdateEvent, MessageSource, EventType

@pytest.fixture
def parser():
    p = FSViewDriver(id="test_view", view_id="1")
    p.hot_file_threshold = 30.0
    # Reset clock to a small value so 1000.0 is considered "hot" (vs time.time())
    p._logical_clock.reset(0.001)
    return p

@pytest.mark.asyncio
async def test_suspect_stability_cleanup_stable(parser):
    # 1. Add a file to suspect list
    path = "/hot_file.txt"
    mtime = 1000.0
    # Set logical clock watermark close to mtime to make it hot
    parser._logical_clock.update(mtime + 1.0)
    
    payload = {
        'file_path': path,
        'modified_time': mtime,
        'size': 100,
        'is_dir': False
    }
    event = UpdateEvent(
        event_schema="nfs",
        table="files",
        rows=[payload],
        fields=list(payload.keys()),
        message_source=MessageSource.AUDIT,
        index=int((mtime + 1.0) * 1000)
    )
    
    await parser.process_event(event)
    
    # Assert it started as suspect
    assert path in parser._suspect_list
    assert parser._get_node(path).integrity_suspect is True
    
    # 2. Simulate TTL expiry (physical time)
    now_monotonic = time.monotonic()
    # Force expired
    expiry = now_monotonic - 10.0
    parser._suspect_list[path] = (expiry, mtime) 
    heapq.heappush(parser._suspect_heap, (expiry, path))
    parser._last_suspect_cleanup_time = 0.0 # Ensure cleanup runs
    
    # Act: trigger cleanup
    parser._cleanup_expired_suspects_unlocked()
        
    # Assert: Stable file (mtime 1000 -> 1000) should be ELIMINATED
    assert path not in parser._suspect_list
    assert parser._get_node(path).integrity_suspect is False

@pytest.mark.asyncio
async def test_suspect_stability_cleanup_renewal(parser):
    # 1. Add a file to suspect list
    path = "/active_file.txt"
    mtime = 1000.0
    parser._logical_clock.update(mtime + 1.0)
    
    payload = {
        'file_path': path,
        'modified_time': mtime,
        'size': 100,
        'is_dir': False
    }
    event = UpdateEvent(
        event_schema="nfs",
        table="files",
        rows=[payload],
        fields=list(payload.keys()),
        message_source=MessageSource.AUDIT,
        index=int((mtime + 1.0) * 1000)
    )
    await parser.process_event(event)
    
    # 2. Simulate mtime change in memory tree (Update but same TTL)
    node = parser._get_node(path)
    node.modified_time = 1005.0 # Mtime changed!
    
    # 3. Simulate TTL expiry
    now_monotonic = time.monotonic()
    # Force expiry, recorded was 1000
    expiry = now_monotonic - 10.0
    parser._suspect_list[path] = (expiry, mtime) 
    heapq.heappush(parser._suspect_heap, (expiry, path))
    parser._last_suspect_cleanup_time = 0.0 # Ensure cleanup runs
    
    # Act: trigger cleanup
    parser._cleanup_expired_suspects_unlocked()
        
    # Assert: Active file (mtime 1000 -> 1005) should be RENEWED
    assert path in parser._suspect_list
    new_expiry, new_recorded_mtime = parser._suspect_list[path]
    assert new_recorded_mtime == 1005.0
    assert parser._get_node(path).integrity_suspect is True

@pytest.mark.asyncio
async def test_no_reset_ttl_on_unchanged_mtime(parser):
    path = "/steady.txt"
    mtime = 1000.0
    parser._logical_clock.update(mtime + 1.0)
    
    # First report
    payload = {'file_path': path, 'modified_time': mtime, 'size': 100, 'is_dir': False}
    event = UpdateEvent(
        event_schema="nfs", 
        table="files", 
        rows=[payload], 
        fields=list(payload.keys()), 
        message_source=MessageSource.AUDIT, 
        index=int((mtime+1)*1000)
    )
    await parser.process_event(event)
    
    initial_expiry, _ = parser._suspect_list[path]
    
    # Wait a bit
    await asyncio.sleep(0.01)
    
    # Second report: same mtime
    await parser.process_event(event)
    
    # Assert: Expiry should NOT have changed!
    new_expiry, _ = parser._suspect_list[path]
    assert new_expiry == initial_expiry
