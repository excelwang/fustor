import pytest
import asyncio
import time
from fustor_view_fs import FSViewProvider
from fustor_event_model.models import UpdateEvent, MessageSource, DeleteEvent

@pytest.fixture
def parser():
    return FSViewProvider(datastore_id="1", view_id="test_view")

@pytest.mark.asyncio
async def test_audit_late_start_signal(parser):
    """
    Test scenario where an Audit event arrives slightly BEFORE the handle_audit_start signal.
    Fusion should auto-detect audit start and handle_audit_start should preserve existing state.
    """
    now = time.time()
    # 1. Audit event arrives out-of-order
    await parser.process_event(UpdateEvent(
        table="files",
        rows=[{"path": "/audit/file.txt", "modified_time": now, "size": 100}],
        index=int(now * 1000),
        fields=[],
        message_source=MessageSource.AUDIT,
        event_schema="s"
    ))
    
    assert parser._last_audit_start is not None, "Audit start should be auto-detected"
    first_detected_start = parser._last_audit_start
    assert "/audit/file.txt" in parser._audit_seen_paths
    
    # 2. handle_audit_start signal arrives shortly after
    await asyncio.sleep(0.1)
    await parser.handle_audit_start()
    
    # Verify: _last_audit_start is updated, but _audit_seen_paths is NOT cleared (late start protection)
    assert parser._last_audit_start >= first_detected_start
    assert "/audit/file.txt" in parser._audit_seen_paths, "Late start should preserve observed paths"

@pytest.mark.asyncio
async def test_sentinel_sweep_verification_flow(parser):
    """
    Test Sentinel Sweep: Suspect file marked, then verified, then cleared.
    """
    now = time.time()
    parser.hot_file_threshold = 1.0 # 1 second for test
    
    # 1. Audit discovery of hot file -> Mark Suspect
    await parser.process_event(UpdateEvent(
        table="files",
        rows=[{"path": "/hot/file.txt", "modified_time": now, "size": 100}],
        index=int(now * 1000),
        fields=[],
        message_source=MessageSource.AUDIT,
        event_schema="s"
    ))
    
    node = parser._get_node("/hot/file.txt")
    assert node.integrity_suspect is True
    assert "/hot/file.txt" in parser._suspect_list
    
    # 2. Sentinel Sweep: Report verified (mtime same)
    # Note: simulate wait so it becomes 'cold' relative to logical clock
    parser._logical_clock.update(now + 2.0)
    await parser.update_suspect("/hot/file.txt", now)
    
    # 3. Verify: Suspect cleared because it's now stable/cold
    assert node.integrity_suspect is False
    assert "/hot/file.txt" not in parser._suspect_list

@pytest.mark.asyncio
async def test_sentinel_sweep_mtime_mismatch(parser):
    """
    Test Sentinel Sweep: Mtime mismatch updates node and extends suspect window.
    """
    now = time.time()
    parser.hot_file_threshold = 10.0
    
    await parser.process_event(UpdateEvent(
        table="files",
        rows=[{"path": "/hot/mismatch.txt", "modified_time": now, "size": 100}],
        fields=[],
        message_source=MessageSource.AUDIT,
        event_schema="s"
    ))
    
    # Sentinel finds it modified again
    new_mtime = now + 5.0
    await parser.update_suspect("/hot/mismatch.txt", new_mtime)
    
    node = parser._get_node("/hot/mismatch.txt")
    assert node.modified_time == new_mtime
    assert node.integrity_suspect is True, "Still hot, window extended"

@pytest.mark.asyncio
async def test_audit_vs_tombstone_precedence(parser):
    """
    Verify Rule 2: Tombstone protects against 'missing' deletion in Audit.
    If a file is missing in Audit but has a valid Tombstone, we keep the Tombstone.
    """
    now = time.time()
    
    # 1. Realtime Delete -> Create Tombstone
    await parser.process_event(DeleteEvent(
        table="files",
        rows=[{"path": "/del/zombie.txt"}],
        fields=[],
        message_source=MessageSource.REALTIME,
        event_schema="s"
    ))
    assert "/del/zombie.txt" in parser._tombstone_list
    
    # 2. Audit Start & Scan parent dir D
    await parser.handle_audit_start()
    # Mock parent scan
    parser._audit_seen_paths.add("/del")
    
    # 3. Audit End
    await parser.handle_audit_end()
    
    # 4. Verify: File is NOT added to blind_spot_deletions 
    # (because handle_audit_end skips paths in _tombstone_list)
    assert "/del/zombie.txt" not in parser._blind_spot_deletions
    assert "/del/zombie.txt" in parser._tombstone_list

@pytest.mark.asyncio
async def test_rule3_protection_old_mtime_injection(parser):
    """
    Test Rule 3: Stale Evidence Protection.
    Ensures that a file created/updated via Realtime AFTER an audit started
    is NOT deleted by that audit, even if the file has an old mtime (e.g., cp -p).
    """
    # Setup: Parent directory exists
    await parser.process_event(UpdateEvent(
        table="dirs",
        rows=[{"path": "/data", "modified_time": 900, "is_dir": True}],
        index=900000,
        fields=[],
        message_source=MessageSource.REALTIME,
        event_schema="s"
    ))
    
    # 1. Audit Start at T=1000
    parser._logical_clock.update(1000)
    await parser.handle_audit_start()
    assert parser._last_audit_start == 1000
    
    # 2. Realtime Event at T=1100 (Logical Watermark)
    # This represents a 'cp -p' where mtime is old (500)
    parser._logical_clock.update(1100)
    await parser.process_event(UpdateEvent(
        table="files",
        rows=[{"path": "/data/copied_file.txt", "modified_time": 500, "size": 100}],
        index=1100000,
        fields=[],
        message_source=MessageSource.REALTIME,
        event_schema="s"
    ))
    
    node = parser._get_node("/data/copied_file.txt")
    assert node is not None
    assert node.last_updated_at == 1100
    assert node.modified_time == 500
    
    # 3. Audit End
    # The audit didn't see the file (because it scanned /data before the copy happened)
    # But it DID scan /data
    parser._audit_seen_paths.add("/data")
    
    await parser.handle_audit_end()
    
    # 4. Verification
    # Even though mtime (500) < audit_start (1000), 
    # Rule 3 should preserve it because last_updated_at (1100) > audit_start (1000)
    node_after = parser._get_node("/data/copied_file.txt")
    assert node_after is not None, "File should be preserved by Rule 3 protection"
    assert "/data/copied_file.txt" not in parser._blind_spot_deletions
