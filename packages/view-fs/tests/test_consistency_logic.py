import pytest
import time
from fustor_view_fs import FSViewProvider
from fustor_core.event import EventBase, EventType, MessageSource, UpdateEvent, DeleteEvent

@pytest.mark.asyncio
async def test_arbitration_logic():
    parser = FSViewProvider(id="test_view", view_id="1")

    
    # 1. Snapshot event (old data)
    old_time = time.time() - 1000
    rows = [{"path": "/test/file1", "modified_time": old_time, "size": 100}]
    evt1 = UpdateEvent(table="files", rows=rows, index=1, fields=[], message_source=MessageSource.SNAPSHOT, event_schema="s")
    await parser.process_event(evt1)
    
    node = parser.state.get_node("/test/file1")
    assert node.size == 100
    
    # 2. Realtime event (newer data)
    new_time = time.time()
    rows2 = [{"path": "/test/file1", "modified_time": new_time, "size": 200}]
    evt2 = UpdateEvent(table="files", rows=rows2, index=2, fields=[], message_source=MessageSource.REALTIME, event_schema="s")
    await parser.process_event(evt2)
    
    node = parser.state.get_node("/test/file1")
    assert node.size == 200
    assert node.modified_time == new_time
    
    # 3. Snapshot event (older than current) - Should be ignored due to Mtime check
    rows3 = [{"path": "/test/file1", "modified_time": old_time, "size": 300}]
    evt3 = UpdateEvent(table="files", rows=rows3, index=3, fields=[], message_source=MessageSource.SNAPSHOT, event_schema="s")
    await parser.process_event(evt3)
    
    node = parser.state.get_node("/test/file1")
    assert node.size == 200 # Should NOT change to 300
    
    # 4. Realtime Delete
    del_evt = DeleteEvent(table="files", rows=[{"path": "/test/file1"}], index=4, fields=[], message_source=MessageSource.REALTIME, event_schema="s")
    await parser.process_event(del_evt)
    
    node = parser.state.get_node("/test/file1")
    assert node is None
    assert "/test/file1" in parser.state.tombstone_list
    
    # 5. Snapshot resurrect check - Should be ignored due to Tombstone
    await parser.process_event(evt1) # Resend old snapshot
    node = parser.state.get_node("/test/file1")
    assert node is None # Still dead

@pytest.mark.asyncio
async def test_audit_sentinel_logic():
    parser = FSViewProvider(id="test_view", view_id="1")
    
    # 1. Init clock to now so last_audit_start is meaningful
    now = time.time()
    parser.state.logical_clock.update(now)

    # 2. Audit Start
    await parser.handle_audit_start()
    audit_start_time = parser.state.last_audit_start
    assert audit_start_time is not None
    
    # 3. Audit Event (Update)
    rows = [{"path": "/test/audit_file", "modified_time": now + 1, "size": 500}] # +1 to advance
    evt = UpdateEvent(table="files", rows=rows, index=2000, fields=[], message_source=MessageSource.AUDIT, event_schema="s")
    await parser.process_event(evt)
    
    node = parser.state.get_node("/test/audit_file")
    assert node.size == 500
    # Audit events mark files as blind-spot additions
    assert node.path in parser.state.blind_spot_additions
    
    # Sentinel check logic relies on hot_file_threshold (default 30s)
    # (clock - mtime) < 30 ? 
    # clock is now at now+1 (from row mtime update). row mtime is now+1. diff is 0. So Suspect.
    assert node.integrity_suspect == True
    assert "/test/audit_file" in parser.state.suspect_list
    
    # 4. Audit End (Cleanup)
    # Tombstone cleanup logic: Purge tombstones older than 1 HOUR
    # Current watermark is ~ now+1
    
    # Case A: Ancient tombstone (2 hours ago) -> Should be removed
    parser.state.tombstone_list["/d/ancient"] = (now - 7300, now - 7300)
    
    # Case B: Recent tombstone (1 minute ago) -> Should be kept
    parser.state.tombstone_list["/d/recent"] = (now - 60, now - 60) 
    
    await parser.handle_audit_end()
    
    # Assertions
    assert "/d/ancient" not in parser.state.tombstone_list, "Ancient tombstone should be cleaned"
    assert "/d/recent" in parser.state.tombstone_list, "Recent tombstone should be preserved"
    assert parser.state.last_audit_start is None


@pytest.mark.asyncio
async def test_auto_audit_start():
    parser = FSViewProvider(id="test_view", view_id="1")
    assert parser.state.last_audit_start is None
    
    now_ms = int(time.time() * 1000)
    rows = [{"path": "/test/auto", "modified_time": 0, "size": 0}]
    evt = UpdateEvent(table="files", rows=rows, index=now_ms, fields=[], message_source=MessageSource.AUDIT, event_schema="s")
    
    await parser.process_event(evt)
    
    assert parser.state.last_audit_start is not None
    assert abs(parser.state.last_audit_start - now_ms/1000.0) < 0.1

@pytest.mark.asyncio
async def test_parent_mtime_check():
    """Test Section 5.3: Parent Mtime Check for Audit events."""
    parser = FSViewProvider(id="test_view", view_id="1")
    
    # 1. First, establish a parent directory in memory via Realtime
    parent_realtime_mtime = time.time()
    parent_rows = [{"path": "/data", "modified_time": parent_realtime_mtime, "size": 0, "is_dir": True}]
    parent_evt = UpdateEvent(table="dirs", rows=parent_rows, index=1, fields=[], message_source=MessageSource.REALTIME, event_schema="s")
    await parser.process_event(parent_evt)
    
    parent_node = parser.state.directory_path_map.get("/data")
    assert parent_node is not None
    assert parent_node.modified_time == parent_realtime_mtime
    
    # 2. Now send an Audit event for a NEW file, but with an OLDER parent_mtime
    #    This simulates: Audit scanned /data before Realtime updated it
    old_parent_mtime = parent_realtime_mtime - 100  # Audit saw older parent
    file_mtime = time.time()
    audit_rows = [{
        "path": "/data/stale_file.txt",
        "modified_time": file_mtime,
        "size": 100,
        "parent_path": "/data",
        "parent_mtime": old_parent_mtime  # Key: older than memory's parent mtime
    }]
    audit_evt = UpdateEvent(table="files", rows=audit_rows, index=2000, fields=[], message_source=MessageSource.AUDIT, event_schema="s")
    await parser.process_event(audit_evt)
    
    # File should NOT be added because parent_mtime check failed
    file_node = parser.state.get_node("/data/stale_file.txt")
    assert file_node is None, "File should be discarded due to stale parent_mtime"
    
    # 3. Now send an Audit event with a CURRENT parent_mtime
    current_audit_rows = [{
        "path": "/data/valid_file.txt",
        "modified_time": file_mtime,
        "size": 200,
        "parent_path": "/data",
        "parent_mtime": parent_realtime_mtime  # Same as memory
    }]
    valid_audit_evt = UpdateEvent(table="files", rows=current_audit_rows, index=3000, fields=[], message_source=MessageSource.AUDIT, event_schema="s")
    await parser.process_event(valid_audit_evt)
    
    # File SHOULD be added
    valid_file_node = parser.state.get_node("/data/valid_file.txt")
    assert valid_file_node is not None, "File should be added when parent_mtime is current"
    assert valid_file_node.path in parser.state.blind_spot_additions  # New file from Audit = blind-spot

@pytest.mark.asyncio
async def test_audit_missing_file_detection():
    """Test Section 5.3 Scenario 2: Detecting files missing from audit."""
    parser = FSViewProvider(id="test_view", view_id="1")
    
    # 1. Create initial state via Realtime: parent dir + 2 files
    now = time.time()
    
    # Parent directory
    dir_evt = UpdateEvent(
        table="dirs", 
        rows=[{"path": "/project", "modified_time": now, "size": 0, "is_dir": True}],
        index=1, fields=[], message_source=MessageSource.REALTIME, event_schema="s"
    )
    await parser.process_event(dir_evt)
    
    # File A (will be seen in audit)
    file_a_evt = UpdateEvent(
        table="files",
        rows=[{"path": "/project/a.txt", "modified_time": now, "size": 100}],
        index=2, fields=[], message_source=MessageSource.REALTIME, event_schema="s"
    )
    await parser.process_event(file_a_evt)
    
    # File B (will be MISSING from audit - deleted in blind-spot)
    file_b_evt = UpdateEvent(
        table="files",
        rows=[{"path": "/project/b.txt", "modified_time": now, "size": 200}],
        index=3, fields=[], message_source=MessageSource.REALTIME, event_schema="s"
    )
    await parser.process_event(file_b_evt)
    
    # Verify initial state
    assert parser.state.get_node("/project/a.txt") is not None
    assert parser.state.get_node("/project/b.txt") is not None
    
    # 2. Start Audit
    await parser.handle_audit_start()
    assert len(parser.state.audit_seen_paths) == 0
    
    # 3. Send Audit events - only file A is reported (B was deleted on blind-spot node)
    audit_evt = UpdateEvent(
        table="files",
        rows=[{
            "path": "/project/a.txt", 
            "modified_time": now + 1,  # Slightly updated
            "size": 100,
            "parent_path": "/project",
            "parent_mtime": now
        }],
        index=int(now * 1000),
        fields=[],
        message_source=MessageSource.AUDIT,
        event_schema="s"
    )
    await parser.process_event(audit_evt)
    
    # Also "scan" the parent directory in audit
    parser.state.audit_seen_paths.add("/project")
    
    # 4. End Audit - should detect /project/b.txt as missing and DELETE it
    await parser.handle_audit_end()
    
    # 5. Verify: file B should be DELETED from memory tree (Section 5.3 Scenario 2)
    file_b = parser.state.get_node("/project/b.txt")
    assert file_b is None, "File B should be deleted from memory tree (blind-spot deletion)"
    
    # But tracked in blind-spot deletions
    assert "/project/b.txt" in parser.state.blind_spot_deletions
    
    # File A should still exist
    file_a = parser.state.get_node("/project/a.txt")
    assert file_a is not None, "File A should still exist"
