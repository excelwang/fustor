import pytest
import time
from fustor_fusion.parsers.file_directory_parser import DirectoryStructureParser
from fustor_event_model.models import EventBase, EventType, MessageSource, UpdateEvent, DeleteEvent

@pytest.mark.asyncio
async def test_arbitration_logic():
    parser = DirectoryStructureParser(datastore_id=1)
    
    # 1. Snapshot event (old data)
    old_time = time.time() - 1000
    rows = [{"path": "/test/file1", "modified_time": old_time, "size": 100}]
    evt1 = UpdateEvent(table="files", rows=rows, index=1, fields=[], message_source=MessageSource.SNAPSHOT, event_schema="s")
    await parser.process_event(evt1)
    
    node = parser._get_node("/test/file1")
    assert node.size == 100
    
    # 2. Realtime event (newer data)
    new_time = time.time()
    rows2 = [{"path": "/test/file1", "modified_time": new_time, "size": 200}]
    evt2 = UpdateEvent(table="files", rows=rows2, index=2, fields=[], message_source=MessageSource.REALTIME, event_schema="s")
    await parser.process_event(evt2)
    
    node = parser._get_node("/test/file1")
    assert node.size == 200
    assert node.modified_time == new_time
    
    # 3. Snapshot event (older than current) - Should be ignored due to Mtime check
    rows3 = [{"path": "/test/file1", "modified_time": old_time, "size": 300}]
    evt3 = UpdateEvent(table="files", rows=rows3, index=3, fields=[], message_source=MessageSource.SNAPSHOT, event_schema="s")
    await parser.process_event(evt3)
    
    node = parser._get_node("/test/file1")
    assert node.size == 200 # Should NOT change to 300
    
    # 4. Realtime Delete
    del_evt = DeleteEvent(table="files", rows=[{"path": "/test/file1"}], index=4, fields=[], message_source=MessageSource.REALTIME, event_schema="s")
    await parser.process_event(del_evt)
    
    node = parser._get_node("/test/file1")
    assert node is None
    assert "/test/file1" in parser._tombstone_list
    
    # 5. Snapshot resurrect check - Should be ignored due to Tombstone
    await parser.process_event(evt1) # Resend old snapshot
    node = parser._get_node("/test/file1")
    assert node is None # Still dead

@pytest.mark.asyncio
async def test_audit_sentinel_logic():
    parser = DirectoryStructureParser(datastore_id=1)
    
    # 1. Audit Start
    await parser.handle_audit_start()
    assert parser._last_audit_start is not None
    
    # 2. Audit Event (Update)
    now = time.time()
    rows = [{"path": "/test/audit_file", "modified_time": now, "size": 500}]
    evt = UpdateEvent(table="files", rows=rows, index=1000, fields=[], message_source=MessageSource.AUDIT, event_schema="s")
    await parser.process_event(evt)
    
    node = parser._get_node("/test/audit_file")
    assert node.size == 500
    # Audit events mark files as agent_missing=True
    assert node.agent_missing == True
    
    # 3. Sentinel Suspect List
    # Check if hot file is in suspect list
    # logic: if (now - mtime) < HOT_FILE_THRESHOLD_SECONDS (600): suspect=True
    assert node.integrity_suspect == True
    assert "/test/audit_file" in parser._suspect_list
    
    # 4. Audit End (Cleanup)
    # Create a tombstone OLDER than audit start (simulating pre-audit deletion)
    original_start_time = parser._last_audit_start
    parser._tombstone_list["/d/old"] = original_start_time - 10
    parser._tombstone_list["/d/new"] = original_start_time + 10
    
    await parser.handle_audit_end()
    
    assert "/d/old" not in parser._tombstone_list
    assert "/d/new" in parser._tombstone_list
    assert parser._last_audit_start is None

@pytest.mark.asyncio
async def test_auto_audit_start():
    parser = DirectoryStructureParser(datastore_id=1)
    assert parser._last_audit_start is None
    
    now_ms = int(time.time() * 1000)
    rows = [{"path": "/test/auto", "modified_time": 0, "size": 0}]
    evt = UpdateEvent(table="files", rows=rows, index=now_ms, fields=[], message_source=MessageSource.AUDIT, event_schema="s")
    
    await parser.process_event(evt)
    
    assert parser._last_audit_start is not None
    assert abs(parser._last_audit_start - now_ms/1000.0) < 0.1
