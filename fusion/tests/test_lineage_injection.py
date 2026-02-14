import pytest
from unittest.mock import AsyncMock, MagicMock, patch
from fustor_fusion.runtime.fusion_pipe import FusionPipe
from fustor_core.event import EventBase
from fustor_fusion.core.session_manager import SessionManager, session_manager
from fustor_fusion_sdk.interfaces import SessionInfo

@pytest.mark.asyncio
async def test_lineage_injection_flow():
    # 1. Setup FusionPipe with a mock handler
    mock_handler = MagicMock()
    mock_handler.id = "fs-handler"
    mock_handler.schema_name = "fs"
    mock_handler.initialize = AsyncMock()
    mock_handler.on_session_start = AsyncMock()
    mock_handler.on_session_close = AsyncMock()
    mock_handler.close = AsyncMock()
    mock_handler.process_event = AsyncMock(return_value=True)
    
    pipe = FusionPipe(pipe_id="v1", config={"view_ids": ["v1"]}, view_handlers=[mock_handler])
    await pipe.start()
    
    from fustor_fusion.runtime.session_bridge import create_session_bridge
    bridge = create_session_bridge(pipe)
    
    # 2. Setup a session with lineage info
    sid = "sess-abc"
    tid = "agent-XYZ:task-1"
    uri = "nfs://server/share"
    
    # Create session via bridge so lineage is recorded in pipe's store
    await bridge.create_session(
        task_id=tid, 
        session_id=sid, 
        source_uri=uri
    )
    
    # 3. Process an event and verify lineage injection
    # Use a valid EventBase dict structure
    event_dict = {
        "event_id": "e1",
        "event_type": "insert",
        "event_schema": "fs",
        "table": "files",
        "fields": ["path", "size"],
        "rows": [["/file.txt", 100]],
        "metadata": {} # Prevent AttributeError
    }
    
    await pipe.process_events([event_dict], session_id=sid)
    
    # Drain the pipe queue
    await pipe.wait_for_drain(timeout=1.0)
    
    # 4. Check if the handler received the event with injected metadata
    assert mock_handler.process_event.called
    processed_event = mock_handler.process_event.call_args[0][0]
    
    assert processed_event.metadata["agent_id"] == "agent-XYZ"
    assert processed_event.metadata["source_uri"] == "nfs://server/share"
    
    await pipe.stop()
