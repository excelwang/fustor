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
    
    pipe = FusionPipe(pipe_id="v1", config={"view_id": "v1"}, view_handlers=[mock_handler])
    await pipe.start()
    
    # 2. Setup a session with lineage info
    sid = "sess-abc"
    si = SessionInfo(
        session_id=sid, view_id="v1", task_id="agent-XYZ:task-1",
        source_uri="nfs://server/share", last_activity=1, created_at=1
    )
    
    # Mock session_manager to return our session info
    with patch.object(session_manager, "get_session_info", return_value=si):
        await pipe.on_session_created(sid, task_id=si.task_id, is_leader=True)
        
        # 3. Process an event and verify lineage injection
        # Use a valid EventBase dict structure
        event_dict = {
            "event_id": "e1",
            "event_type": "insert",
            "event_schema": "fs",
            "path": "/file.txt",
            "payload": {"size": 100},
            # Mandatory fields for validation
            "table": "files",
            "fields": ["path", "size"],
            "rows": [["/file.txt", 100]]
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
