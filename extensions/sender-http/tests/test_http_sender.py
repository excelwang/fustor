import pytest
from unittest.mock import AsyncMock, MagicMock, patch
from fustor_sender_http import HTTPSender
from fustor_core.event import EventBase
from fustor_core.exceptions import SessionObsoletedError
import httpx

class MockEvent(EventBase):
    def model_dump(self, mode=None):
        return {"id": 1, "data": "test"}

@pytest.fixture
def mock_fusion_client():
    with patch("fustor_fusion_sdk.client.FusionClient") as MockClient:
        client_instance = MockClient.return_value
        client_instance.create_session = AsyncMock()
        client_instance.push_events = AsyncMock()
        client_instance.send_heartbeat = AsyncMock()
        client_instance.terminate_session = AsyncMock()
        client_instance.signal_audit_start = AsyncMock()
        client_instance.signal_audit_end = AsyncMock()
        yield client_instance

@pytest.fixture
def sender(mock_fusion_client):
    credential = {"api_key": "test-key"}
    return HTTPSender("test-sender", "http://localhost", credential)

@pytest.mark.asyncio
async def test_init(sender, mock_fusion_client):
    assert sender.id == "test-sender"
    assert sender.credential["api_key"] == "test-key"
    # Verify client init happened (implicitly via fixture)

@pytest.mark.asyncio
async def test_create_session(sender, mock_fusion_client):
    mock_fusion_client.create_session.return_value = {
        "session_id": "sess-1",
        "role": "leader",
        "session_timeout_seconds": 60
    }
    
    session_id, result = await sender.create_session("task-1", "snapshot", 60)
    
    assert result["session_id"] == "sess-1"
    assert session_id == "sess-1"
    assert sender.session_id == "sess-1"
    mock_fusion_client.create_session.assert_called_once_with("task-1", source_type="snapshot", session_timeout_seconds=60, client_info={})

@pytest.mark.asyncio
async def test_create_session_failure(sender, mock_fusion_client):
    mock_fusion_client.create_session.return_value = None
    
    with pytest.raises(RuntimeError, match="Failed to create session"):
        await sender.create_session("task-1")

@pytest.mark.asyncio
async def test_send_events_success(sender, mock_fusion_client):
    sender.session_id = "sess-1"
    mock_fusion_client.push_events.return_value = True
    events = [{"event_type": "INSERT", "table": "t", "rows": []}]
    
    result = await sender._send_events_impl(events, "message", False)
    
    assert result["success"] is True
    mock_fusion_client.push_events.assert_called_once()
    args, kwargs = mock_fusion_client.push_events.call_args
    assert kwargs["session_id"] == "sess-1"
    assert len(kwargs["events"]) == 1

@pytest.mark.asyncio
async def test_send_events_no_session(sender):
    # No session_id set
    result = await sender._send_events_impl([], "message")
    assert result["success"] is False
    assert result["error"] == "No session"

@pytest.mark.asyncio
async def test_send_events_obsolete_session(sender, mock_fusion_client):
    sender.session_id = "sess-1"
    # Simulate 419 error
    request = httpx.Request("POST", "http://locahost")
    response = httpx.Response(419, request=request)
    mock_fusion_client.push_events.side_effect = httpx.HTTPStatusError("obsolete", request=request, response=response)
    
    with pytest.raises(SessionObsoletedError):
        await sender._send_events_impl([{"event_type": "INSERT", "table": "t", "rows": []}])

@pytest.mark.asyncio
async def test_heartbeat_success(sender, mock_fusion_client):
    sender.session_id = "sess-1"
    mock_fusion_client.send_heartbeat.return_value = {"status": "ok", "role": "follower"}
    
    result = await sender.heartbeat()
    
    assert result["status"] == "ok"
    mock_fusion_client.send_heartbeat.assert_called_once_with("sess-1", can_realtime=False)

@pytest.mark.asyncio
async def test_audit_signals(sender, mock_fusion_client):
    mock_fusion_client.signal_audit_start.return_value = True
    mock_fusion_client.signal_audit_end.return_value = True
    
    assert await sender.signal_audit_start() is True
    assert await sender.signal_audit_end() is True
    
    mock_fusion_client.signal_audit_start.assert_called_once_with("test-sender")
    mock_fusion_client.signal_audit_end.assert_called_once_with("test-sender")
