import pytest
from unittest.mock import AsyncMock, MagicMock, patch
from fustor_sender_openapi import OpenApiDriver, _spec_cache
from fustor_core.exceptions import DriverError

SAMPLE_SPEC = {
    "servers": [{"url": "/api/v1"}],
    "paths": {
        "/events/session": {
            "post": {"responses": {"200": {}}}
        },
        "/events/batch": {
            "post": {
                "requestBody": {
                    "content": {"application/json": {"schema": {}}}
                },
                "responses": {"200": {}}
            }
        },
        "/events/heartbeat": {
            "post": {"responses": {"200": {}}}
        }
    }
}

class MockEventObj:
    def model_dump(self, mode=None):
        return {"event_type": "INSERT", "table": "t", "rows": []}

@pytest.fixture
def mock_httpx_client():
    with patch("httpx.AsyncClient") as MockClient:
        client_instance = MockClient.return_value
        client_instance.__aenter__.return_value = client_instance
        client_instance.__aexit__.return_value = None
        
        async def mock_request(method, url, **kwargs):
            mock_resp = MagicMock()
            mock_resp.status_code = 200
            
            # Simple routing
            if method == "GET" and "openapi.json" in url:
                mock_resp.json.return_value = SAMPLE_SPEC
            elif method == "POST":
                if "session" in url:
                    mock_resp.json.return_value = {"session_id": "sess-1", "role": "leader"}
                else:
                    mock_resp.json.return_value = {"success": True}
            else:
                mock_resp.json.return_value = {}
                
            mock_resp.raise_for_status = MagicMock()
            return mock_resp
            
        client_instance.request.side_effect = lambda method, url, **kwargs: mock_request(method, url, **kwargs)
        client_instance.get.side_effect = lambda url, **kwargs: mock_request("GET", url, **kwargs)
        client_instance.post.side_effect = lambda url, **kwargs: mock_request("POST", url, **kwargs)
        
        yield client_instance

@pytest.fixture
def driver(mock_httpx_client):
    _spec_cache.clear()
    credential = {"api_key": "test-key"}
    return OpenApiDriver("test-openapi", "http://localhost/openapi.json", credential)

@pytest.mark.asyncio
async def test_create_session(driver, mock_httpx_client):
    result = await driver.create_session("task-1")
    assert result["session_id"] == "sess-1"
    
    mock_httpx_client.get.assert_called_with("http://localhost/openapi.json", timeout=10.0)
    
    # Check that session called
    posted_urls = [call[0][0] for call in mock_httpx_client.post.call_args_list]
    assert any("events/session" in url for url in posted_urls)

@pytest.mark.asyncio
async def test_send_events_impl(driver, mock_httpx_client):
    driver.session_id = "sess-1"
    events = [MockEventObj()]
    
    result = await driver._send_events_impl(events)
    
    assert result.get("success") is True
    
    # Check that batch called
    posted_urls = [call[0][0] for call in mock_httpx_client.post.call_args_list]
    assert any("events/batch" in url for url in posted_urls)

@pytest.mark.asyncio
async def test_heartbeat(driver, mock_httpx_client):
    driver.session_id = "sess-1"
    
    await driver.heartbeat()
    
    # Check that heartbeat called
    posted_urls = [call[0][0] for call in mock_httpx_client.post.call_args_list]
    assert any("events/heartbeat" in url for url in posted_urls)

@pytest.mark.asyncio
async def test_connect(driver):
    # Should be no-op but logs
    await driver.connect()

