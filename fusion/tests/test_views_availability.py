import pytest
import pytest_asyncio
from httpx import AsyncClient, ASGITransport
from unittest.mock import patch, AsyncMock
from fustor_fusion.main import app
from fustor_fusion.view_state_manager import view_state_manager

# 模拟 API Key 认证，直接返回 view_id = 1
async def mock_get_view_id():
    return "test"

@pytest_asyncio.fixture
async def client():
    # 覆盖认证依赖
    from fustor_fusion.auth.dependencies import get_view_id_from_api_key
    app.dependency_overrides[get_view_id_from_api_key] = lambda: "test"
    
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as c:
        c.headers["X-API-Key"] = "test-key"
        yield c
    
    app.dependency_overrides.clear()

@pytest_asyncio.fixture(autouse=True)
async def mock_view_deps():
    """每个测试前 Mock ViewManager 和相关驱动"""
    from unittest.mock import MagicMock, AsyncMock, patch
    mock_vm = MagicMock()
    mock_driver = MagicMock()
    mock_driver.is_ready = True
    mock_vm.driver_instances = {"test_driver": mock_driver}
    
    with patch("fustor_fusion.api.views.get_cached_view_manager", new_callable=AsyncMock) as mock_get:
        mock_get.return_value = mock_vm
        yield mock_vm, mock_driver

@pytest_asyncio.fixture(autouse=True)
async def clean_state():
    """每个测试前清空状态管理器"""
    await view_state_manager.clear_state("test")
    await view_state_manager.clear_state("test_driver")
    yield

@pytest.mark.asyncio
async def test_api_unavailable_initially(client):
    """验证初始状态下接口返回 503"""
    # Use generic status check endpoint
    response = await client.get("/api/v1/views/test/status_check")
    assert response.status_code == 503
    assert "No active leader session" in response.json()["detail"]

@pytest.mark.asyncio
async def test_api_unavailable_during_sync_phase(client):
    """验证同步进行中（有权威但未完成）返回 503"""
    await view_state_manager.set_authoritative_session("test", "session-1")
    await view_state_manager.set_authoritative_session("test_driver", "session-1")
    
    response = await client.get("/api/v1/views/test/status_check")
    assert response.status_code == 503
    assert "Initial snapshot sync phase in progress" in response.json()["detail"]

@pytest.mark.asyncio
async def test_api_available_after_sync_phase_complete(client):
    """验证同步完成后接口正常工作"""
    session_id = "session-1"
    await view_state_manager.set_authoritative_session("test", session_id)
    await view_state_manager.set_snapshot_complete("test", session_id)
    await view_state_manager.set_authoritative_session("test_driver", session_id)
    await view_state_manager.set_snapshot_complete("test_driver", session_id)
    
    response = await client.get("/api/v1/views/test/status_check")
    assert response.status_code == 200

@pytest.mark.asyncio
async def test_api_re_locks_on_new_session(client):
    """验证新同步开始后，接口重新变为不可用"""
    session_old = "session-old"
    session_new = "session-new"
    
    # 1. 旧会话完成，接口可用
    await view_state_manager.set_authoritative_session("test", session_old)
    await view_state_manager.set_snapshot_complete("test", session_old)
    await view_state_manager.set_authoritative_session("test_driver", session_old)
    await view_state_manager.set_snapshot_complete("test_driver", session_old)
    
    res = await client.get("/api/v1/views/test/status_check")
    assert res.status_code == 200
    
    # 2. 新会话启动，接口应立即变为 503
    await view_state_manager.set_authoritative_session("test", session_new)
    await view_state_manager.set_authoritative_session("test_driver", session_new)
    response = await client.get("/api/v1/views/test/status_check")
    assert response.status_code == 503
    
    # 3. 新会话完成后重新可用
    await view_state_manager.set_snapshot_complete("test", session_new)
    await view_state_manager.set_snapshot_complete("test_driver", session_new)
    res = await client.get("/api/v1/views/test/status_check")
    assert res.status_code == 200