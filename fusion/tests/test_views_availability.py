import pytest
import pytest_asyncio
from httpx import AsyncClient, ASGITransport
from unittest.mock import patch, AsyncMock
from fustor_fusion.main import app
from fustor_fusion.datastore_state_manager import datastore_state_manager

# 模拟 API Key 认证，直接返回 datastore_id = 1
async def mock_get_datastore_id():
    return 1

@pytest_asyncio.fixture
async def client():
    # 覆盖认证依赖
    from fustor_fusion.auth.dependencies import get_datastore_id_from_api_key
    app.dependency_overrides[get_datastore_id_from_api_key] = mock_get_datastore_id
    
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as c:
        yield c
    
    app.dependency_overrides.clear()

@pytest_asyncio.fixture(autouse=True)
async def clean_state():
    """每个测试前清空状态管理器"""
    await datastore_state_manager.clear_state(1)
    yield

@pytest.mark.asyncio
async def test_api_unavailable_initially(client):
    """验证初始状态下接口返回 503"""
    endpoints = [
        ("/views/fs/tree", {"path": "/"}),
        ("/views/fs/search", {"pattern": "test"}),
        ("/views/fs/stats", {}),
    ]
    
    for url, params in endpoints:
        response = await client.get(url, params=params)
        assert response.status_code == 503
        assert "Initial snapshot sync in progress" in response.json()["detail"]

@pytest.mark.asyncio
async def test_api_unavailable_during_sync(client):
    """验证同步进行中（有权威但未完成）返回 503"""
    await datastore_state_manager.set_authoritative_session(1, "session-1")
    
    response = await client.get("/views/fs/tree", params={"path": "/"})
    assert response.status_code == 503

@pytest.mark.asyncio
async def test_api_available_after_sync_complete(client):
    """验证同步完成后接口正常工作"""
    session_id = "session-1"
    await datastore_state_manager.set_authoritative_session(1, session_id)
    await datastore_state_manager.set_snapshot_complete(1, session_id)
    
    with patch("fustor_fusion.api.views.get_directory_tree", new_callable=AsyncMock) as mock_get:
        mock_get.return_value = {"name": "root"}
        response = await client.get("/views/fs/tree", params={"path": "/"})
        assert response.status_code == 200

@pytest.mark.asyncio
async def test_api_re_locks_on_new_session(client):
    """验证新同步开始后，接口重新变为不可用"""
    session_old = "session-old"
    session_new = "session-new"
    
    # 1. 旧会话完成，接口可用
    await datastore_state_manager.set_authoritative_session(1, session_old)
    await datastore_state_manager.set_snapshot_complete(1, session_old)
    
    with patch("fustor_fusion.api.views.get_directory_tree", new_callable=AsyncMock) as mock_get:
        mock_get.return_value = {}
        res = await client.get("/views/fs/tree")
        assert res.status_code == 200
    
    # 2. 新会话启动，接口应立即变为 503
    await datastore_state_manager.set_authoritative_session(1, session_new)
    response = await client.get("/views/fs/tree")
    assert response.status_code == 503
    
    # 3. 新会话完成后重新可用
    await datastore_state_manager.set_snapshot_complete(1, session_new)
    with patch("fustor_fusion.api.views.get_directory_tree", new_callable=AsyncMock) as mock_get:
        mock_get.return_value = {}
        res = await client.get("/views/fs/tree")
        assert res.status_code == 200