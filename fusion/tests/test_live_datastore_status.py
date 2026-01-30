import pytest
import asyncio
from httpx import AsyncClient, ASGITransport
from unittest.mock import patch, AsyncMock, MagicMock, PropertyMock
from fustor_fusion.main import app
from fustor_fusion.datastore_state_manager import datastore_state_manager
from fustor_fusion.core.session_manager import SessionManager, session_manager
from fustor_fusion.auth.datastore_cache import datastore_config_cache
from fustor_common.models import DatastoreConfig

# Mock API Key authentication: always returns datastore_id = 1
async def mock_get_datastore_id():
    return 1

@pytest.fixture
def mock_live_config():
    return DatastoreConfig(
        datastore_id=1,
        allow_concurrent_push=True,
        session_timeout_seconds=30
        # meta removed
    )

@pytest.fixture
def mock_normal_config():
    return DatastoreConfig(
        datastore_id=1,
        allow_concurrent_push=True,
        session_timeout_seconds=30
        # meta removed
    )

@pytest.fixture
def mock_view_manager():
    manager = MagicMock()
    provider = MagicMock()
    
    # Explicitly mock the property to return False by default (Persistent)
    # This prevents MagicMock from returning a truthy Mock object when accessing this attribute
    p = PropertyMock(return_value=False)
    type(provider).requires_full_reset_on_session_close = p
    
    # Default config
    provider.config = {}
    manager.providers = {"test_driver": provider}
    
    # Manager must be awaitable if used with await get_cached_view_manager
    return manager, provider

@pytest.mark.asyncio
async def test_live_datastore_503_without_sessions(mock_live_config, mock_view_manager):
    """验证 Live 存储库在没有活跃 Session 时返回 503"""
    from fustor_fusion.auth.dependencies import get_datastore_id_from_api_key
    app.dependency_overrides[get_datastore_id_from_api_key] = mock_get_datastore_id
    
    manager, provider = mock_view_manager
    provider.config = {"mode": "live"} # Set Live Mode via View Config
    # Also set the property check to true for robust testing
    type(provider).requires_full_reset_on_session_close = PropertyMock(return_value=True)

    # 设置 Snapshot 已完成，排除 Snapshot 导致的 503
    await datastore_state_manager.set_authoritative_session(1, "dummy")
    await datastore_state_manager.set_snapshot_complete(1, "dummy")

    # Mock Config (generic) and View Manager (specific)
    # Patching api.views reference because it is imported at module level there
    with patch.object(datastore_config_cache, 'get_datastore_config', return_value=mock_live_config), \
         patch("fustor_fusion.api.views.get_cached_view_manager", new_callable=AsyncMock) as mock_get_vm:
        
        mock_get_vm.return_value = manager

        # 确保 session_manager 中没有 datastore 1 的 session
        if 1 in session_manager._sessions:
            del session_manager._sessions[1]
            
        async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
            response = await client.get("/api/v1/views/test/status_check")
            assert response.status_code == 503
            assert "No active sessions for this live datastore" in response.json()["detail"]

    app.dependency_overrides.clear()
    await datastore_state_manager.clear_state(1)

@pytest.mark.asyncio
async def test_live_datastore_200_with_sessions(mock_live_config, mock_view_manager):
    """验证 Live 存储库在有活跃 Session 时返回 200"""
    from fustor_fusion.auth.dependencies import get_datastore_id_from_api_key
    app.dependency_overrides[get_datastore_id_from_api_key] = mock_get_datastore_id
    
    manager, provider = mock_view_manager
    provider.config = {"mode": "live"}
    type(provider).requires_full_reset_on_session_close = PropertyMock(return_value=True)

    await datastore_state_manager.set_authoritative_session(1, "s1")
    await datastore_state_manager.set_snapshot_complete(1, "s1")

    with patch.object(datastore_config_cache, 'get_datastore_config', return_value=mock_live_config), \
         patch("fustor_fusion.api.views.get_cached_view_manager", new_callable=AsyncMock) as mock_get_vm:
        
        mock_get_vm.return_value = manager

        # 创建一个 session
        await session_manager.create_session_entry(1, "s1")
        
        async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
            response = await client.get("/api/v1/views/test/status_check")
            assert response.status_code == 200
        
        # 清理 session
        await session_manager.terminate_session(1, "s1")

    app.dependency_overrides.clear()
    await datastore_state_manager.clear_state(1)

@pytest.mark.asyncio
async def test_normal_datastore_200_without_sessions(mock_normal_config, mock_view_manager):
    """验证非 Live 存储库在没有 Session 时依然可以访问 (只要同步已完成)"""
    from fustor_fusion.auth.dependencies import get_datastore_id_from_api_key
    app.dependency_overrides[get_datastore_id_from_api_key] = mock_get_datastore_id
    
    manager, provider = mock_view_manager
    provider.config = {"mode": "persistent"} # Not live

    await datastore_state_manager.set_authoritative_session(1, "dummy")
    await datastore_state_manager.set_snapshot_complete(1, "dummy")

    with patch.object(datastore_config_cache, 'get_datastore_config', return_value=mock_normal_config), \
         patch("fustor_fusion.api.views.get_cached_view_manager", new_callable=AsyncMock) as mock_get_vm:
        
        mock_get_vm.return_value = manager

        if 1 in session_manager._sessions:
            del session_manager._sessions[1]
            
        async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
            response = await client.get("/api/v1/views/test/status_check")
            assert response.status_code == 200

    app.dependency_overrides.clear()
    await datastore_state_manager.clear_state(1)

@pytest.mark.asyncio
async def test_live_session_cleanup_resets_tree(mocker, mock_view_manager):
    """验证 Live 存储库在最后一个 Session 退出时重置目录树"""
    datastore_id = 99
    session_id = "s99"
    
    manager, provider = mock_view_manager
    provider.config = {"mode": "live"}
    # Explicitly set Live property for this test
    type(provider).requires_full_reset_on_session_close = PropertyMock(return_value=True)
    # providers needs to include iterator for values() used in _check_if_datastore_live
    manager.providers = {"test": provider}

    # 使用独立的 SessionManager 实例避免干扰全局
    sm = SessionManager(default_session_timeout=60)
    
    mock_config = DatastoreConfig(
        datastore_id=datastore_id,
        allow_concurrent_push=True,
        session_timeout_seconds=30
    )
    
    # 模拟依赖
    mocker.patch('fustor_fusion.auth.datastore_cache.datastore_config_cache.get_datastore_config', return_value=mock_config)
    mock_reset = mocker.patch('fustor_fusion.view_manager.manager.reset_views', new_callable=AsyncMock)
    mocker.patch('fustor_fusion.in_memory_queue.memory_event_queue.clear_datastore_data', new_callable=AsyncMock)
    
    # Needs to patch get_cached_view_manager called by session manager
    mock_get_vm = mocker.patch('fustor_fusion.view_manager.manager.get_cached_view_manager', new_callable=AsyncMock)
    mock_get_vm.return_value = manager

    # 1. 创建 Session
    si = await sm.create_session_entry(datastore_id, session_id)
    assert datastore_id in sm._sessions
    
    # 2. 终止 Session
    await sm.terminate_session(datastore_id, session_id)
    
    # 3. 验证 reset_views 被调用 (因为 provider.config rule)
    mock_reset.assert_called_once_with(datastore_id)
    assert datastore_id not in sm._sessions
    
    # Cleanup background task
    si.cleanup_task.cancel()
    try:
        await si.cleanup_task
    except asyncio.CancelledError:
        pass
