import pytest
import asyncio
from httpx import AsyncClient, ASGITransport
from unittest.mock import patch, AsyncMock, MagicMock
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
        session_timeout_seconds=30,
        meta={"type": "live"}
    )

@pytest.fixture
def mock_normal_config():
    return DatastoreConfig(
        datastore_id=1,
        allow_concurrent_push=True,
        session_timeout_seconds=30,
        meta={"type": "persistent"}
    )

@pytest.mark.asyncio
async def test_live_datastore_503_without_sessions(mock_live_config):
    """验证 Live 存储库在没有活跃 Session 时返回 503"""
    from fustor_fusion.auth.dependencies import get_datastore_id_from_api_key
    app.dependency_overrides[get_datastore_id_from_api_key] = mock_get_datastore_id
    
    # 设置 Snapshot 已完成，排除 Snapshot 导致的 503
    await datastore_state_manager.set_authoritative_session(1, "dummy")
    await datastore_state_manager.set_snapshot_complete(1, "dummy")

    # 模拟 Config 为 live
    with patch.object(datastore_config_cache, 'get_datastore_config', return_value=mock_live_config):
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
async def test_live_datastore_200_with_sessions(mock_live_config):
    """验证 Live 存储库在有活跃 Session 时返回 200"""
    from fustor_fusion.auth.dependencies import get_datastore_id_from_api_key
    app.dependency_overrides[get_datastore_id_from_api_key] = mock_get_datastore_id
    
    await datastore_state_manager.set_authoritative_session(1, "s1")
    await datastore_state_manager.set_snapshot_complete(1, "s1")

    with patch.object(datastore_config_cache, 'get_datastore_config', return_value=mock_live_config):
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
async def test_normal_datastore_200_without_sessions(mock_normal_config):
    """验证非 Live 存储库在没有 Session 时依然可以访问 (只要同步已完成)"""
    from fustor_fusion.auth.dependencies import get_datastore_id_from_api_key
    app.dependency_overrides[get_datastore_id_from_api_key] = mock_get_datastore_id
    
    await datastore_state_manager.set_authoritative_session(1, "dummy")
    await datastore_state_manager.set_snapshot_complete(1, "dummy")

    with patch.object(datastore_config_cache, 'get_datastore_config', return_value=mock_normal_config):
        if 1 in session_manager._sessions:
            del session_manager._sessions[1]
            
        async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
            response = await client.get("/api/v1/views/test/status_check")
            assert response.status_code == 200

    app.dependency_overrides.clear()
    await datastore_state_manager.clear_state(1)

@pytest.mark.asyncio
async def test_live_session_cleanup_resets_tree(mocker):
    """验证 Live 存储库在最后一个 Session 退出时重置目录树"""
    datastore_id = 99
    session_id = "s99"
    
    # 使用独立的 SessionManager 实例避免干扰全局
    sm = SessionManager(default_session_timeout=60)
    
    mock_config = DatastoreConfig(
        datastore_id=datastore_id,
        allow_concurrent_push=True,
        session_timeout_seconds=30,
        meta={"type": "live"}
    )
    
    # 模拟依赖
    mocker.patch('fustor_fusion.auth.datastore_cache.datastore_config_cache.get_datastore_config', return_value=mock_config)
    mock_reset = mocker.patch('fustor_fusion.view_manager.manager.reset_views', new_callable=AsyncMock)
    mocker.patch('fustor_fusion.in_memory_queue.memory_event_queue.clear_datastore_data', new_callable=AsyncMock)

    # 1. 创建 Session
    si = await sm.create_session_entry(datastore_id, session_id)
    assert datastore_id in sm._sessions
    
    # 2. 终止 Session
    await sm.terminate_session(datastore_id, session_id)
    
    # 3. 验证 reset_views 被调用
    mock_reset.assert_called_once_with(datastore_id)
    assert datastore_id not in sm._sessions
    
    # Cleanup background task
    si.cleanup_task.cancel()
    try:
        await si.cleanup_task
    except asyncio.CancelledError:
        pass
