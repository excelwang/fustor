import pytest
import asyncio
from httpx import AsyncClient, ASGITransport
from unittest.mock import patch, MagicMock, AsyncMock
from fustor_fusion.main import app
from fustor_fusion.datastore_state_manager import datastore_state_manager
from fustor_fusion.processing_manager import processing_manager
from fustor_fusion.in_memory_queue import memory_event_queue
from fustor_event_model.models import EventBase, EventType

# 模拟 API Key 认证
async def mock_get_datastore_id():
    return 1

@pytest.fixture
def client_override():
    from fustor_fusion.auth.dependencies import get_datastore_id_from_api_key
    app.dependency_overrides[get_datastore_id_from_api_key] = mock_get_datastore_id
    with patch("fustor_fusion.api.views.get_directory_tree", new_callable=AsyncMock) as m:
        m.return_value = {"name": "root"}
        yield AsyncClient(transport=ASGITransport(app=app), base_url="http://test")
    app.dependency_overrides.clear()

@pytest.mark.asyncio
async def test_readiness_logic_full_chain(client_override):
    """
    核心测试：验证 READY 状态的三个必要条件
    1. Snapshot 信号已到
    2. 队列已空
    3. Inflight 处理数为 0
    """
    ds_id = 1
    session_id = "test-session"
    
    # 初始化状态：未同步
    await datastore_state_manager.clear_state(ds_id)
    await memory_event_queue.clear_datastore_data(ds_id)
    
    # 状态 1: 没有任何信号 -> 503
    res = await client_override.get("/api/v1/views/fs/tree")
    assert res.status_code == 503
    assert "sync in progress" in res.json()["detail"]

    # 状态 2: 收到信号，但队列里还有数据 -> 503
    await datastore_state_manager.set_authoritative_session(ds_id, session_id)
    await datastore_state_manager.set_snapshot_complete(ds_id, session_id)
    
    # 模拟队列堆积
    fake_event = EventBase(
        event_type=EventType.INSERT, event_schema="ds1", table="fs", 
        rows=[{"path": "/test"}], fields=["path"], index=123
    )
    await memory_event_queue.add_event(ds_id, fake_event)
    
    res = await client_override.get("/api/v1/views/fs/tree")
    assert res.status_code == 503
    # 检查日志（模拟判定逻辑）
    assert memory_event_queue.get_queue_size(ds_id) == 1

    # 状态 3: 队列空了，但正在解析中 (Inflight) -> 503
    # 手动清空队列，但手动设置 inflight
    await memory_event_queue.clear_datastore_data(ds_id)
    
    # 模拟 ProcessingManager 正在工作
    with patch.object(processing_manager, "get_inflight_count", return_value=5):
        res = await client_override.get("/api/v1/views/fs/tree")
        assert res.status_code == 503
        assert "still processing" in res.text.lower()

    # 状态 4: 全部完成 -> 200
    with patch.object(processing_manager, "get_inflight_count", return_value=0):
        res = await client_override.get("/api/v1/views/fs/tree")
        assert res.status_code == 200

@pytest.mark.asyncio
async def test_dynamic_processor_activation():
    """验证 ProcessingManager 能够动态激活新的 Datastore 处理器"""
    ds_id = 999
    # 初始无任务
    await processing_manager.stop_all()
    assert ds_id not in processing_manager._tasks
    
    # 触发确保逻辑
    await processing_manager.ensure_processor(ds_id)
    
    assert ds_id in processing_manager._tasks
    assert not processing_manager._tasks[ds_id].done()
    
    await processing_manager.stop_all()

@pytest.mark.asyncio
async def test_inflight_counter_increment_decrement():
    """验证处理循环中 inflight 计数器的准确性"""
    ds_id = 2
    
    # 模拟一个会阻塞的处理过程
    process_started = asyncio.Event()
    can_finish = asyncio.Event()

    async def slow_process(event, d_id):
        process_started.set()
        await can_finish.wait()
        return True

    # 准备数据
    ev = EventBase(event_type=EventType.INSERT, event_schema="x", table="y", rows=[{}], fields=[], index=1)
    await memory_event_queue.add_event(ds_id, ev)

    # 启动后台处理
    with patch("fustor_fusion.processing_manager.process_single_event", side_effect=slow_process):
        task = asyncio.create_task(processing_manager._per_datastore_processing_loop(ds_id))
        
        # 等待处理开始
        await asyncio.wait_for(process_started.wait(), timeout=2)
        
        # 此时数据已出队，但未处理完，inflight 应为 1
        assert memory_event_queue.get_queue_size(ds_id) == 0
        assert processing_manager.get_inflight_count(ds_id) == 1
        
        # 允许处理结束
        can_finish.set()
        await asyncio.sleep(0.1)
        
        # 处理完后，inflight 应回落到 0
        assert processing_manager.get_inflight_count(ds_id) == 0
        
        task.cancel()
