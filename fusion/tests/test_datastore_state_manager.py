import pytest
import asyncio
from datetime import datetime
from fustor_fusion.datastore_state_manager import DatastoreStateManager, DatastoreState

@pytest.fixture
def state_manager():
    return DatastoreStateManager()

@pytest.mark.asyncio
async def test_initial_state(state_manager):
    """验证初始状态下快照未完成"""
    assert await state_manager.is_snapshot_complete(1) is False

@pytest.mark.asyncio
async def test_snapshot_completion_logic(state_manager):
    """验证快照完成判定的核心逻辑"""
    ds_id = 1
    session_a = "session-alpha"
    session_b = "session-beta"

    # 1. 设置权威会话
    await state_manager.set_authoritative_session(ds_id, session_a)
    # 尚未标记完成，应为 False
    assert await state_manager.is_snapshot_complete(ds_id) is False

    # 2. 正确标记完成
    await state_manager.set_snapshot_complete(ds_id, session_a)
    # ID 匹配，应为 True
    assert await state_manager.is_snapshot_complete(ds_id) is True

    # 3. 新会话抢占权威
    await state_manager.set_authoritative_session(ds_id, session_b)
    # 此时完成的是旧会话 A，权威是 B，数据应被视为“过期/未就绪”，返回 False
    assert await state_manager.is_snapshot_complete(ds_id) is False

    # 4. 新会话完成
    await state_manager.set_snapshot_complete(ds_id, session_b)
    assert await state_manager.is_snapshot_complete(ds_id) is True

@pytest.mark.asyncio
async def test_dirty_signal_defense(state_manager):
    """验证脏信号防御：非权威会话发出的完成信号不应使系统变为就绪态"""
    ds_id = 1
    authoritative_session = "real-master"
    stale_session = "old-zombie"

    # 设置权威
    await state_manager.set_authoritative_session(ds_id, authoritative_session)
    
    # 僵尸会话尝试标记完成
    await state_manager.set_snapshot_complete(ds_id, stale_session)
    
    # 系统判定逻辑应识别出 ID 不匹配
    assert await state_manager.is_snapshot_complete(ds_id) is False
    
    # 只有真正的权威标记完成才生效
    await state_manager.set_snapshot_complete(ds_id, authoritative_session)
    assert await state_manager.is_snapshot_complete(ds_id) is True

@pytest.mark.asyncio
async def test_state_persistence_in_memory(state_manager):
    """验证更新操作不会丢失其他状态字段"""
    ds_id = 1
    session_id = "sess-123"
    
    await state_manager.set_state(ds_id, status='ACTIVE', locked_by_session_id='locker')
    await state_manager.set_authoritative_session(ds_id, session_id)
    await state_manager.set_snapshot_complete(ds_id, session_id)
    
    state = await state_manager.get_state(ds_id)
    assert state.status == 'ACTIVE'
    assert state.locked_by_session_id == 'locker'
    assert state.authoritative_session_id == session_id
    assert state.completed_snapshot_session_id == session_id
