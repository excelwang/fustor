import asyncio
import pytest
from unittest.mock import MagicMock, AsyncMock, patch
from fustor_core.pipe import PipeState
from fustor_core.exceptions import SessionObsoletedError
from fustor_agent.runtime.agent_pipe import AgentPipe

@pytest.fixture
def mock_pipe():
    config = {
        "source": "src",
        "sender": "push",
        "audit_interval_sec": 0.1,
        "sentinel_interval_sec": 0.1
    }
    source_h = MagicMock()
    sender_h = AsyncMock()
    pipe = AgentPipe("test-pipe", "agent:test-pipe", config, source_h, sender_h)
    pipe.session_id = "s123"
    pipe.state = PipeState.RUNNING
    pipe.current_role = "leader"
    return pipe

@pytest.mark.asyncio
async def test_run_leader_sequence_snapshot_failure(mock_pipe):
    """测试 Snapshot 启动失败路径"""
    mock_pipe._run_snapshot_sync = MagicMock()  # Use MagicMock to avoid unawaited coroutine warning when create_task fails
    with patch("asyncio.create_task", side_effect=RuntimeError("spawn fail")):
        await mock_pipe._run_leader_sequence()
        assert mock_pipe.state == PipeState.ERROR
        assert "Failed to start snapshot sync phase" in mock_pipe.info

@pytest.mark.asyncio
async def test_snapshot_sync_fatal_error_propagation(mock_pipe):
    """测试 Snapshot 内部发生致命错误"""
    mock_pipe._handle_fatal_error = AsyncMock()
    
    # 模拟 phases.run_snapshot_sync 抛出异常
    with patch("fustor_agent.runtime.pipe.phases.run_snapshot_sync", new_callable=AsyncMock) as mock_run:
        mock_run.side_effect = ValueError("fatal")
        await mock_pipe._run_snapshot_sync()
        mock_pipe._handle_fatal_error.assert_called_once()
        assert mock_pipe._initial_snapshot_done is False

@pytest.mark.asyncio
async def test_audit_loop_role_switch(mock_pipe):
    """测试 Audit 循环在角色切换时的行为"""
    mock_pipe.current_role = "follower"
    mock_pipe.role_check_interval = 0.01
    
    # 启动循环
    task = asyncio.create_task(mock_pipe._run_audit_loop())
    
    await asyncio.sleep(0.05)
    # 应该是等待状态，没有执行 audit_sync
    assert mock_pipe._audit_task is None 
    
    # 停止 Pipe 以结束循环
    mock_pipe.state = PipeState.STOPPED
    await task

@pytest.mark.asyncio
async def test_audit_loop_session_obsoleted(mock_pipe):
    """测试 Audit 循环处理 Session 过期"""
    mock_pipe._run_audit_sync = AsyncMock(side_effect=SessionObsoletedError("obsolete"))
    mock_pipe._handle_fatal_error = AsyncMock()
    mock_pipe.audit_interval_sec = 0.01
    
    task = asyncio.create_task(mock_pipe._run_audit_loop())
    await asyncio.sleep(0.05)
    
    mock_pipe._handle_fatal_error.assert_called_once()
    mock_pipe.state = PipeState.STOPPED # 结束测试
    await task

@pytest.mark.asyncio
async def test_trigger_audit_not_leader(mock_pipe, caplog):
    """测试非 Leader 触发 Audit 警告"""
    mock_pipe.current_role = "follower"
    await mock_pipe.trigger_audit()
    assert "Cannot trigger audit, not a leader" in caplog.text

@pytest.mark.asyncio
async def test_cancel_leader_tasks(mock_pipe):
    """测试取消所有 Leader 任务"""
    async def slow_task():
        try:
            await asyncio.sleep(10)
        except asyncio.CancelledError:
            pass

    s_task = asyncio.create_task(slow_task())
    a_task = asyncio.create_task(slow_task())
    sn_task = asyncio.create_task(slow_task())
    
    mock_pipe._snapshot_task = s_task
    mock_pipe._audit_task = a_task
    mock_pipe._sentinel_task = sn_task
    
    await mock_pipe._cancel_leader_tasks()
    
    assert s_task.cancelled()
    assert a_task.cancelled()
    assert sn_task.cancelled()
    
    assert mock_pipe._snapshot_task is None
    assert mock_pipe._audit_task is None
    assert mock_pipe._sentinel_task is None
