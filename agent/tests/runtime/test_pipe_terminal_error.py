# agent/tests/runtime/test_pipe_terminal_error.py
"""
U1: max_consecutive_errors 触发后 Pipe 的行为。


被测代码: agent/src/fustor_agent/runtime/agent_pipe.py → _handle_loop_error() + _run_control_loop()

验证重点:
1. 连续错误达到阈值时，Pipe 进入终态 (ERROR) 并停止循环。
2. 错误未达阈值时恢复，计数器重置。
"""
import pytest
import asyncio
from unittest.mock import AsyncMock
from fustor_core.pipe import PipeState
from fustor_core.exceptions import FusionConnectionError
from fustor_agent.runtime.agent_pipe import AgentPipe
from .mocks import MockSourceHandler, MockSenderHandler


@pytest.fixture
def pipe_with_low_max_errors():
    """创建一个 max_consecutive_errors=3 的 Pipe。"""
    src = MockSourceHandler()
    snd = MockSenderHandler()
    config = {
        "heartbeat_interval_sec": 0.01,
        "error_retry_interval": 0.01,
        "backoff_multiplier": 1.5,
        "max_backoff_seconds": 0.05,
        "max_consecutive_errors": 3,
        "batch_size": 10,
    }
    pipe = AgentPipe("test-pipe", "agent:pipe", config, src, snd)
    return pipe, src, snd


class TestPipeTerminalError:
    """验证 Pipe 在达到 max_consecutive_errors 后的行为。"""

    @pytest.mark.asyncio
    async def test_handle_error_sets_error_state_at_threshold(
        self, pipe_with_low_max_errors
    ):
        """
        验证: 达到错误阈值时，_handle_loop_error 设置 PipeState.ERROR。
        """
        pipe, _, _ = pipe_with_low_max_errors
        pipe.max_consecutive_errors = 3
        
        # Simulate N errors
        for i in range(pipe.max_consecutive_errors):
            pipe._handle_loop_error(Exception(f"Error {i+1}"), "test")
            
        assert pipe.state == PipeState.ERROR, \
            "_handle_error 应在 max_consecutive_errors 处设置 ERROR"

    @pytest.mark.asyncio
    async def test_pipe_exits_after_max_errors(
        self, pipe_with_low_max_errors
    ):
        """
        验证: 达到 max_consecutive_errors 后，Pipe 进入 ERROR 态并停止运行。
        """
        pipe, src, snd = pipe_with_low_max_errors
        # Force value to ensure config isn't ignored/overridden
        pipe.max_consecutive_errors = 3
        
        snd.create_session = AsyncMock(
            side_effect=FusionConnectionError("Connection refused")
        )

        await pipe.start()
        # 等待足够时间让 3 次错误 + 退避完成
        await asyncio.sleep(1.0)

        # 修复验证: Pipe 应停止 (is_running() == False)
        assert not pipe.is_running(), \
            "Pipe 应在达到 max_consecutive_errors 后停止运行 (进入终态 ERROR)"

        # 错误计数确实已超过阈值
        assert pipe._consecutive_errors >= pipe.max_consecutive_errors, \
            "错误计数应已达到阈值"

        # ERROR 位应已设置
        assert pipe.state & PipeState.ERROR, \
            "ERROR 位应已设置"

        # RECONNECTING 位不应设置
        assert not (pipe.state & PipeState.RECONNECTING), \
            "RECONNECTING 位不应在终态设置"

        await pipe.stop()

    @pytest.mark.asyncio
    async def test_pipe_recovers_if_errors_clear_before_threshold(
        self, pipe_with_low_max_errors
    ):
        """
        验证: 在达到阈值之前恢复，错误计数应重置。
        """
        pipe, src, snd = pipe_with_low_max_errors

        call_count = 0
        async def flaky_create_session(*args, **kwargs):
            nonlocal call_count
            call_count += 1
            if call_count <= 2:
                raise FusionConnectionError("Connection refused")
            return ("recovered-session", {"role": "leader"})

        snd.create_session = AsyncMock(side_effect=flaky_create_session)

        await pipe.start()
        await asyncio.sleep(0.5)

        assert pipe._consecutive_errors == 0, \
            "成功恢复后错误计数应重置为 0"
        assert pipe.session_id == "recovered-session"

        await pipe.stop()
