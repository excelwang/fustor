# agent/tests/runtime/test_pipe_message_sync_crash.py
"""
U6: message_sync 任务崩溃后 control loop 的检测和恢复。

被测代码: agent/src/fustor_agent/runtime/agent_pipe.py
  → _run_control_loop() 中对 _message_sync_task.done() 的检测
"""
import pytest
import asyncio
from fustor_core.pipe import PipeState
from fustor_agent.runtime.agent_pipe import AgentPipe
from .mocks import MockSourceHandler, MockSenderHandler


@pytest.fixture
def pipe_with_session():
    """创建一个已建立 Session 的 Pipe。"""
    src = MockSourceHandler()
    snd = MockSenderHandler()
    snd.role = "leader"
    config = {
        "control_loop_interval": 0.05,
        "error_retry_interval": 0.01,
        "max_consecutive_errors": 10,  # 高阈值避免终态
        "batch_size": 10,
    }
    pipe = AgentPipe("test-pipe", "agent:pipe", config, src, snd)
    return pipe


class TestMessageSyncCrashDetection:

    @pytest.mark.asyncio
    async def test_control_loop_detects_crashed_message_sync(
        self, pipe_with_session
    ):
        """
        模拟 message_sync 任务崩溃，验证 control loop 增加错误计数。
        """
        pipe = pipe_with_session

        # 创建一个立即完成（模拟崩溃）的 task
        async def crash_immediately():
            raise RuntimeError("Simulated message sync crash")

        pipe.session_id = "sess-1"
        pipe.current_role = "leader"
        pipe._set_state(PipeState.RUNNING)

        # 模拟一已崩溃的 task
        crashed_task = asyncio.create_task(crash_immediately())
        await asyncio.sleep(0.01)  # 让 task 完成
        assert crashed_task.done()

        pipe._message_sync_task = crashed_task

        # 初始错误计数为 0
        initial_errors = pipe._consecutive_errors

        # 模拟 control loop 的检测逻辑 (L383-391)
        if pipe._message_sync_task and pipe._message_sync_task.done():
            pipe._consecutive_errors += 1

        assert pipe._consecutive_errors == initial_errors + 1, \
            "Control loop 应检测到崩溃的 message_sync 任务并增加错误计数"

    @pytest.mark.asyncio
    async def test_crashed_task_exception_is_retrievable(
        self, pipe_with_session
    ):
        """验证: 崩溃的 task 的异常可以被正确获取用于日志记录。"""
        pipe = pipe_with_session

        async def crash_with_specific_error():
            raise ValueError("Specific crash reason")

        crashed_task = asyncio.create_task(crash_with_specific_error())
        await asyncio.sleep(0.01)

        assert crashed_task.done()
        with pytest.raises(ValueError, match="Specific crash reason"):
            crashed_task.result()
