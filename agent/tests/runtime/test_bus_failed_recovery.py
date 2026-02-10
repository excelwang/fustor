# agent/tests/runtime/test_bus_failed_recovery.py
"""
U4: EventBus Failed 状态的行为和恢复(不)能力。

被测代码: agent/src/fustor_agent/runtime/bus.py → MemoryEventBus.mark_as_failed()
"""
import pytest
import asyncio
from fustor_agent.runtime.bus import MemoryEventBus, EventBusFailedError
from fustor_core.event import EventBase
from fustor_core.event.types import EventType


def _make_event(index: int) -> EventBase:
    """Helper to create a minimal EventBase for testing."""
    return EventBase(
        event_type=EventType.INSERT,
        event_schema="s", table="t",
        rows=[{"id": index}], fields=["id"], index=index
    )


class TestBusFailedState:
    """验证 EventBus 进入 Failed 状态后的行为。"""

    @pytest.mark.asyncio
    async def test_failed_bus_rejects_all_puts(self):
        """验证: Failed Bus 拒绝所有新事件写入。"""
        bus = MemoryEventBus(bus_id="test", capacity=10, start_position=0)
        await bus.subscribe("task1", 0, [])

        # 正常写入
        await bus.put(_make_event(0))  # 应成功

        # 标记为 Failed
        bus.mark_as_failed("Simulated failure")

        # 后续写入应抛异常
        with pytest.raises(EventBusFailedError):
            await bus.put(_make_event(1))

    @pytest.mark.asyncio
    async def test_failed_bus_can_recover(self):
        """
        验证修复后: EventBus 提供了 recover() 方法恢复状态。
        """
        bus = MemoryEventBus("test", 10, 0)
        bus.mark_as_failed("Initial error")
        assert bus.failed

        # Use recover method
        bus.recover()

        assert not bus.failed
        assert bus.error_message is None

        # Verify put works after recovery
        await bus.put(_make_event(0))  # Should succeed

    @pytest.mark.asyncio
    async def test_failed_bus_wakes_blocked_producer(self):
        """验证: 等待中的 Producer 在 Bus fail 后感知到异常。"""
        bus = MemoryEventBus(bus_id="test", capacity=1, start_position=0)
        await bus.subscribe("task1", 0, [])

        # 填满 buffer
        await bus.put(_make_event(0))

        # 启动一个阻塞的 put
        async def blocked_put():
            await bus.put(_make_event(1))

        put_task = asyncio.create_task(blocked_put())
        await asyncio.sleep(0.05)  # 让 put 阻塞

        # 在 Producer 等待时标记 Bus 为 Failed
        bus.mark_as_failed("Runtime error")

        # Producer 应收到 EventBusFailedError
        with pytest.raises(EventBusFailedError):
            await put_task
