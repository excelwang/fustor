# agent/tests/runtime/test_bus_backpressure.py
"""
U5: EventBus 背压策略 — Transient 事件在 Buffer 满时被丢弃。

被测代码: agent/src/fustor_agent/runtime/bus.py → MemoryEventBus.put(is_transient=True)
"""
import pytest
import asyncio
from fustor_agent.runtime.bus import MemoryEventBus
from fustor_core.event import EventBase
from fustor_core.event.types import EventType
from fustor_core.exceptions import TransientSourceBufferFullError


def _make_event(index: int) -> EventBase:
    return EventBase(
        event_type=EventType.INSERT,
        event_schema="s", table="t",
        rows=[{"id": index}], fields=["id"], index=index
    )


class TestBusBackpressure:

    @pytest.mark.asyncio
    async def test_transient_event_dropped_when_buffer_full(self):
        """Buffer 满时，transient 事件应抛 TransientSourceBufferFullError。"""
        bus = MemoryEventBus(bus_id="test", capacity=2, start_position=0)
        await bus.subscribe("task1", 0, [])

        # 填满 buffer
        await bus.put(_make_event(0))
        await bus.put(_make_event(1))

        # Transient 事件应被拒绝
        with pytest.raises(TransientSourceBufferFullError):
            await bus.put(_make_event(2), is_transient=True)

    @pytest.mark.asyncio
    async def test_non_transient_event_blocks_when_buffer_full(self):
        """Buffer 满时，非 transient 事件应阻塞等待空间。"""
        bus = MemoryEventBus(bus_id="test", capacity=1, start_position=0)
        await bus.subscribe("task1", 0, [])

        await bus.put(_make_event(0))

        # 启动一个非 transient put（应阻塞）
        put_task = asyncio.create_task(bus.put(_make_event(1)))

        await asyncio.sleep(0.05)
        assert not put_task.done(), "非 transient 事件应阻塞等待 Buffer 空间"

        # 消费一个事件释放空间
        await bus.commit("task1", 1, 0)
        await asyncio.sleep(0.05)
        assert put_task.done(), "消费事件后 Producer 应被唤醒"
