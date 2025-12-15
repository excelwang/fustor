import pytest
import asyncio
from fustor_agent.runtime.bus import MemoryEventBus, EventBusFailedError
from fustor_event_model.models import EventBase, InsertEvent
from fustor_core.models.config import FieldMapping

@pytest.mark.asyncio
class TestMemoryEventBus:
    async def test_put_and_get_events(self):
        bus = MemoryEventBus(bus_id="test_bus", capacity=10, start_position=0)
        event1 = InsertEvent(event_schema="s", table="t", rows=[{'id': 1}], index=0)
        event2 = InsertEvent(event_schema="s", table="t", rows=[{'id': 2}], index=1)

        await bus.subscribe("task1", 0, [])
        await bus.put(event1)
        await bus.put(event2)

        events = await bus.get_events_for("task1", 10, 0.1)
        assert len(events) == 2
        assert events[0].index == 0
        assert events[1].index == 1

    async def test_buffer_capacity(self):
        bus = MemoryEventBus(bus_id="test_bus", capacity=2, start_position=0)
        await bus.put(InsertEvent(event_schema="s", table="t", rows=[{'id': 1}], index=0))
        await bus.put(InsertEvent(event_schema="s", table="t", rows=[{'id': 2}], index=1))

        put_task = asyncio.create_task(bus.put(InsertEvent(event_schema="s", table="t", rows=[{'id': 3}], index=2)))
        
        await asyncio.sleep(0.01) # Give the put task time to block
        assert len(bus.buffer) == 2
        assert not put_task.done()

        await bus.commit("task1", 1, 0)
        await asyncio.sleep(0.01) # Give the put task time to unblock

        assert put_task.done()
        assert len(bus.buffer) == 2
        assert bus.buffer[0].index == 1
        assert bus.buffer[1].index == 2

    async def test_mark_as_failed(self):
        bus = MemoryEventBus(bus_id="test_bus", capacity=10, start_position=0)
        bus.mark_as_failed("Test failure")
        assert bus.failed is True
        with pytest.raises(EventBusFailedError):
            await bus.put(InsertEvent(event_schema="s", table="t", rows=[{'id': 1}], index=0))

    async def test_subscribe_and_unsubscribe(self):
        bus = MemoryEventBus(bus_id="test_bus", capacity=10, start_position=0)
        assert bus.get_subscriber_count() == 0
        await bus.subscribe("task1", 0, [FieldMapping(to="a.b", source=["c.d.e:0"])])
        assert bus.get_subscriber_count() == 1
        assert "c.d.e" in bus.required_fields
        await bus.unsubscribe("task1")
        assert bus.get_subscriber_count() == 0
        assert not bus.required_fields

    async def test_subscribe_with_empty_fields_mapping_for_all_fields(self):
        """Tests that subscribing with an empty fields_mapping correctly sets required_fields to None (ALL_FIELDS)."""
        bus = MemoryEventBus(bus_id="test_bus_all_fields", capacity=10, start_position=0)
        
        # Subscribe with an empty fields_mapping
        await bus.subscribe("task_all", 0, [])
        
        # Assert that required_fields is None
        assert bus.required_fields is None
        
        # Test with another subscriber that has specific fields
        await bus.subscribe("task_specific", 0, [FieldMapping(to="a.b", source=["c.d.e:0"])])
        assert bus.required_fields is None # Still None because 'task_all' needs all

        await bus.unsubscribe("task_all")
        assert bus.required_fields == {"c.d.e"} # Now only specific fields are needed

        await bus.unsubscribe("task_specific")
        assert bus.required_fields == set() # No subscribers, so no fields needed

    async def test_trim_buffer(self):
        bus = MemoryEventBus(bus_id="test_bus", capacity=10, start_position=0)
        await bus.subscribe("task1", 0, [])
        await bus.subscribe("task2", 0, [])

        for i in range(5):
            await bus.put(InsertEvent(event_schema="s", table="t", rows=[{'id': i}], index=i))
        
        assert len(bus.buffer) == 5
        await bus.commit("task1", 3, 2)
        assert len(bus.buffer) == 5 # No trim yet

        await bus.commit("task2", 2, 1)
        # low_watermark is now min(2, 1) = 1
        # Events with index <= 1 should be trimmed (0, 1)
        assert len(bus.buffer) == 3
        assert bus.buffer[0].index == 2

    async def test_check_for_split(self):
        bus = MemoryEventBus(bus_id="test_bus", capacity=10, start_position=0)
        await bus.subscribe("fast_consumer", 0, [])
        await bus.subscribe("slow_consumer", 0, [])

        for i in range(10):
            await bus.put(InsertEvent(event_schema="s", table="t", rows=[{'id': 6}], index=6))

        # Slow consumer stays at the start
        await bus.commit("slow_consumer", 1, 0)
        
        # Fast consumer moves to the end
        task_to_split = await bus.commit("fast_consumer", 8, 8)

        # 90% of capacity (9 events) are backlogged for the slow consumer
        assert task_to_split == "fast_consumer"

    async def test_can_subscribe(self):
        bus = MemoryEventBus(bus_id="test_bus", capacity=10, start_position=5)
        for i in range(5, 10):
            await bus.put(InsertEvent(event_schema="s", table="t", rows=[{'id': i}], index=i))
        await bus.put(InsertEvent(event_schema="s", table="t", rows=[{'id': 5}], index=5))

        assert bus.can_subscribe(4) is False
        assert bus.can_subscribe(5) is True
        assert bus.can_subscribe(6) is True
        assert bus.can_subscribe(7) is False

    async def test_update_subscriber_position(self):
        bus = MemoryEventBus(bus_id="test_bus", capacity=10, start_position=0)
        await bus.subscribe("task1", 0, [])
        await bus.update_subscriber_position("task1", 100)
        assert bus.subscribers["task1"]['last_consumed_index'] == 100
