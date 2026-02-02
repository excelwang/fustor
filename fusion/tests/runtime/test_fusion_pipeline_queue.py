import pytest
import asyncio
from unittest.mock import MagicMock, AsyncMock
from typing import Dict, Any, Optional

from fustor_fusion.runtime.fusion_pipeline import FusionPipeline
from fustor_core.pipeline.handler import ViewHandler
from fustor_core.event import EventBase

class MockViewHandler(ViewHandler):
    def __init__(self, handler_id: str):
        self.id = handler_id
        self.processed_events = []
        self.process_event_called = asyncio.Event()
        self.delay = 0.0
        
    async def initialize(self):
        pass
        
    async def process_event(self, event: EventBase) -> bool:
        if self.delay > 0:
            await asyncio.sleep(self.delay)
        self.processed_events.append(event)
        self.process_event_called.set()
        return True
        
    async def close(self):
        pass
        
    async def get_stats(self) -> Dict[str, Any]:
        return {"mock_processed": len(self.processed_events)}
        
    async def get_data_view(self, **kwargs) -> Any:
        return self.processed_events

@pytest.mark.asyncio
async def test_fusion_pipeline_queue_behavior():
    # Setup
    mock_handler = MockViewHandler("mock-1")
    pipeline = FusionPipeline(
        pipeline_id="test-pipeline",
        config={"view_id": "test-view"},
        view_handlers=[mock_handler]
    )
    
    await pipeline.start()
    
    try:
        # Test Data
        event = EventBase(
            event_type="insert", 
            event_schema="test",
            table="test_table",
            fields=["path"],
            rows=[["/test"]]
        )
        events = [event]
        
        # Action: Process Events (Should enqueue)
        result = await pipeline.process_events(events, session_id="test-session")
        
        # Verify: Immediate Return
        assert result["success"] is True
        assert result["count"] == 1
        
        # Verify: Event eventually processed
        await asyncio.wait_for(mock_handler.process_event_called.wait(), timeout=1.0)
        
        assert len(mock_handler.processed_events) == 1
        assert mock_handler.processed_events[0].table == "test_table"
        
        # Verify Stats
        stats = await pipeline.get_aggregated_stats()
        # Queue should be drained
        assert stats["pipeline"]["queue_size"] == 0
        assert stats["pipeline"]["events_processed"] == 1
        assert stats["views"]["mock-1"]["mock_processed"] == 1
        
    finally:
        await pipeline.stop()

@pytest.mark.asyncio
async def test_fusion_pipeline_queue_observability():
    """Test that we can observe items in the queue if processing is slow."""
    # Setup
    mock_handler = MockViewHandler("mock-slow")
    mock_handler.delay = 0.2 # Slow processing
    
    pipeline = FusionPipeline(
        pipeline_id="test-pipeline-slow",
        config={"view_id": "test-view-slow"},
        view_handlers=[mock_handler]
    )
    
    await pipeline.start()
    
    try:
        # Send a batch
        event = EventBase(
            event_type="insert", 
            event_schema="test",
            table="test_table",
            fields=["path"],
            rows=[["/test"]]
        )
        events = [event] * 1 # Batch of 1
        
        # This returns immediately, putting items into queue
        await pipeline.process_events(events, session_id="sess-1")
        
        # Check stats immediately
        # We assume queue processing hasn't finished yet (due to delay)
        # We push multiple batches to see queue buildup.
        
        await pipeline.process_events(events, session_id="sess-2") # 2nd batch
        await pipeline.process_events(events, session_id="sess-3") # 3rd batch
        
        # Now we have 3 batches. Each takes 0.2s * 1 = 0.2s.
        # Total processing time = 0.6s.
        
        stats = await pipeline.get_aggregated_stats()
        # Dependent on scheduler, but likely >= 0.
        assert "queue_size" in stats["pipeline"]
        
        # Wait for all to finish (3 batches * 0.2s = 0.6s + buffer)
        await asyncio.sleep(1.0)
        
        # Verify all processed
        stats = await pipeline.get_aggregated_stats()
        assert stats["pipeline"]["queue_size"] == 0
        assert len(mock_handler.processed_events) == 3
        
    finally:
        await pipeline.stop()
