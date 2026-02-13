import pytest
import sys
import os

# Add fusion/src to path if running from root
sys.path.insert(0, os.path.abspath("fusion/src"))

from fustor_fusion.models.event import EventCreate, EventResponse
from fustor_core.event import EventBase, EventType, MessageSource # Import EventType and MessageSource

def test_event_create_model():
    """Test instantiation and basic properties of EventCreate."""
    event_data = {
        "event_id": "test-123",
        "timestamp": 1678886400,
        "event_type": EventType.INSERT, 
        "fields": ["key"],
        "rows": [["value"]],
        "event_schema": "test_schema",
        "table": "test_table",
        "message_source": MessageSource.REALTIME,
        "metadata": {"source": "test"}
    }
    event = EventCreate(**event_data)
    
    assert isinstance(event, EventCreate)
    assert isinstance(event, EventBase) # Verify inheritance
    assert event.event_id == "test-123"
    assert event.timestamp == 1678886400
    assert event.event_type == EventType.INSERT 
    assert event.fields == ["key"]
    assert event.rows == [["value"]]
    assert event.event_schema == "test_schema"
    assert event.table == "test_table"
    assert event.message_source == MessageSource.REALTIME
    assert event.metadata == {"source": "test"}


def test_event_response_model():
    """Test instantiation and basic properties of EventResponse."""
    event_data = {
        "event_id": "response-456",
        "timestamp": 1678886500,
        "id": "response-id-1", # EventResponse adds 'id' field
        "event_type": EventType.UPDATE,
        "fields": ["status"],
        "rows": [["success"]],
        "event_schema": "response_schema",
        "table": "response_table",
        "message_source": MessageSource.REALTIME,
        "metadata": None
    }
    event = EventResponse(**event_data)
    
    assert isinstance(event, EventResponse)
    assert isinstance(event, EventBase) # Verify inheritance from EventBase
    assert event.event_id == "response-456"
    assert event.timestamp == 1678886500
    assert event.event_type == EventType.UPDATE
    assert event.fields == ["status"]
    assert event.rows == [["success"]]
    assert event.event_schema == "response_schema"
    assert event.table == "response_table"
    assert event.id == "response-id-1"
    assert event.message_source == MessageSource.REALTIME
    assert event.metadata is None

def test_event_response_missing_id():
    """Test EventResponse raises error if 'id' is missing."""
    event_data = {
        "event_id": "response-456",
        "timestamp": 1678886500,
        "event_type": EventType.UPDATE,
        "fields": ["status"],
        "rows": [["success"]],
        "event_schema": "response_schema",
        "table": "response_table",
        "message_source": MessageSource.REALTIME,
        "metadata": None
    }
    # Construct event_data without 'id' from the start
    event_data_without_id = {k: v for k, v in event_data.items() if k != "id"}
    
    with pytest.raises(ValueError, match="Field required"):
        EventResponse(**event_data_without_id)
