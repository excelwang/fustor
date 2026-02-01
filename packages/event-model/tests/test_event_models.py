import pytest
from pydantic import ValidationError
from fustor_event_model.models import EventBase, EventType

def test_event_base():
    event = EventBase(
        event_type=EventType.INSERT,
        fields=["col1"],
        rows=[{"col1": "val1"}],
        event_schema="test_schema",
        table="test_table",
        index=1
    )
    assert event.event_type == EventType.INSERT
    assert event.fields == ["col1"]
    assert event.rows == [{"col1": "val1"}]
    assert event.event_schema == "test_schema"
    assert event.table == "test_table"
    assert event.index == 1

    with pytest.raises(ValidationError):
        EventBase(fields=["col1"]) # Missing required fields

