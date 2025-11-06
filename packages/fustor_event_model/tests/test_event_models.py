import pytest
from pydantic import ValidationError
from fustor_event_model.models import EventBase

def test_event_base():
    event = EventBase(content={"key": "value"})
    assert event.content == {"key": "value"}

    event_default = EventBase()
    assert event_default.content == {}

    with pytest.raises(ValidationError):
        EventBase(content="not a dict")
