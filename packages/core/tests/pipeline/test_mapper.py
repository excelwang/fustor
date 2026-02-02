import pytest
from logging import getLogger
from fustor_core.pipeline.mapper import EventMapper

logger = getLogger(__name__)

class TestEventMapper:
    def test_basic_renaming(self):
        config = [
            {"source": ["user_id"], "to": "id"}
        ]
        mapper = EventMapper(config)
        event = {"user_id": 123, "name": "Alice"}
        result = mapper.process(event)
        assert result == {"id": 123}

    def test_nested_target(self):
        # Test target side dot notation which is supported
        mapper = EventMapper([
            {"source": ["age"], "to": "meta.user_age"}
        ])
        event = {"age": 30}
        result = mapper.process(event)
        assert result == {"meta": {"user_age": 30}}

    def test_type_conversion(self):
        config = [
            {"source": ["count:int"], "to": "count"}
        ]
        mapper = EventMapper(config)
        event = {"count": "42"}
        result = mapper.process(event)
        assert result == {"count": 42}

    def test_hardcoded_value(self):
        config = [
            {"hardcoded_value": "test_source", "to": "meta.source"}
        ]
        mapper = EventMapper(config)
        event = {"data": "foo"}
        result = mapper.process(event)
        
        # Verify both original data (if preserved? current implementation creates new dict) and mapped data
        # actually current implementation creates a NEW dict 'processed_data' and returns it.
        # So 'data' field would be lost unless mapped.
        assert result == {"meta": {"source": "test_source"}}

    def test_map_batch(self):
        config = [{"source": ["a"], "to": "b"}]
        mapper = EventMapper(config)
        
        # Test valid object with rows
        class MockEvent:
            def __init__(self, rows):
                self.rows = rows
        
        batch = [MockEvent([{"a": 1}, {"a": 2}])]
        mapped_batch = mapper.map_batch(batch)
        assert mapped_batch[0].rows == [{"b": 1}, {"b": 2}]

    def test_empty_mapping(self):
        mapper = EventMapper([])
        event = {"a": 1}
        assert mapper.process(event) == event
