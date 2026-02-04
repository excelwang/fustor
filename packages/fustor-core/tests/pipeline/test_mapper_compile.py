import pytest
import logging
from unittest.mock import MagicMock, patch
from fustor_core.pipeline.mapper import EventMapper

class TestEventMapperCompile:
    """Targeted tests for the 'compilation' logic in EventMapper."""
    
    def test_float_conversion(self):
        config = [{"source": ["val:float"], "to": "out"}]
        mapper = EventMapper(config)
        assert mapper.process({"val": "1.23"}) == {"out": 1.23}
        assert mapper.process({"val": 42}) == {"out": 42.0}

    def test_boolean_conversion(self):
        config = [{"source": ["active:bool"], "to": "is_active"}]
        mapper = EventMapper(config)
        
        # True cases
        assert mapper.process({"active": "true"}) == {"is_active": True}
        assert mapper.process({"active": "1"}) == {"is_active": True}
        assert mapper.process({"active": "yes"}) == {"is_active": True}
        assert mapper.process({"active": True}) == {"is_active": True}
        
        # False cases
        assert mapper.process({"active": "false"}) == {"is_active": False}
        assert mapper.process({"active": "0"}) == {"is_active": False}
        assert mapper.process({"active": None}) == {} # None is skipped in process loop but let's check extractor
        
    def test_string_conversion(self):
        config = [{"source": ["id:str"], "to": "id"}]
        mapper = EventMapper(config)
        assert mapper.process({"id": 123}) == {"id": "123"}

    def test_invalid_type_conversion_fallback(self):
        # Implementation returns raw value if conversion fails
        config = [{"source": ["age:int"], "to": "age"}]
        mapper = EventMapper(config)
        # "abc" cannot be int, should fall back to "abc"
        assert mapper.process({"age": "abc"}) == {"age": "abc"}

    def test_path_conflict_avoidance(self):
        # Test what happens when we try to create a child under a leaf which is already a scalar
        config = [
            {"source": ["a"], "to": "root"},
            {"source": ["b"], "to": "root.child"}
        ]
        mapper = EventMapper(config)
        
        # We need a mock logger to verify warning
        l = MagicMock()
        result = mapper._mapper_func({"a": 1, "b": 2}, l)
        
        # 'root' was set to 1. Then 'root.child' attempted to use 'root' as dict.
        # Current implementation: current['root'] is 1. isinstance(current['root'], dict) is False.
        # It logs warning and breaks. Result should be {'root': 1}
        assert result == {"root": 1}
        assert l.warning.called

    def test_multiple_mapping_steps(self):
        config = [
            {"source": ["src_a"], "to": "a.b.c"},
            {"source": ["src_d"], "to": "a.b.d"},
            {"source": ["src_e"], "to": "e"}
        ]
        mapper = EventMapper(config)
        event = {"src_a": 1, "src_d": 2, "src_e": 3}
        assert mapper.process(event) == {
            "a": {"b": {"c": 1, "d": 2}},
            "e": 3
        }

    def test_missing_source_field(self):
        config = [{"source": ["missing"], "to": "present"}]
        mapper = EventMapper(config)
        assert mapper.process({"other": 1}) == {}
