import pytest
import logging
from fustor_core.pipe.mapper import EventMapper

def test_event_mapper_init_variations():
    """测试 Mapper 初始化时处理不同类型的配置对象"""
    class ConfigObj:
        def __init__(self, to, source):
            self.to = to
            self.source = source
            
    # 测试 vars(item) 路径
    obj = ConfigObj(to="a", source=["b"])
    mapper = EventMapper([obj])
    assert mapper.config[0]["to"] == "a"
    
    # 测试无效项（既没有 __dict__ 也没有 vars）
    mapper2 = EventMapper([123])
    assert len(mapper2.config) == 0

def test_event_mapper_map_batch_with_raw_objects():
    """测试 map_batch 处理非 dict/pydantic 对象"""
    class RowObj:
        def __init__(self, f1):
            self.f1 = f1
            
    class EventObj:
        def __init__(self, rows):
            self.rows = rows
            self.fields = []
            
    mapper = EventMapper([{"to": "new_f", "source": ["f1"]}])
    
    row = RowObj(f1="val")
    event = EventObj(rows=[row])
    
    res = mapper.map_batch([event])
    assert res[0].rows[0] == {"new_f": "val"}
    assert res[0].fields == ["new_f"]

def test_event_mapper_conversion_edge_cases():
    """测试类型转换的边缘情况"""
    # 无效类型名：回退到 None converter
    mapper = EventMapper([{"to": "a", "source": ["f1:unknown"]}])
    assert mapper.process({"f1": "123"}) == {"a": "123"}
    
    # 转换失败：保留原值 (try-except block)
    mapper_int = EventMapper([{"to": "i", "source": ["f1:int"]}])
    assert mapper_int.process({"f1": "not_an_int"}) == {"i": "not_an_int"}
    
    # 字段不存在
    assert mapper_int.process({"f2": 1}) == {}

def test_event_mapper_path_conflict(caplog):
    """测试路径冲突警告"""
    config = [
        {"to": "a", "hardcoded_value": 1},
        {"to": "a.b", "hardcoded_value": 2} # a 已经是 scalar，不能再往下走
    ]
    mapper = EventMapper(config)
    with caplog.at_level(logging.WARNING):
        res = mapper.process({})
        assert res == {"a": 1}
        assert "Mapping conflict" in caplog.text

def test_event_mapper_boolean_conversion():
    """测试布尔转换逻辑"""
    mapper = EventMapper([{"to": "b", "source": ["f1:bool"]}])
    assert mapper.process({"f1": "true"}) == {"b": True}
    assert mapper.process({"f1": "YES"}) == {"b": True}
    assert mapper.process({"f1": "1"}) == {"b": True}
    assert mapper.process({"f1": "0"}) == {"b": False}
    assert mapper.process({"f1": 1}) == {"b": True}
    assert mapper.process({"f1": None}) == {}
