from datetime import datetime
from fustor_core.models.log import LogEntry

def test_log_entry_creation():
    """测试 LogEntry 模型的创建和字段别名"""
    now = datetime.now()
    log = LogEntry(
        ts=now,
        level="INFO",
        source="test_component",
        msg="test message",
        line_number=10
    )
    
    assert log.timestamp == now
    assert log.level == "INFO"
    assert log.component == "test_component"
    assert log.message == "test message"
    assert log.line_number == 10

def test_log_entry_serialization():
    """测试 LogEntry 模型的序列化和反序列化"""
    data = {
        "ts": "2023-10-27T10:00:00",
        "level": "ERROR",
        "source": "api_service",
        "msg": "database connection failed",
        "line_number": 42
    }
    
    log = LogEntry(**data)
    assert log.level == "ERROR"
    assert log.component == "api_service"
    assert log.message == "database connection failed"
    
    # 验证导出时是否可以使用别名
    exported = log.model_dump(by_alias=True)
    assert exported["ts"] == log.timestamp
    assert exported["source"] == "api_service"
    assert exported["msg"] == "database connection failed"
    assert exported["line_number"] == 42
