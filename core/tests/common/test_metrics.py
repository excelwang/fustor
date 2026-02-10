
import logging
import pytest
from unittest.mock import MagicMock
from fustor_core.common.metrics import Metrics, NoOpMetrics, LoggingMetrics, get_metrics, set_global_metrics

def test_noop_metrics_does_not_crash():
    m = NoOpMetrics()
    m.counter("test.counter", 1)
    m.gauge("test.gauge", 42)
    m.histogram("test.hist", 100)
    # No assertion needed, just checking it doesn't raise exception

def test_logging_metrics(caplog):
    caplog.set_level(logging.DEBUG)
    m = LoggingMetrics("test_logger")
    
    m.counter("test.counter", 1, tags={"env": "prod"})
    m.gauge("test.gauge", 42)
    
    # Check logs
    assert "METRIC counter test.counter +1 env=prod" in caplog.text
    assert "METRIC gauge test.gauge =42" in caplog.text

def test_global_metrics_singleton():
    original = get_metrics()
    try:
        mock_metrics = MagicMock(spec=Metrics)
        set_global_metrics(mock_metrics)
        
        m = get_metrics()
        assert m is mock_metrics
        m.counter("foo")
        mock_metrics.counter.assert_called_with("foo")
        
    finally:
        set_global_metrics(original)
