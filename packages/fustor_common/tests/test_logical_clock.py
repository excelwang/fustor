"""
Unit tests for LogicalClock class.
"""
import time
import threading
import pytest

from fustor_common.logical_clock import LogicalClock


class TestLogicalClockBasic:
    """Basic functionality tests for LogicalClock."""
    
    def test_initial_value_default(self):
        """Clock should start at 0.0 by default."""
        clock = LogicalClock()
        assert clock.now() == 0.0
    
    def test_initial_value_custom(self):
        """Clock should accept custom initial value."""
        clock = LogicalClock(initial_time=1000.0)
        assert clock.now() == 1000.0
    
    def test_update_advances_clock(self):
        """Update should advance clock when mtime is newer."""
        clock = LogicalClock(initial_time=100.0)
        result = clock.update(200.0)
        assert result == 200.0
        assert clock.now() == 200.0
    
    def test_update_ignores_older_time(self):
        """Update should ignore mtime older than current value."""
        clock = LogicalClock(initial_time=200.0)
        result = clock.update(100.0)
        assert result == 200.0
        assert clock.now() == 200.0
    
    def test_update_ignores_equal_time(self):
        """Update with equal time should not change clock."""
        clock = LogicalClock(initial_time=150.0)
        result = clock.update(150.0)
        assert result == 150.0
        assert clock.now() == 150.0
    
    def test_update_handles_none(self):
        """Update should handle None gracefully."""
        clock = LogicalClock(initial_time=100.0)
        result = clock.update(None)
        assert result == 100.0

class TestLogicalClockReset:
    """Tests for reset functionality."""
    
    def test_reset_to_zero(self):
        """Reset should set clock to 0 by default."""
        clock = LogicalClock(initial_time=500.0)
        clock.reset()
        assert clock.now() == 0.0
    
    def test_reset_to_value(self):
        """Reset should set clock to specified value."""
        clock = LogicalClock(initial_time=500.0)
        clock.reset(100.0)
        assert clock.now() == 100.0


class TestLogicalClockThreadSafety:
    """Thread safety tests for LogicalClock."""
    
    def test_concurrent_updates(self):
        """Multiple threads updating should be safe."""
        clock = LogicalClock()
        errors = []
        
        def worker(start_value: int, count: int):
            try:
                for i in range(count):
                    clock.update(start_value + i)
            except Exception as e:
                errors.append(e)
        
        threads = [
            threading.Thread(target=worker, args=(i * 1000, 100))
            for i in range(10)
        ]
        
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        
        assert len(errors) == 0
        # Final value should be the max of all updates
        assert clock.now() == 9099  # 9 * 1000 + 99
    
    def test_concurrent_read_write(self):
        """Concurrent reads and writes should be safe."""
        clock = LogicalClock(initial_time=100.0)
        errors = []
        reads = []
        
        def writer():
            try:
                for i in range(100):
                    clock.update(200.0 + i)
            except Exception as e:
                errors.append(e)
        
        def reader():
            try:
                for _ in range(100):
                    val = clock.now()
                    reads.append(val)
            except Exception as e:
                errors.append(e)
        
        threads = [
            threading.Thread(target=writer),
            threading.Thread(target=reader),
            threading.Thread(target=reader),
        ]
        
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        
        assert len(errors) == 0
        # All reads should be >= initial value
        assert all(r >= 100.0 for r in reads)
