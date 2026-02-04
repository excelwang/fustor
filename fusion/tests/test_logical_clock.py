"""
Unit tests for LogicalClock class.
"""
import time
import threading
import pytest

from unittest.mock import patch
from fustor_core.clock import LogicalClock


class TestLogicalClockBasic:
    """Basic functionality tests for LogicalClock."""
    
    def test_initial_value_default(self):
        """Clock should start at 0.0 internal value but return time.time() as fallback."""
        clock = LogicalClock()
        # Internal value is 0.0, but public API returns safe fallback
        now = time.time()
        assert abs(clock.now() - now) < 1.0 # Within 1 second of current time
    
    def test_initial_value_custom(self):
        """Clock should accept custom initial value."""
        with patch('time.time', return_value=1000.0):
            clock = LogicalClock(initial_time=1000.0)
            assert clock.now() == 1000.0
    
    def test_update_advances_clock(self):
        """Update should advance clock when mtime is newer."""
        with patch('time.time', return_value=200.0):
            clock = LogicalClock(initial_time=100.0)
            result = clock.update(200.0)
            assert result == 200.0
            assert clock.now() == 200.0
    
    def test_update_ignores_older_time(self):
        """Update should ignore mtime older than current value."""
        with patch('time.time', return_value=200.0):
            clock = LogicalClock(initial_time=200.0)
            result = clock.update(100.0)
            assert result == 200.0
            assert clock.now() == 200.0
    
    def test_update_ignores_equal_time(self):
        """Update with equal time should not change clock."""
        with patch('time.time', return_value=150.0):
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
        """Reset should set clock to 0 by default (triggering fallback)."""
        clock = LogicalClock(initial_time=500.0)
        clock.reset()
        # Reset to 0.0 -> Fallback to time.time()
        now = time.time()
        assert abs(clock.now() - now) < 1.0
    
    def test_reset_to_value(self):
        """Reset should set clock to specified value."""
        with patch('time.time', return_value=100.0):
            clock = LogicalClock(initial_time=500.0)
            clock.reset(100.0)
            assert clock.now() == 100.0


class TestLogicalClockThreadSafety:
    """Thread safety tests for LogicalClock."""
    
    def test_concurrent_updates(self):
        """Multiple threads updating should be safe."""
        # Use patched time to control BaseLine
        with patch('time.time', return_value=9100.0):
            # Initialize with a fixed small value to ensure updates (0 to 9099) advance it
            clock = LogicalClock(initial_time=0.001)
            errors = []
            
            def worker(start_value: int, count: int):
                try:
                    for i in range(count):
                        val = start_value + i
                        clock.update(val)
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
            # Final value should be the max of all updates (9099) or BaseLine
            assert clock.now() >= 9099  # At least the max update value
    
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
        

    def test_it_initializes_with_physical_time(self):
        """On cold start, Clock.now() should return the current system time."""
        t_system = 1738400000.0
        with patch('time.time', return_value=t_system):
            clock = LogicalClock()
            # Initial watermark should be anchored to system 'now'
            assert abs(clock.get_watermark() - t_system) < 0.001

    def test_audit_does_not_pull_clock_backwards(self):
        """Old files found during early audit should not regress the clock from system 'now'."""
        t_system = 2000.0
        with patch('time.time', return_value=t_system):
            clock = LogicalClock()

            # Audit finds a very old file (mtime=1000)
            # Observed mtime (1000) < Current Watermark (2000)
            clock.update(observed_mtime=1000.0, can_sample_skew=False)

            # Clock must stay at 2000 (BaseLine = time.time() when no skew established)
            assert clock.get_watermark() == 2000.0

    def test_realtime_events_establish_skew_and_take_control(self):
        """Establishing a skew mode allows the clock to move based on physical progress."""
        # Use Fusion Local Time = 10500.0
        t_fusion = 10500.0
        nfs_mtime = 10400.0  # NFS mtime being written
        # Expected Skew = 10500 - 10400 = 100s
        
        with patch('time.time', return_value=t_fusion):
            # Use small initial_time so mtime can advance the clock
            clock = LogicalClock(initial_time=0.001)
            
            # Inject some samples to stabilize Mode
            for _ in range(5):
                clock.update(observed_mtime=nfs_mtime)
            
            # After updates: skew=100, baseline=10400, _value advanced to 10400
            assert clock._value == nfs_mtime
            assert clock.get_watermark() == nfs_mtime
        
        # Time progresses to 10510
        with patch('time.time', return_value=10510.0):
            # Logic clock should advance based on BaseLine = 10510 - 100 = 10410
            clock.update(observed_mtime=None)
            assert clock.get_watermark() == 10410.0

    def test_global_consensus_isolates_rogue_mtime(self):
        """Clock should follow the majority skew and not be affected by anomalous mtime."""
        t_fusion = 2000.0
        
        with patch('time.time', return_value=t_fusion):
            # Use small initial_time so mtime can advance the clock
            clock = LogicalClock(initial_time=0.001)

            # 1. Majority of samples establish a Skew of 100
            # Formula: Diff = FusionTime - mtime = 2000 - 1900 = 100
            for i in range(3):
                clock.update(observed_mtime=1900.0)
            
            # After updates: baseline=1900, _value=1900
            assert clock._value == 1900.0
            assert clock._cached_global_skew == 100

            # 2. A file with future mtime (outside trust window) should not pull clock forward  
            clock.update(observed_mtime=3500.0)  # Way beyond trust window (upper_bound=1901)
            
            # 3. VERIFY: The global skew is affected but Mode picks the majority
            assert clock._cached_global_skew == 100
            
            # 4. _value should still be 1900 (baseline enforcement = max(1900, 1900))
            assert clock._value == 1900.0
            assert clock.get_watermark() == 1900.0
        
        # 5. Time progresses to 7000
        with patch('time.time', return_value=7000.0):
            clock.update(None)  # Deletion event
            # BaseLine = 7000 - 100 = 6900
            assert clock.get_watermark() == 6900.0


