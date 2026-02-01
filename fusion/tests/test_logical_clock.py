"""
Unit tests for LogicalClock class.
"""
import time
import threading
import pytest

from unittest.mock import patch
from fustor_common.logical_clock import LogicalClock


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
        """Reset should set clock to 0 by default (triggering fallback)."""
        clock = LogicalClock(initial_time=500.0)
        clock.reset()
        # Reset to 0.0 -> Fallback to time.time()
        now = time.time()
        assert abs(clock.now() - now) < 1.0
    
    def test_reset_to_value(self):
        """Reset should set clock to specified value."""
        clock = LogicalClock(initial_time=500.0)
        clock.reset(100.0)
        assert clock.now() == 100.0


class TestLogicalClockThreadSafety:
    """Thread safety tests for LogicalClock."""
    
    def test_concurrent_updates(self):
        """Multiple threads updating should be safe."""
        # Initialize with a fixed small value to ensure updates (0 to 9099) advance it
        clock = LogicalClock(initial_time=0.001)
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

        # Clock must stay at 2000
        assert clock.get_watermark() == 2000.0

    def test_realtime_events_establish_skew_and_take_control(self):
        """Establishing a skew mode allows the clock to move based on physical progress."""
        t_start = 10000.0
        with patch('time.time', return_value=t_start):
            clock = LogicalClock()

        # Phase 1: Establish Skew
        # Agent physical clock: 10500
        # NFS mtime being written: 10400
        # Skew = 100s (Agent - NFS)
        agent_now = 10500.0
        nfs_mtime = 10400.0
        
        # Inject some samples to stabilize Mode
        for _ in range(5):
            clock.update(observed_mtime=nfs_mtime, agent_time=agent_now) # Removed session_id
        
        # Logic clock should advance to 10410 even if mtime is None
        clock.update(observed_mtime=None, agent_time=10510.0) # Removed session_id
        
        assert clock.get_watermark() == 10410.0

    def test_global_consensus_isolates_rogue_agent(self):
        """Clock should follow the majority skew and ignore a rogue agent's unstable samples."""
        t_start = 1000.0
        with patch('time.time', return_value=t_start):
            clock = LogicalClock()

        # 1. Majority of samples (3 agents) establish a Skew of 100
        # Formula: Diff = AgentTime - mtime = 2000 - 1900 = 100
        for i in range(3):
            clock.update(observed_mtime=1900.0, agent_time=2000.0)
        
        assert clock.get_watermark() == 1900.0
        assert clock._cached_global_skew == 100

        # 2. A rogue agent appears with a wildly different skew of 5000 (Agent is far in future)
        # Diff = 6900 - 1900 = 5000
        clock.update(observed_mtime=1900.0, agent_time=6900.0)

        # 3. VERIFY: The global skew MUST still be 100 (because it's the Mode)
        assert clock._cached_global_skew == 100
        
        # 4. Progress check: Even if rogue agent sends a deletion at its 'physical' 7000,
        # it should be translated using the global skew (100), not its own (5000).
        # Baseline = 7000 - 100 = 6900
        clock.update(None, agent_time=7000.0)
        
        # The clock will advance to 6900 because we trust the ROGUE agent's physical flow
        # but translated via the STABLE global skew.
        # Note: In a real rogue scenario, if its physical time is wrong, we still 
        # follow its 'tick', but the translation is now consistent with the cluster.
        assert clock.get_watermark() == 6900.0
