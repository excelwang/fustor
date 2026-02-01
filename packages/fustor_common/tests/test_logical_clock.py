import time
import unittest
from unittest.mock import patch
import statistics
from fustor_common.logical_clock import LogicalClock

class TestLogicalClockColdStart(unittest.TestCase):
    """
    Unit tests for LogicalClock focusing on the transition from physical cold-start
    to accurate logical watermarking.
    """

    def test_it_initializes_with_physical_time(self):
        """On cold start, Clock.now() should return the current system time."""
        t_system = 1738400000.0
        with patch('time.time', return_value=t_system):
            clock = LogicalClock()
            # Initial watermark should be anchored to system 'now'
            self.assertAlmostEqual(clock.get_watermark(), t_system)

    def test_audit_does_not_pull_clock_backwards(self):
        """Old files found during early audit should not regress the clock from system 'now'."""
        t_system = 2000.0
        with patch('time.time', return_value=t_system):
            clock = LogicalClock()

        # Audit finds a very old file (mtime=1000)
        # Observed mtime (1000) < Current Watermark (2000)
        clock.update(observed_mtime=1000.0, can_sample_skew=False)

        # Clock must stay at 2000
        self.assertEqual(clock.get_watermark(), 2000.0)

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
            clock.update(observed_mtime=nfs_mtime, agent_time=agent_now, session_id="agent-A")
        
        # Logical Watermark should now be 10400 (from mtime)
        self.assertEqual(clock.get_watermark(), 10400.0)

        # Phase 2: Physical Progress without new file activity (e.g., DELETION)
        # Agent advances to 10510 (+10s)
        # Baseline = 10510 - 100 = 10410
        # Logic clock should advance to 10410 even if mtime is None
        clock.update(observed_mtime=None, agent_time=10510.0, session_id="agent-A")
        
        self.assertEqual(clock.get_watermark(), 10410.0)

    def test_multi_agent_skew_isolation(self):
        """Clock should respect different skews for different sessions."""
        t_start = 1000.0
        with patch('time.time', return_value=t_start):
            clock = LogicalClock()

        # Agent A: Skew 100 (Agent is ahead)
        # At AgentTime 2000, NFS Time is 1900
        clock.update(observed_mtime=1900.0, agent_time=2000.0, session_id="A")
        
        # Agent B: Skew -200 (Agent is behind)
        # At AgentTime 1700, NFS Time is 1900
        clock.update(observed_mtime=1900.0, agent_time=1700.0, session_id="B")
        
        # Both see 1900, clock is 1900
        self.assertEqual(clock.get_watermark(), 1900.0)

        # Progress check for A
        # A moves to 2010 -> Baseline 1910
        clock.update(None, agent_time=2010.0, session_id="A")
        self.assertEqual(clock.get_watermark(), 1910.0)

        # Progress check for B
        # B moves to 1720 -> Baseline 1920
        clock.update(None, agent_time=1720.0, session_id="B")
        self.assertEqual(clock.get_watermark(), 1920.0)

if __name__ == '__main__':
    unittest.main()
