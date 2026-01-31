import unittest
import time
from unittest.mock import MagicMock
from fustor_common.logical_clock import LogicalClock

class TestRobustLogicalClock(unittest.TestCase):
    
    def setUp(self):
        self.clock = LogicalClock()
        self.session_a = "session_A"
        self.session_b = "session_B"

    def test_legacy_mode(self):
        """Verify backward compatibility for update(mtime) without context."""
        # Initial update
        self.clock.update(100.0)
        self.assertEqual(self.clock.now(), 100.0)
        
        # Advance
        self.clock.update(101.0)
        self.assertEqual(self.clock.now(), 101.0)
        
        # Ignored (old)
        self.clock.update(99.0)
        self.assertEqual(self.clock.now(), 101.0)

    def test_skew_convergence(self):
        """Verify the clock learns skew from samples."""
        # Simulated scenario: Agent is 10s AHEAD of NFS mtime.
        # AgentTime = 1000, mtime = 990 -> Diff = 10
        
        agent_t = 1000.0
        mtime = 990.0
        
        # Update 1: One sample is enough to start calculation
        self.clock.update(mtime, agent_time=agent_t, session_id=self.session_a)
        
        # Expect: Skew = 10. BaseLine = AgentTime(1000) - Skew(10) = 990.
        # Watermark should be 990.
        self.assertAlmostEqual(self.clock.now(), 990.0)
        
        # Update 2: Consistent skew
        self.clock.update(mtime + 1, agent_time=agent_t + 1, session_id=self.session_a)
        self.assertAlmostEqual(self.clock.now(), 991.0)

    def test_trust_window_fast_path(self):
        """Verify mtime is trusted if slightly ahead of BaseLine (Fast Path)."""
        # Establish Skew = 0
        self.clock.update(100.0, agent_time=100.0, session_id=self.session_a)
        self.assertEqual(self.clock.now(), 100.0)
        
        # Next event: mtime is 0.5s ahead of AgentTime (due to jitter/granularity)
        # BaseLine = 101 - 0 = 101. mtime = 101.5.
        # 101.5 <= 101 + 1.0 (Trust Window). Should accept 101.5.
        self.clock.update(101.5, agent_time=101.0, session_id=self.session_a)
        
        self.assertEqual(self.clock.now(), 101.5) # Fast forwarded!

    def test_future_timestamp_rejection(self):
        """Verify future timestamps (jump attacks) are rejected."""
        # Establish Skew = 0 with multiple samples to stabilize Mode
        # If we only have 1 sample, the outlier might become the mode (or tie-break winner)
        for i in range(5):
             self.clock.update(100.0 + i, agent_time=100.0 + i, session_id=self.session_a)
        
        # Current Clock ~ 104.0. Skew = 0.
        
        # Attack: User touches file to Year 2050 (Timestamp 2524608000)
        future_mtime = 2524608000.0
        current_agent_time = 105.0 # next second
        
        # BaseLine = 105.0 - 0 = 105.0. 
        # TrustWindow = 106.0.
        # FutureMtime (2050) > 106.0.
        # Should REJECT future_mtime.
        # Should ADVANCE to BaseLine (105.0).
        self.clock.update(future_mtime, agent_time=current_agent_time, session_id=self.session_a)
        
        self.assertEqual(self.clock.now(), 105.0) # Clock protected!
        self.assertNotEqual(self.clock.now(), future_mtime)

    def test_multi_session_mode_election(self):
        """Verify Global Skew is the Mode of all session samples."""
        # Session A: Skew = 10 (Reliable) - 3 samples
        for i in range(3):
            self.clock.update(100+i, agent_time=110+i, session_id=self.session_a)
            
        # Session B: Skew = 50 (Glitchy/Wrong) - 1 sample
        self.clock.update(100, agent_time=150, session_id=self.session_b)
        
        # Histogram: {10: 3, 50: 1}. Mode is 10.
        # Global Skew should be 10.
        
        # Verification update from Session A
        # Agent=120. BaseLine = 120 - 10 = 110.
        self.clock.update(110, agent_time=120, session_id=self.session_a)
        self.assertEqual(self.clock.now(), 110.0)

    def test_safeguard(self):
        """Verify 0.0 uninitialized state returns time.time(), but initialized does not forcefully advance."""
        
        # Uninitialized
        now_sys = time.time()
        c_val = self.clock.now()
        # Should be close to system time
        self.assertTrue(abs(c_val - now_sys) < 1.0)
        
        # Initialize with OLD time
        self.clock.update(100.0) # Set to 100
        
        # Should NOT return max(100, system_time). Should return 100.
        # This proves safeguard is removed for initialized clock.
        self.assertEqual(self.clock.now(), 100.0)


    def test_remove_session_cleanup(self):
        """Verify session cleanup removes samples from Global Histogram."""
        # Session A: Skew=10. 3 Samples.
        for i in range(3):
            self.clock.update(100+i, agent_time=110+i, session_id=self.session_a)
        
        # Session B: Skew=50. 5 Samples. (Mode is 50)
        for i in range(5):
             self.clock.update(100+i, agent_time=150+i, session_id=self.session_b)
             
        # Histogram: {10: 3, 50: 5}. Mode is 50.
        # Check current effect (approx, needs one update to apply cached value if any)
        # But internals can be checked directly if we want to trust white-box testing.
        # Or trigger an update from Session A -> BaseLine = Agent(120) - 50 = 70.
        
        self.clock.update(110, agent_time=120, session_id=self.session_a)
        # 120 - 50 = 70. Mtime=110. 110 > 70+1. Result: REJECT (Future).
        # Clock stays at last valid value (from Session B's updates ~100-104)
        # Note: B updated 100/150..104/154. Max mtime was 104.
        self.assertEqual(self.clock.now(), 104.0)
        
        # Remove Session B (the dominant one)
        self.clock.remove_session(self.session_b)
        
        # Expect Histogram: {10: 3+1}. Mode is 10.
        # Next update from A -> BaseLine = Agent(121) - 10 = 111.
        self.clock.update(111, agent_time=121, session_id=self.session_a)
        
        self.assertEqual(self.clock.now(), 111.0)


if __name__ == '__main__':
    unittest.main()
