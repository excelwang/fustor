"""
Test: Clock Skew Voting (Layer 1 — replaces test_h integration logic).

Validates LogicalClock's Mode-based skew election:
  1. Majority skew wins
  2. Outlier skew is rejected
  3. Audit events don't pollute skew samples
  4. Clock survives mixed-skew agents
"""
import pytest
from unittest.mock import patch

from fustor_core.clock import LogicalClock


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# Scenario 1: Majority skew wins
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━

def test_majority_skew_wins():
    """
    When multiple agents contribute different skews,
    Mode selects the skew with the most samples.
    
    Agent B (5 samples, skew=100):  Fusion=2000, mtime=1900
    Agent A (2 samples, skew=-500): Fusion=2000, mtime=2500
    → Mode = 100 (Agent B wins)
    """
    with patch('time.time', return_value=2000.0):
        clock = LogicalClock(initial_time=0.001)
        
        # Agent B: 5 realtime events (skew = 2000 - 1900 = 100)
        for _ in range(5):
            clock.update(1900.0, can_sample_skew=True)
        
        assert clock.get_skew() == 100.0
        assert clock.get_watermark() == 1900.0
        
        # Agent A: 2 outlier events (skew = 2000 - 2500 = -500)
        for _ in range(2):
            clock.update(2500.0, can_sample_skew=True)
        
        # Mode still = 100 (5 > 2)
        assert clock.get_skew() == 100.0
        assert clock.get_watermark() == 1900.0


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# Scenario 2: Single outlier is rejected
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

def test_single_rogue_mtime_rejected():
    """
    A single future-timestamp file (touch -d 2050)
    should not pull the clock forward.
    """
    with patch('time.time', return_value=5000.0):
        clock = LogicalClock(initial_time=0.001)
        
        # 10 normal events (skew = 5000 - 4990 = 10)
        for _ in range(10):
            clock.update(4990.0)
        
        assert clock.get_skew() == 10.0
        
        # 1 rogue event (skew = 5000 - 99999 = -94999)
        clock.update(99999.0)
        
        # Mode unchanged: 10 samples at skew=10 vs 1 at skew=-94999
        assert clock.get_skew() == 10.0
        assert clock.get_watermark() == 4990.0


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# Scenario 3: Audit events don't pollute skew
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

def test_audit_does_not_sample_skew():
    """
    Events with can_sample_skew=False (Audit/Snapshot) should
    not contribute to the skew histogram.
    """
    with patch('time.time', return_value=3000.0):
        clock = LogicalClock(initial_time=0.001)
        
        # 3 realtime events (skew = 3000 - 2900 = 100)
        for _ in range(3):
            clock.update(2900.0, can_sample_skew=True)
        
        assert clock.get_skew() == 100.0
        
        # 10 audit events with very old files (skew = 3000 - 100 = 2900)
        for _ in range(10):
            clock.update(100.0, can_sample_skew=False)
        
        # Mode should STILL be 100 (audit didn't contribute)
        assert clock.get_skew() == 100.0


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# Scenario 4: Tie-breaking selects smaller skew (lower lag)
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

def test_tie_breaking_prefers_smaller_skew():
    """
    When two skew values have equal frequency,
    the clock should pick the smaller one (lower latency).
    """
    with patch('time.time', return_value=1000.0):
        clock = LogicalClock(initial_time=0.001)
        
        # 3 samples with skew=50
        for _ in range(3):
            clock.update(950.0)
        
        # 3 samples with skew=200
        for _ in range(3):
            clock.update(800.0)
        
        # Tie: 3 vs 3 → pick smaller skew = 50
        assert clock.get_skew() == 50.0


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# Scenario 5: Clock advances with physical time
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

def test_watermark_tracks_physical_time():
    """
    Once skew is calibrated, watermark should advance
    as physical time (time.time()) advances.
    """
    # t=1000: calibrate
    with patch('time.time', return_value=1000.0):
        clock = LogicalClock(initial_time=0.001)
        for _ in range(5):
            clock.update(990.0)  # skew = 10
    
    assert clock.get_skew() == 10.0
    
    # t=2000: watermark should be 2000 - 10 = 1990
    with patch('time.time', return_value=2000.0):
        assert clock.get_watermark() == 1990.0
    
    # t=5000: watermark should be 5000 - 10 = 4990
    with patch('time.time', return_value=5000.0):
        assert clock.get_watermark() == 4990.0
