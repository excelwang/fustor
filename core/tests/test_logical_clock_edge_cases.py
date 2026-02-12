"""
Tests for LogicalClock — skew calculation, monotonicity, and reset.

LogicalClock.watermark = Fusion_Physical_Time - Mode_Skew(observed mtimes).
The skew is the mode of (time.time() - observed_mtime) samples.
"""
import pytest
import time
from fustor_core.clock import LogicalClock


def test_cold_start_returns_physical_time():
    """Before any samples, watermark equals physical time."""
    clock = LogicalClock()
    before = time.time()
    wm = clock.get_watermark()
    after = time.time()
    # Should be close to physical time (no skew calibrated)
    assert before <= wm <= after + 0.01


def test_update_calibrates_skew():
    """After providing mtime samples, skew adjusts watermark."""
    clock = LogicalClock()
    now = time.time()
    # Feed mtimes that are 10 seconds in the past
    for _ in range(3):
        clock.update(now - 10.0)
    
    skew = clock.get_skew()
    assert abs(skew - 10) <= 1  # Mode should be ~10 (int rounding)


def test_watermark_uses_skew():
    """Watermark = time.time() - mode_skew."""
    clock = LogicalClock()
    now = time.time()
    # Feed multiple mtimes with consistent skew (e.g., distant past)
    for _ in range(5):
        clock.update(now - 100.0)
    
    wm = clock.get_watermark()
    # wm ≈ time.time() - 100, so wm ≈ now - 100
    assert abs(wm - (now - 100.0)) < 2.0


def test_skew_mode_wins_over_outliers():
    """Mode-based skew ignores outliers."""
    clock = LogicalClock()
    now = time.time()
    
    # 5 samples with skew ~10
    for _ in range(5):
        clock.update(now - 10.0)
    
    # 1 outlier with skew ~1000
    clock.update(now - 1000.0)
    
    # Mode should still be ~10, not 1000
    skew = clock.get_skew()
    assert abs(skew - 10) <= 1


def test_reset_clears_calibration():
    """Reset clears all skew samples, returning to physical time."""
    clock = LogicalClock()
    now = time.time()
    for _ in range(5):
        clock.update(now - 100.0)
    
    clock.reset()
    
    # After reset, watermark should be near physical time again
    wm = clock.get_watermark()
    assert abs(wm - time.time()) < 1.0
    assert clock.get_skew() == 0.0


def test_update_none_mtime():
    """update(None) returns watermark without crashing (deletion events)."""
    clock = LogicalClock()
    now = time.time()
    clock.update(now - 5.0)
    wm = clock.update(None)
    assert isinstance(wm, float)


def test_repr():
    """__repr__ includes watermark and skew."""
    clock = LogicalClock()
    r = repr(clock)
    assert "LogicalClock" in r
    assert "watermark=" in r
