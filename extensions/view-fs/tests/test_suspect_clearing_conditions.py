"""
Test: Suspect Clearing Conditions (Layer 1 — replaces test_c3 integration logic).

Validates 3 scenarios:
  1. Old file (age > threshold) is NOT marked suspect on ingestion
  2. Realtime atomic-write event clears suspect status
  3. Stability timeout (TTL expiry + stable mtime) clears suspect
"""
import pytest
import time
import heapq
from unittest.mock import patch

from fustor_view_fs import FSViewDriver
from fustor_core.event import UpdateEvent, DeleteEvent, MessageSource


@pytest.fixture
def driver():
    """Create a deterministic FSViewDriver with controlled clock."""
    with patch('time.time', return_value=10000.0):
        d = FSViewDriver(id="test_view", view_id="1")
        d.hot_file_threshold = 30.0
        # Feed 5 samples to establish skew = 0 (mtime == fusion_time)
        for _ in range(5):
            d._logical_clock.update(10000.0)
        return d


def _make_update_event(path, mtime, source, is_atomic=True, is_dir=False, index=None):
    payload = {
        'file_path': path,
        'modified_time': mtime,
        'size': 100,
        'is_dir': is_dir,
        'is_atomic_write': is_atomic,
    }
    return UpdateEvent(
        event_schema="fs",
        table="files",
        rows=[payload],
        fields=list(payload.keys()),
        message_source=source,
        index=index or int(mtime * 1000),
    )


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# Scenario 1: Old file → NOT suspect
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

@pytest.mark.asyncio
async def test_old_file_not_suspect(driver):
    """
    An AUDIT-discovered file whose age > hot_file_threshold
    should NOT be marked as suspect.
    
    Watermark ~= 10000. File mtime = 9000. Age = 1000 >> 30.
    """
    path = "/old_file.txt"
    mtime = 9000.0  # age = 10000 - 9000 = 1000 >> 30

    with patch('time.time', return_value=10000.0):
        event = _make_update_event(path, mtime, MessageSource.AUDIT)
        await driver.process_event(event)

    node = driver._get_node(path)
    assert node is not None, "Node should exist"
    assert node.integrity_suspect is False, "Old file should NOT be suspect"
    assert path not in driver._suspect_list, "Old file should NOT be in suspect list"


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# Scenario 2: Realtime atomic write clears suspect
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

@pytest.mark.asyncio
async def test_realtime_atomic_write_clears_suspect(driver):
    """
    A file marked suspect by AUDIT should be cleared when a
    Realtime atomic-write (CLOSE_WRITE/CREATE) event arrives.
    """
    path = "/hot_file.txt"
    mtime = 9999.0  # age = 1 < 30 → suspect

    # Step 1: AUDIT discovers hot file → suspect
    with patch('time.time', return_value=10000.0):
        audit_event = _make_update_event(path, mtime, MessageSource.AUDIT)
        await driver.process_event(audit_event)

    assert driver._get_node(path).integrity_suspect is True
    assert path in driver._suspect_list

    # Step 2: Realtime atomic write → clears suspect
    with patch('time.time', return_value=10001.0):
        rt_event = _make_update_event(
            path, mtime + 1, MessageSource.REALTIME, is_atomic=True
        )
        await driver.process_event(rt_event)

    assert driver._get_node(path).integrity_suspect is False
    assert path not in driver._suspect_list


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# Scenario 3: Stability timeout clears suspect (no sleep!)
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

@pytest.mark.asyncio
async def test_stability_timeout_clears_suspect(driver):
    """
    After TTL expires and mtime is stable, cleanup_expired_suspects
    should clear the suspect flag. NO sleep needed — we directly
    manipulate the monotonic expiry.
    """
    path = "/suspect_file.txt"
    mtime = 9999.0  # age = 1 < 30 → suspect

    # Step 1: AUDIT → suspect
    with patch('time.time', return_value=10000.0):
        event = _make_update_event(path, mtime, MessageSource.AUDIT)
        await driver.process_event(event)

    assert path in driver._suspect_list
    assert driver._get_node(path).integrity_suspect is True

    # Step 2: Fast-forward monotonic clock past expiry
    now_mono = time.monotonic()
    expired_time = now_mono - 10.0
    driver._suspect_list[path] = (expired_time, mtime)  # Force expired
    heapq.heappush(driver._suspect_heap, (expired_time, path))
    driver.state.last_suspect_cleanup_time = 0.0  # Force cleanup to run

    # Step 3: Trigger cleanup — mtime is stable (unchanged)
    driver._cleanup_expired_suspects_unlocked()

    assert path not in driver._suspect_list
    assert driver._get_node(path).integrity_suspect is False


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# Scenario 3b: Active file (mtime changed) gets renewed
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

@pytest.mark.asyncio
async def test_active_file_gets_renewed(driver):
    """
    If mtime changed between suspect entry and TTL expiry,
    the suspect should be renewed, not cleared.
    """
    path = "/active_file.txt"
    mtime = 9999.0

    with patch('time.time', return_value=10000.0):
        event = _make_update_event(path, mtime, MessageSource.AUDIT)
        await driver.process_event(event)

    # Simulate mtime change in the actual node (e.g., realtime partial write)
    node = driver._get_node(path)
    node.modified_time = 10005.0

    # Fast-forward TTL
    now_mono = time.monotonic()
    expired_time = now_mono - 10.0
    driver._suspect_list[path] = (expired_time, mtime)
    heapq.heappush(driver._suspect_heap, (expired_time, path))
    driver.state.last_suspect_cleanup_time = 0.0

    driver._cleanup_expired_suspects_unlocked()

    # Should still be suspect with renewed expiry
    assert path in driver._suspect_list
    assert driver._get_node(path).integrity_suspect is True
    _, new_recorded = driver._suspect_list[path]
    assert new_recorded == 10005.0, "Renewed with latest mtime"
