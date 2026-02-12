"""
Test: Future Mtime Suspect Lifecycle (Layer 1 — replaces test_i integration logic).

Validates the full lifecycle of a file with mtime ahead of the watermark:
  1. mtime > watermark → marked suspect
  2. After clock catches up and sentinel verifies → suspect cleared
  3. Edge case: mtime == watermark (boundary)
"""
import pytest
import time
from unittest.mock import patch

from fustor_view_fs import FSViewDriver
from fustor_core.event import UpdateEvent, MessageSource


@pytest.fixture
def driver():
    """FSViewDriver with clock calibrated at t=10000, skew=0."""
    with patch('time.time', return_value=10000.0):
        d = FSViewDriver(id="test_view", view_id="1")
        d.hot_file_threshold = 30.0
        for _ in range(5):
            d._logical_clock.update(10000.0)  # skew = 0
        return d


def _make_audit_event(path, mtime):
    payload = {
        'file_path': path,
        'modified_time': mtime,
        'size': 100,
        'is_dir': False,
        'is_atomic_write': True,
    }
    return UpdateEvent(
        event_schema="fs",
        table="files",
        rows=[payload],
        fields=list(payload.keys()),
        message_source=MessageSource.AUDIT,
        index=int(mtime * 1000),
    )


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# Scenario 1: Future mtime → suspect
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

@pytest.mark.asyncio
async def test_future_mtime_marked_suspect(driver):
    """
    File with mtime 60s ahead of watermark should be suspect.
    
    Watermark = 10000 (skew=0, time.time()=10000)
    File mtime = 10060
    Age = 10000 - 10060 = -60 < 30 → SUSPECT
    """
    path = "/future_file.txt"
    mtime = 10060.0  # 60s in the future

    with patch('time.time', return_value=10000.0):
        event = _make_audit_event(path, mtime)
        await driver.process_event(event)

    node = driver._get_node(path)
    assert node is not None
    assert node.integrity_suspect is True, "Future mtime should be suspect"
    assert path in driver._suspect_list


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# Scenario 2: Clock catches up → sentinel clears suspect
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

@pytest.mark.asyncio
async def test_clock_catchup_clears_suspect(driver):
    """
    After physical time advances past future_mtime + threshold,
    update_suspect() should clear the suspect flag.
    
    NO sleep needed — we just patch time.time to a later value.
    """
    path = "/future_file.txt"
    mtime = 10060.0

    # Step 1: Ingest → suspect
    with patch('time.time', return_value=10000.0):
        event = _make_audit_event(path, mtime)
        await driver.process_event(event)

    assert driver._get_node(path).integrity_suspect is True

    # Step 2: Fast-forward time to 10100 (past mtime + threshold)
    # Watermark = 10100 - 0 (skew) = 10100
    # Age = 10100 - 10060 = 40 > 30 (threshold) → cold
    # Sentinel reports stable mtime → CLEAR
    with patch('time.time', return_value=10100.0):
        await driver.update_suspect(path, mtime=mtime, size=100)

    node = driver._get_node(path)
    assert node.integrity_suspect is False, "Suspect should be cleared after clock catch-up"
    assert path not in driver._suspect_list


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# Scenario 3: Clock NOT yet caught up → stays suspect
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

@pytest.mark.asyncio
async def test_clock_not_caught_up_stays_suspect(driver):
    """
    If physical time advances but watermark is still < mtime + threshold,
    the file should remain suspect (stable but still hot).
    """
    path = "/future_file.txt"
    mtime = 10060.0

    with patch('time.time', return_value=10000.0):
        event = _make_audit_event(path, mtime)
        await driver.process_event(event)

    # Step 2: Advance to 10070 → watermark = 10070
    # Age = 10070 - 10060 = 10 < 30 → still hot
    with patch('time.time', return_value=10070.0):
        await driver.update_suspect(path, mtime=mtime, size=100)

    node = driver._get_node(path)
    # Stable + Hot → keep suspect until TTL
    assert path in driver._suspect_list, "File still hot, should stay in suspect list"


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# Scenario 4: Boundary — age exactly at threshold
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

@pytest.mark.asyncio
async def test_boundary_age_at_threshold(driver):
    """
    File with age exactly at threshold boundary.
    age = watermark - mtime = threshold → NOT hot (>= threshold → cold).
    """
    path = "/boundary_file.txt"
    threshold = driver.hot_file_threshold  # 30.0
    # watermark = 10000 (time.time=10000, skew=0)
    mtime = 10000.0 - threshold  # 9970.0, age = 30.0

    with patch('time.time', return_value=10000.0):
        event = _make_audit_event(path, mtime)
        await driver.process_event(event)

    node = driver._get_node(path)
    # age = 30.0, threshold = 30.0 → age < threshold is False → NOT suspect
    assert node.integrity_suspect is False
    assert path not in driver._suspect_list
