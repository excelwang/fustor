"""
Test: API Blind-spot Visibility (Recovers deleted test_g).

Validates that:
  1. Audit-discovered files appear in /blind-spots additions
  2. Audit-End missing files appear in /blind-spots deletions
  3. After session reset, blind-spot lists are cleared
"""
import pytest
import time
from unittest.mock import patch

from fastapi import FastAPI
from fastapi.testclient import TestClient

from fustor_view_fs.api import create_fs_router
from fustor_view_fs.driver import FSViewDriver
from fustor_core.event import UpdateEvent, DeleteEvent, MessageSource


# --- Fixtures ---

async def _noop_check(view_id: str):
    pass

async def _get_view_id():
    return "1"


@pytest.fixture
def setup():
    """Create a real FSViewDriver and wire it into a TestClient."""
    with patch('time.time', return_value=10000.0):
        driver = FSViewDriver(id="test_view", view_id="1")
        driver.hot_file_threshold = 30.0
        for _ in range(5):
            driver._logical_clock.update(10000.0)

    async def get_driver(view_id: str):
        return driver

    router = create_fs_router(
        get_driver_func=get_driver,
        check_snapshot_func=_noop_check,
        get_view_id_dep=_get_view_id,
    )
    app = FastAPI()
    app.include_router(router)
    client = TestClient(app)
    return client, driver


# --- Tests ---

@pytest.mark.asyncio
async def test_audit_addition_visible_in_blind_spots(setup):
    """
    A file discovered by Audit (not previously known via Realtime)
    should appear in /blind-spots additions.
    """
    client, driver = setup

    # Audit discovers a new file
    with patch('time.time', return_value=10000.0):
        event = UpdateEvent(
            event_schema="fs",
            table="files",
            rows=[{
                "file_path": "/audit_found.txt",
                "modified_time": 9000.0,
                "size": 100,
                "is_dir": False,
                "is_atomic_write": True,
            }],
            fields=["file_path", "modified_time", "size", "is_dir", "is_atomic_write"],
            message_source=MessageSource.AUDIT,
            index=9000000,
        )
        await driver.process_event(event)

    response = client.get("/blind-spots", params={"view_id": "1"})
    assert response.status_code == 200
    data = response.json()

    assert data["additions_count"] >= 1
    addition_paths = [a["path"] for a in data["additions"]]
    assert "/audit_found.txt" in addition_paths, \
        "Audit-discovered file should be in blind-spot additions"


@pytest.mark.asyncio
async def test_audit_deletion_visible_in_blind_spots(setup):
    """
    A file that exists in memory but is missing from Audit scan
    should appear in /blind-spots deletions after audit-end.
    """
    client, driver = setup

    # Step 1: Create file via REALTIME
    with patch('time.time', return_value=10000.0):
        rt_event = UpdateEvent(
            event_schema="fs",
            table="files",
            rows=[{
                "file_path": "/dir/missing.txt",
                "modified_time": 9900.0,
                "size": 200,
                "is_dir": False,
                "is_atomic_write": True,
            }],
            fields=["file_path", "modified_time", "size", "is_dir", "is_atomic_write"],
            message_source=MessageSource.REALTIME,
            index=9900000,
        )
        await driver.process_event(rt_event)

    # Create parent dir via REALTIME
    with patch('time.time', return_value=10000.0):
        dir_event = UpdateEvent(
            event_schema="fs",
            table="dirs",
            rows=[{
                "path": "/dir",
                "modified_time": 10000.0,
                "is_dir": True,
                "size": 0,
            }],
            fields=["path", "modified_time", "is_dir", "size"],
            message_source=MessageSource.REALTIME,
            index=10000000,
        )
        await driver.process_event(dir_event)

    assert driver._get_node("/dir/missing.txt") is not None

    # Step 2: Audit cycle — scan parent /dir but DON'T report missing.txt
    await driver.handle_audit_start()
    driver._audit_seen_paths.add("/dir")

    await driver.handle_audit_end()

    # Step 3: Verify blind-spot deletions via API
    response = client.get("/blind-spots", params={"view_id": "1"})
    assert response.status_code == 200
    data = response.json()

    assert "/dir/missing.txt" in data["deletions"], \
        "File missing from audit should be in blind-spot deletions"
    assert data["deletion_count"] >= 1


@pytest.mark.asyncio
async def test_session_reset_clears_blind_spots(setup):
    """
    After on_session_start(), blind-spot lists should be cleared.
    Verified via /blind-spots API.
    """
    client, driver = setup

    # Populate blind-spots
    with patch('time.time', return_value=10000.0):
        event = UpdateEvent(
            event_schema="fs",
            table="files",
            rows=[{
                "file_path": "/bs_file.txt",
                "modified_time": 9000.0,
                "size": 50,
                "is_dir": False,
                "is_atomic_write": True,
            }],
            fields=["file_path", "modified_time", "size", "is_dir", "is_atomic_write"],
            message_source=MessageSource.AUDIT,
            index=9000000,
        )
        await driver.process_event(event)

    # Verify populated
    response = client.get("/blind-spots", params={"view_id": "1"})
    data = response.json()
    assert data["additions_count"] >= 1

    # Reset session
    await driver.on_session_start()

    # Verify cleared
    response = client.get("/blind-spots", params={"view_id": "1"})
    data = response.json()
    assert data["additions_count"] == 0, "Session reset should clear blind-spot additions"
    assert data["deletion_count"] == 0, "Session reset should clear blind-spot deletions"
