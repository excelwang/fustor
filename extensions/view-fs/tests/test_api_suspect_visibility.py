"""
Test: API Suspect Visibility (Recovers deleted test_f).

Validates that:
  1. Suspect files expose integrity_suspect=True in tree API (to_dict)
  2. /suspect-list API returns the correct suspect paths
  3. After suspect is cleared, both APIs reflect the change
"""
import pytest
from unittest.mock import patch

from fastapi import FastAPI
from fastapi.testclient import TestClient

from fustor_view_fs.api import create_fs_router
from fustor_view_fs.driver import FSViewDriver
from fustor_core.event import UpdateEvent, MessageSource


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
        # Calibrate clock: skew=0
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


def _make_event(path, mtime, source, is_atomic=True):
    return UpdateEvent(
        event_schema="fs",
        table="files",
        rows=[{
            "file_path": path,
            "modified_time": mtime,
            "size": 100,
            "is_dir": False,
            "is_atomic_write": is_atomic,
        }],
        fields=["file_path", "modified_time", "size", "is_dir", "is_atomic_write"],
        message_source=source,
        index=int(mtime * 1000),
    )


# --- Tests ---

@pytest.mark.asyncio
async def test_suspect_flag_visible_in_tree_api(setup):
    """
    A file marked suspect should have integrity_suspect=True
    in the /tree API response.
    """
    client, driver = setup
    path = "/hot_file.txt"

    with patch('time.time', return_value=10000.0):
        event = _make_event(path, 9999.0, MessageSource.AUDIT)
        await driver.process_event(event)

    assert driver._get_node(path).integrity_suspect is True

    response = client.get("/tree", params={"path": "/", "view_id": "1"})
    assert response.status_code == 200
    data = response.json()

    # Find the file in the tree
    file_found = False
    for child in data.get("children", []):
        if child.get("name") == "hot_file.txt":
            assert child["integrity_suspect"] is True, \
                "API should expose integrity_suspect=True"
            file_found = True
            break
    assert file_found, "File should appear in tree"


@pytest.mark.asyncio
async def test_suspect_list_api_returns_paths(setup):
    """
    /suspect-list API should return the suspect file path.
    """
    client, driver = setup
    path = "/hot_file.txt"

    with patch('time.time', return_value=10000.0):
        event = _make_event(path, 9999.0, MessageSource.AUDIT)
        await driver.process_event(event)

    response = client.get("/suspect-list", params={"view_id": "1"})
    assert response.status_code == 200
    data = response.json()

    paths = [item["path"] for item in data]
    assert path in paths, "Suspect file should appear in /suspect-list"


@pytest.mark.asyncio
async def test_suspect_cleared_reflects_in_api(setup):
    """
    After a Realtime atomic event clears suspect,
    both /tree and /suspect-list should reflect the change.
    """
    client, driver = setup
    path = "/hot_file.txt"

    # Step 1: Create suspect via AUDIT
    with patch('time.time', return_value=10000.0):
        event = _make_event(path, 9999.0, MessageSource.AUDIT)
        await driver.process_event(event)
    assert driver._get_node(path).integrity_suspect is True

    # Step 2: Clear via REALTIME atomic write
    with patch('time.time', return_value=10001.0):
        rt_event = _make_event(path, 10000.0, MessageSource.REALTIME, is_atomic=True)
        await driver.process_event(rt_event)
    assert driver._get_node(path).integrity_suspect is False

    # Step 3: Verify tree API
    response = client.get("/tree", params={"path": "/", "view_id": "1"})
    data = response.json()
    for child in data.get("children", []):
        if child.get("name") == "hot_file.txt":
            assert child["integrity_suspect"] is False, \
                "Cleared suspect should show False in API"
            break

    # Step 4: Verify suspect-list API
    response = client.get("/suspect-list", params={"view_id": "1"})
    data = response.json()
    paths = [item["path"] for item in data]
    assert path not in paths, "Cleared file should not be in /suspect-list"
