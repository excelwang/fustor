import pytest
import pytest_asyncio
from fastapi import FastAPI, Depends
from fastapi.testclient import TestClient
from unittest.mock import AsyncMock, MagicMock
from fustor_view_fs.api import create_fs_router
from fustor_view_fs.driver import FSViewDriver

# Mock Dependencies
async def mock_check_snapshot(view_id: str):
    pass # Always pass

async def mock_get_view_id():
    return "1"

# Setup App with Router
@pytest.fixture
def app_client():
    driver = AsyncMock(spec=FSViewDriver)
    # Setup default return values for driver methods to simulate a tree
    # We will let individual tests override this or set specific return values
    
    async def get_driver(view_id: str):
        return driver

    router = create_fs_router(
        get_driver_func=get_driver,
        check_snapshot_func=mock_check_snapshot,
        get_view_id_dep=mock_get_view_id
    )
    
    app = FastAPI()
    app.include_router(router) # mounts at /tree directly since router has no prefix in my previous fix? 
    # Wait, in Step 6367 I removed prefix="/fs" from APIRouter init.
    # So paths are /tree, /search etc.
    
    return TestClient(app), driver

@pytest.mark.asyncio
async def test_tree_default_recursive(app_client):
    client, driver = app_client
    
    # Mock behavior matches original test's expectations
    # Original test expected driver.get_directory_tree to return full structure
    driver.get_directory_tree.return_value = {
        "path": "/",
        "children": [
            {"name": "dir1", "children": [
                {"name": "file1.txt"},
                {"name": "subdir1", "children": [{"name": "file2.txt"}]}
            ]},
            {"name": "file3.txt"}
        ]
    }
    
    response = client.get("/tree", params={"path": "/", "view_id": "1"})
    assert response.status_code == 200
    data = response.json()
    
    # Verify calls passed to driver correctly
    driver.get_directory_tree.assert_called_with("/", recursive=True, max_depth=None, only_path=False)
    
    # Verify response structure logic (which is mostly pass-through but confirms wiring)
    # API now wraps result in "data" field
    assert data["data"]["path"] == "/"
    names = [c["name"] for c in data["data"]["children"]]
    assert "dir1" in names

@pytest.mark.asyncio
async def test_tree_non_recursive(app_client):
    client, driver = app_client
    driver.get_directory_tree.return_value = {"path": "/", "children": []}
    
    response = client.get("/tree", params={"path": "/", "recursive": "false", "view_id": "1"})
    assert response.status_code == 200
    
    # Verify recursive=False passed
    driver.get_directory_tree.assert_called_with("/", recursive=False, max_depth=None, only_path=False)

@pytest.mark.asyncio
async def test_tree_max_depth(app_client):
    client, driver = app_client
    driver.get_directory_tree.return_value = {"path": "/", "children": []}
    
    response = client.get("/tree", params={"path": "/", "max_depth": 2, "view_id": "1"})
    assert response.status_code == 200
    
    driver.get_directory_tree.assert_called_with("/", recursive=True, max_depth=2, only_path=False)

@pytest.mark.asyncio
async def test_tree_only_path(app_client):
    client, driver = app_client
    driver.get_directory_tree.return_value = {"path": "/"}
    
    response = client.get("/tree", params={"path": "/", "only_path": "true", "view_id": "1"})
    assert response.status_code == 200
    
    driver.get_directory_tree.assert_called_with("/", recursive=True, max_depth=None, only_path=True)
