import pytest
import pytest_asyncio
from fastapi import FastAPI, Depends
from fastapi.testclient import TestClient
from unittest.mock import AsyncMock, MagicMock
from fustor_view_fs.api import create_fs_router
from fustor_view_fs.provider import FSViewProvider

# Mock Dependencies
async def mock_check_snapshot(datastore_id: int):
    pass # Always pass

def mock_get_datastore_id():
    return 1

# Setup App with Router
@pytest.fixture
def app_client():
    provider = AsyncMock(spec=FSViewProvider)
    # Setup default return values for provider methods to simulate a tree
    # We will let individual tests override this or set specific return values
    
    async def get_provider(datastore_id: int):
        return provider

    router = create_fs_router(
        get_provider_func=get_provider,
        check_snapshot_func=mock_check_snapshot,
        get_datastore_id_dep=mock_get_datastore_id
    )
    
    app = FastAPI()
    app.include_router(router) # mounts at /tree directly since router has no prefix in my previous fix? 
    # Wait, in Step 6367 I removed prefix="/fs" from APIRouter init.
    # So paths are /tree, /search etc.
    
    return TestClient(app), provider

@pytest.mark.asyncio
async def test_tree_default_recursive(app_client):
    client, provider = app_client
    
    # Mock behavior matches original test's expectations
    # Original test expected provider.get_directory_tree to return full structure
    provider.get_directory_tree.return_value = {
        "path": "/",
        "children": [
            {"name": "dir1", "children": [
                {"name": "file1.txt"},
                {"name": "subdir1", "children": [{"name": "file2.txt"}]}
            ]},
            {"name": "file3.txt"}
        ]
    }
    
    response = client.get("/tree", params={"path": "/"})
    assert response.status_code == 200
    data = response.json()
    
    # Verify calls passed to provider correctly
    provider.get_directory_tree.assert_called_with("/", recursive=True, max_depth=None, only_path=False)
    
    # Verify response structure logic (which is mostly pass-through but confirms wiring)
    assert data["path"] == "/"
    names = [c["name"] for c in data["children"]]
    assert "dir1" in names

@pytest.mark.asyncio
async def test_tree_non_recursive(app_client):
    client, provider = app_client
    provider.get_directory_tree.return_value = {"path": "/", "children": []}
    
    response = client.get("/tree", params={"path": "/", "recursive": "false"})
    assert response.status_code == 200
    
    # Verify recursive=False passed
    provider.get_directory_tree.assert_called_with("/", recursive=False, max_depth=None, only_path=False)

@pytest.mark.asyncio
async def test_tree_max_depth(app_client):
    client, provider = app_client
    provider.get_directory_tree.return_value = {"path": "/", "children": []}
    
    response = client.get("/tree", params={"path": "/", "max_depth": 2})
    assert response.status_code == 200
    
    provider.get_directory_tree.assert_called_with("/", recursive=True, max_depth=2, only_path=False)

@pytest.mark.asyncio
async def test_tree_only_path(app_client):
    client, provider = app_client
    provider.get_directory_tree.return_value = {"path": "/"}
    
    response = client.get("/tree", params={"path": "/", "only_path": "true"})
    assert response.status_code == 200
    
    provider.get_directory_tree.assert_called_with("/", recursive=True, max_depth=None, only_path=True)
