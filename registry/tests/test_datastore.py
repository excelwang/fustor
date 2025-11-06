import pytest
pytestmark = pytest.mark.asyncio

from fustor_registry.datastore.router import router as datastore_router

base_path = f"/v1{datastore_router.prefix}"


async def test_create_datastore_valid(authorized_client):
    payload = {
        "name": "search_db",
    }
    response = await authorized_client.post(base_path, json=payload, follow_redirects=True)
    assert response.status_code == 201, response.json()

async def test_create_submitstore_valid(authorized_client):
    payload = {
        "name": "valid_submit_db",
        "meta": {
            "temp_storage": {
                "storage_id": 1,
                "relative_path": "/tmp/temp"
            },
            "archive_storage": {
                "storage_id": 2,
                "relative_path": "/tmp/archive"
            },
            "public_storage": {
                "storage_id": 3,
                "relative_path": "/tmp/public"
            }
        }
    }
    response = await authorized_client.post(base_path, json=payload, follow_redirects=True)
    assert response.status_code == 201, response.json()