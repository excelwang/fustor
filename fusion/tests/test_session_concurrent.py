import pytest
import asyncio
import faulthandler
from httpx import AsyncClient
from unittest.mock import patch, MagicMock, AsyncMock
from datetime import datetime
from contextlib import asynccontextmanager


from fustor_fusion.config import DatastoreConfig

faulthandler.enable()

@pytest.mark.asyncio
async def test_new_session_terminates_old_snapshot_and_clears_parser(
    async_client: AsyncClient,
    # test_engine is no longer needed since we removed database dependencies
):
    try:
        await asyncio.wait_for(_test_body(async_client), timeout=30.0)
    except asyncio.TimeoutError:
        pytest.fail("Test timed out after 30 seconds")

async def _test_body(async_client: AsyncClient):
    print("\n--- Test Started ---")
    datastore_id = "1"

    # 1. Arrange: Mock datastore config
    mock_config = DatastoreConfig(
        id="test-concurrent",
        datastore_id=datastore_id,
        api_key="key3",
        allow_concurrent_push=True,
        session_timeout_seconds=60,
    )

    # 2. Act: Create sessions within the patched context
    with patch('fustor_fusion.api.session.datastores_config.get_datastore', return_value=mock_config), \
         patch('fustor_fusion.api.ingestion.datastores_config', return_value=mock_config):

        print("--- Creating first session ---")
        create_session_payload_1 = {"task_id": "agent:sync-1"}
        response1 = await async_client.post("/api/v1/ingest/sessions/", json=create_session_payload_1)
        print(f"--- Create first session response: {response1.status_code} ---")
        assert response1.status_code == 200
        session1_id = response1.json()["session_id"]

        print("--- Creating second session ---")
        create_session_payload_2 = {"task_id": "agent:sync-2"}
        response2 = await async_client.post("/api/v1/ingest/sessions/", json=create_session_payload_2)
        print(f"--- Create second session response: {response2.status_code} ---")
        assert response2.status_code == 200
        session2_id = response2.json()["session_id"]

        # 3. Assert: Old session fails to ingest
        print("--- Asserting old session fails ---")
        ingest_payload_old_session = {
            "session_id": session1_id,
            "source_type": "snapshot",
            "events": [{"file_path": "/dummy/file1.txt"}]
        }
        with patch('fustor_fusion.api.ingestion.datastore_state_manager.is_authoritative_session', return_value=False):
            response_old = await async_client.post("/api/v1/ingest/events/", json=ingest_payload_old_session)
        print(f"--- Old session response: {response_old.status_code} ---")
        assert response_old.status_code == 419

        # 4. Assert: New session succeeds to ingest
        print("--- Asserting new session succeeds ---")
        ingest_payload_new_session = {
            "session_id": session2_id,
            "source_type": "snapshot",
            "events": [{"file_path": "/dummy/file2.txt"}]
        }
        with patch('fustor_fusion.api.ingestion.datastore_state_manager.is_authoritative_session', return_value=True):
            response_new = await async_client.post("/api/v1/ingest/events/", json=ingest_payload_new_session)
        print(f"--- New session response: {response_new.status_code} ---")
        assert response_new.status_code == 204

    print("--- Test Finished ---")
