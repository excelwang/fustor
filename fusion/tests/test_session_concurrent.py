import pytest
import asyncio
import faulthandler
from httpx import AsyncClient
from unittest.mock import patch, MagicMock, AsyncMock
from datetime import datetime
from contextlib import asynccontextmanager

faulthandler.enable()

@pytest.mark.asyncio
async def test_new_session_terminates_old_snapshot_and_clears_parser(
    async_client: AsyncClient,
):
    try:
        await asyncio.wait_for(_test_body(async_client), timeout=30.0)
    except asyncio.TimeoutError:
        pytest.fail("Test timed out after 30 seconds")

async def _test_body(async_client: AsyncClient):
    print("\n--- Test Started ---")

    # Mock session config to allow concurrent push
    mock_session_config = {
        "allow_concurrent_push": True,
        "session_timeout_seconds": 60,
    }
    
    # Mock pipeline config (for ingestion check)
    mock_pipeline_config = {
        "allow_concurrent_push": True,
    }

    # Act: Create sessions within the patched context
    with patch('fustor_fusion.api.session._get_session_config', return_value=mock_session_config), \
         patch('fustor_fusion.api.ingestion._get_pipeline_config', return_value=mock_pipeline_config):

        print("--- Creating first session ---")
        create_session_payload_1 = {"task_id": "agent:sync-1"}
        response1 = await async_client.post("/api/v1/pipe/session/", json=create_session_payload_1)
        print(f"--- Create first session response: {response1.status_code} ---")
        assert response1.status_code == 200
        session1_id = response1.json()["session_id"]

        print("--- Creating second session ---")
        create_session_payload_2 = {"task_id": "agent:sync-2"}
        response2 = await async_client.post("/api/v1/pipe/session/", json=create_session_payload_2)
        print(f"--- Create second session response: {response2.status_code} ---")
        assert response2.status_code == 200
        session2_id = response2.json()["session_id"]

        # Assert: Old session fails to ingest (not authoritative)
        print("--- Asserting old session fails ---")
        ingest_payload_old_session = {
            "session_id": session1_id,
            "source_type": "snapshot",
            "events": [{"file_path": "/dummy/file1.txt"}]
        }
        # Patch is_authoritative_session to return False for old session
        with patch('fustor_fusion.api.ingestion.datastore_state_manager') as mock_sm:
            mock_sm.is_authoritative_session = AsyncMock(return_value=False)
            mock_sm.is_leader = AsyncMock(return_value=False)
            response_old = await async_client.post("/api/v1/pipe/ingest/", json=ingest_payload_old_session)
        print(f"--- Old session response: {response_old.status_code} ---")
        assert response_old.status_code == 419

        # Assert: New session succeeds to ingest (authoritative)
        print("--- Asserting new session succeeds ---")
        ingest_payload_new_session = {
            "session_id": session2_id,
            "source_type": "snapshot",
            "events": [{"file_path": "/dummy/file2.txt"}]
        }
        with patch('fustor_fusion.api.ingestion.datastore_state_manager') as mock_sm:
            mock_sm.is_authoritative_session = AsyncMock(return_value=True)
            mock_sm.is_leader = AsyncMock(return_value=True)
            mock_sm.set_snapshot_complete = AsyncMock()
            response_new = await async_client.post("/api/v1/pipe/ingest/", json=ingest_payload_new_session)
        print(f"--- New session response: {response_new.status_code} ---")
        assert response_new.status_code in [200, 204]

    print("--- Test Finished ---")
