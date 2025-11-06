import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient
from unittest.mock import MagicMock, AsyncMock, patch
from fustor_agent.app import App
from fustor_agent.api.routes import api_router
from fustor_core.models.states import EventBusInstance, SyncInstanceDTO, EventBusState, SyncState

@pytest.fixture
def mock_app():
    app = MagicMock(spec=App)
    app.event_bus_service = MagicMock()
    app.sync_instance_service = MagicMock()
    app.sync_instance_service.start_one = AsyncMock()
    app.sync_instance_service.stop_one = AsyncMock()
    return app

@pytest.fixture
def client(mock_app):
    from fustor_agent.api.dependencies import get_app
    app = FastAPI()
    app.include_router(api_router)
    app.dependency_overrides[get_app] = lambda: mock_app
    with TestClient(app) as c:
        yield c
    api_router.dependency_overrides = {}

class TestSyncInstancesHandlers:
    def test_list_bus_instances(self, client, mock_app):
        mock_bus_instance_dto = EventBusInstance(
            id="bus1", source_name="src1", state=EventBusState.PRODUCING, info="producing", statistics={}
        )
        mock_bus_instance = MagicMock()
        mock_bus_instance.get_dto.return_value = mock_bus_instance_dto
        mock_app.event_bus_service.list_instances.return_value = [mock_bus_instance]

        response = client.get("/instances/buses")
        assert response.status_code == 200
        assert response.json() == [mock_bus_instance_dto.model_dump(mode='json')]
        mock_app.event_bus_service.list_instances.assert_called_once()

    def test_list_sync_instances(self, client, mock_app):
        mock_sync_instance_dto = SyncInstanceDTO(
            id="sync1", state=SyncState.MESSAGE_SYNC, info="running", statistics={}
        )
        # --- END REFACTOR ---
        mock_sync_instance = MagicMock()
        mock_sync_instance.get_dto.return_value = mock_sync_instance_dto
        mock_app.sync_instance_service.list_instances.return_value = [mock_sync_instance]

        response = client.get("/instances/syncs")
        assert response.status_code == 200
        assert response.json() == [mock_sync_instance_dto.model_dump(mode='json')]
        mock_app.sync_instance_service.list_instances.assert_called_once()

    @pytest.mark.asyncio
    async def test_start_sync_instance_success(self, client, mock_app):
        response = client.post("/instances/syncs/test_sync/_actions/start")
        assert response.status_code == 202
        assert response.json() == {"message": "Sync instance test_sync start initiated."}
        mock_app.sync_instance_service.start_one.assert_called_once_with("test_sync")

    @pytest.mark.asyncio
    async def test_start_sync_instance_failure(self, client, mock_app):
        mock_app.sync_instance_service.start_one.side_effect = Exception("Failed to start")
        response = client.post("/instances/syncs/test_sync/_actions/start")
        assert response.status_code == 400
        assert "Failed to start" in response.json()["detail"]

    @pytest.mark.asyncio
    async def test_stop_sync_instance_success(self, client, mock_app):
        response = client.post("/instances/syncs/test_sync/_actions/stop")
        assert response.status_code == 202
        assert response.json() == {"message": "Sync instance test_sync stop initiated."}
        mock_app.sync_instance_service.stop_one.assert_called_once_with("test_sync")

    @pytest.mark.asyncio
    async def test_stop_sync_instance_failure(self, client, mock_app):
        mock_app.sync_instance_service.stop_one.side_effect = Exception("Failed to stop")
        response = client.post("/instances/syncs/test_sync/_actions/stop")
        assert response.status_code == 400
        assert "Failed to stop" in response.json()["detail"]