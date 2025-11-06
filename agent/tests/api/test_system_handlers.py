import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient
from unittest.mock import MagicMock, AsyncMock, patch
from fustor_agent.app import App
from fustor_agent.api.routes import api_router
from fustor_core.models.config import SyncConfig, SourceConfig, PusherConfig
from fustor_core.models.states import SyncState, EventBusState

@pytest.fixture
def mock_app():
    app = MagicMock(spec=App)
    app.sync_config_service = MagicMock()
    app.source_config_service = MagicMock()
    app.pusher_config_service = MagicMock()
    app.sync_instance_service = MagicMock()
    app.sync_instance_service.restart_outdated_syncs = AsyncMock(return_value=2)
    app._app_config = MagicMock()
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

class TestSystemHandlers:
    @pytest.mark.asyncio
    async def test_get_all_instances_status(self, client, mock_app):
        # Mock config services
        mock_app.sync_config_service.list_configs.return_value = {
            "sync1": SyncConfig(source="src1", pusher="rec1", disabled=False),
            "sync2": SyncConfig(source="src2", pusher="rec2", disabled=True),
            "sync3": SyncConfig(source="src3", pusher="rec3", disabled=False),
        }
        mock_app._app_config.check_sync_is_disabled.side_effect = lambda x: x == "sync2"

        # Mock sync instances
        mock_sync_instance1 = MagicMock()
        mock_sync_instance1.id = "sync1"
        # --- REFACTORED: Use a valid v2 state ---
        mock_sync_instance1.state = SyncState.MESSAGE_SYNC
        mock_sync_instance1.config = SyncConfig(source="src1", pusher="rec1", disabled=False)
        mock_sync_instance1.get_dto.return_value.model_dump.return_value = {"id": "sync1", "state": "MESSAGE_SYNC", "info": "running"}
        mock_sync_instance1.bus = MagicMock()
        mock_sync_instance1.bus.state = EventBusState.PRODUCING
        mock_sync_instance1.bus.get_dto.return_value.model_dump.return_value = {"id": "bus1", "state": "PRODUCING"}

        mock_sync_instance3 = MagicMock()
        mock_sync_instance3.id = "sync3"
        mock_sync_instance3.state = SyncState.RUNNING_CONF_OUTDATE
        mock_sync_instance3.config = SyncConfig(source="src3", pusher="rec3", disabled=False)
        mock_sync_instance3.get_dto.return_value.model_dump.return_value = {"id": "sync3", "state": "RUNNING_CONF_OUTDATE", "info": "outdated"}
        mock_sync_instance3.bus = MagicMock()
        mock_sync_instance3.bus.state = EventBusState.ERROR # Bus error should override sync state
        mock_sync_instance3.bus.get_dto.return_value.model_dump.return_value = {"id": "bus3", "state": "ERROR"}

        mock_app.sync_instance_service.list_instances.return_value = [mock_sync_instance1, mock_sync_instance3]

        response = client.get("/instances/status")
        assert response.status_code == 200
        data = response.json()

        assert data["global_summary"] == {
            "running_pipelines": 1, # sync1 is in MESSAGE_SYNC
            "outdated_pipelines": 0, # sync3 is ERROR due to bus
            "error_pipelines": 1 # sync3
        }

        pipelines = data["pipelines"]
        assert len(pipelines) == 3

        # Check sync1
        sync1_data = next(p for p in pipelines if p["id"] == "sync1")
        assert sync1_data["overall_status"] == "MESSAGE_SYNC"
        assert sync1_data["source_id"] == "src1"
        assert sync1_data["pusher_id"] == "rec1"
        assert sync1_data["bus_info"]["state"] == "PRODUCING"
        assert sync1_data["sync_info"]["state"] == "MESSAGE_SYNC"
        assert sync1_data["is_disabled"] is False

        # Check sync2 (disabled)
        sync2_data = next(p for p in pipelines if p["id"] == "sync2")
        assert sync2_data["overall_status"] == "STOPPED"
        assert sync2_data["source_id"] == "src2"
        assert sync2_data["pusher_id"] == "rec2"
        assert sync2_data["bus_info"] is None
        assert sync2_data["sync_info"]["state"] == "STOPPED"
        assert sync2_data["sync_info"]["info"] == "任务已禁用"
        assert sync2_data["is_disabled"] is True

        # Check sync3 (outdated but bus error)
        sync3_data = next(p for p in pipelines if p["id"] == "sync3")
        assert sync3_data["overall_status"] == "ERROR"
        assert sync3_data["source_id"] == "src3"
        assert sync3_data["pusher_id"] == "rec3"
        assert sync3_data["bus_info"]["state"] == "ERROR"
        assert sync3_data["sync_info"]["state"] == "RUNNING_CONF_OUTDATE"
        assert sync3_data["is_disabled"] is False

    @pytest.mark.asyncio
    async def test_apply_all_pending_changes_success(self, client, mock_app):
        mock_app.sync_instance_service.restart_outdated_syncs.return_value = 2
        response = client.post("/instances/_actions/apply_changes")
        assert response.status_code == 202
        assert response.json() == {"message": "Initiated restart for 2 outdated sync tasks."}
        mock_app.sync_instance_service.restart_outdated_syncs.assert_called_once()

    @pytest.mark.asyncio
    async def test_apply_all_pending_changes_failure(self, client, mock_app):
        mock_app.sync_instance_service.restart_outdated_syncs.side_effect = Exception("Restart failed")
        response = client.post("/instances/_actions/apply_changes")
        assert response.status_code == 500
        assert "Restart failed" in response.json()["detail"]