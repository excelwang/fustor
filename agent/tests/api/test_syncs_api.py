import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient
from unittest.mock import MagicMock, AsyncMock, patch
from fustor_agent.app import App
from fustor_agent.api.routes import web_app
from fustor_core.models.config import AppConfig, SyncConfig, PasswdCredential, SyncConfigDict, SourceConfigDict, PusherConfigDict
from fustor_core.exceptions import ConfigError, NotFoundError

@pytest.fixture
def mock_app():
    app = MagicMock(spec=App)
    app._app_config = MagicMock(spec=AppConfig)
    app.sync_config_service = MagicMock()
    app.sync_config_service.add_config = AsyncMock()
    app.sync_config_service.delete_config = AsyncMock()
    app.sync_config_service.disable = AsyncMock()
    app.sync_config_service.enable = AsyncMock()
    return app

@pytest.fixture
def client(mock_app):
    from fustor_agent.api.dependencies import get_app
    from fustor_agent.app import App

    with patch.object(App, "startup", new=AsyncMock()):
        web_app.dependency_overrides[get_app] = lambda: mock_app
        with TestClient(web_app) as c:
            yield c
    web_app.dependency_overrides = {}

@pytest.fixture
def sample_sync_config():
    return SyncConfig(source="source1", pusher="pusher1", disabled=False)

class TestSyncsApi:
    def test_get_configs_includes_syncs(self, client, mock_app, sample_sync_config):
        mock_app._app_config = AppConfig(
            sources=SourceConfigDict(root={}),
            pushers=PusherConfigDict(root={}),
            syncs=SyncConfigDict(root={"test_sync": sample_sync_config})
        )
        response = client.get("/api/configs/")
        assert response.status_code == 200
        data = response.json()
        assert isinstance(data, dict)
        assert "syncs" in data
        assert "test_sync" in data["syncs"]
        assert data["syncs"]["test_sync"]["source"] == "source1"

    @pytest.mark.asyncio
    async def test_add_sync_config_success(self, client, mock_app, sample_sync_config):
        mock_app.sync_config_service.add_config.return_value = sample_sync_config
        response = client.post("/api/configs/syncs/new_sync", json=sample_sync_config.model_dump(mode='json'))
        assert response.status_code == 201
        assert response.json() == {"id": "new_sync", "config": sample_sync_config.model_dump(mode='json')}
        mock_app.sync_config_service.add_config.assert_called_once()

    @pytest.mark.asyncio
    async def test_add_sync_config_conflict(self, client, mock_app, sample_sync_config):
        mock_app.sync_config_service.add_config.side_effect = ValueError("Sync config already exists.")
        response = client.post("/api/configs/syncs/existing_sync", json=sample_sync_config.model_dump(mode='json'))
        assert response.status_code == 409
        assert "Sync config already exists." in response.json()["detail"]

    @pytest.mark.asyncio
    async def test_delete_sync_config_success(self, client, mock_app):
        response = client.delete("/api/configs/syncs/test_sync")
        assert response.status_code == 204
        mock_app.sync_config_service.delete_config.assert_called_once_with("test_sync")

    @pytest.mark.asyncio
    async def test_delete_sync_config_not_found(self, client, mock_app):
        mock_app.sync_config_service.delete_config.side_effect = ValueError("Sync config not found.")
        response = client.delete("/api/configs/syncs/non_existent")
        assert response.status_code == 404
        assert "Sync config not found." in response.json()["detail"]

    @pytest.mark.asyncio
    async def test_disable_sync_config_success(self, client, mock_app):
        response = client.post("/api/configs/syncs/test_sync/_actions/disable")
        assert response.status_code == 204
        mock_app.sync_config_service.disable.assert_called_once_with("test_sync")

    @pytest.mark.asyncio
    async def test_enable_sync_config_success(self, client, mock_app):
        response = client.post("/api/configs/syncs/test_sync/_actions/enable")
        assert response.status_code == 200
        assert response.json() == {"message": "Sync config 'test_sync' enabled successfully."}
        mock_app.sync_config_service.enable.assert_called_once_with("test_sync")
