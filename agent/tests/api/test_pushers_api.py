import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient
from unittest.mock import MagicMock, AsyncMock, patch
from fustor_agent.app import App
from fustor_agent.api.routes import web_app
from fustor_core.models.config import AppConfig, PusherConfig, PasswdCredential, PusherConfigDict, SourceConfigDict, SyncConfigDict
from fustor_core.exceptions import ConfigError, NotFoundError

@pytest.fixture
def mock_app():
    app = MagicMock(spec=App)
    app._app_config = MagicMock(spec=AppConfig)
    app.pusher_config_service = MagicMock()
    app.pusher_config_service.add_config = AsyncMock()
    app.pusher_config_service.delete_config = AsyncMock()
    app.pusher_config_service.cleanup_obsolete_configs = AsyncMock(return_value=[])
    app.pusher_config_service.disable = AsyncMock()
    app.pusher_config_service.enable = AsyncMock()
    app.pusher_config_service.get_config = MagicMock()

    app.pusher_driver_service = MagicMock()
    app.pusher_driver_service.get_wizard_definition_by_type = AsyncMock()
    app.pusher_driver_service.test_connection = AsyncMock()
    app.pusher_driver_service.check_privileges = AsyncMock()
    app.pusher_driver_service.get_needed_fields = AsyncMock()

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
def sample_pusher_config():
    return PusherConfig(driver="http", endpoint="http://localhost", credential=PasswdCredential(user="u"), disabled=False)

class TestPushersApi:
    def test_get_configs_includes_pushers(self, client, mock_app, sample_pusher_config):
        mock_app._app_config = AppConfig(
            sources=SourceConfigDict(root={}),
            pushers=PusherConfigDict(root={"test_pusher": sample_pusher_config}),
            syncs=SyncConfigDict(root={})
        )
        response = client.get("/api/configs/")
        assert response.status_code == 200
        data = response.json()
        assert isinstance(data, dict)
        assert "pushers" in data
        assert "test_pusher" in data["pushers"]

    @pytest.mark.asyncio
    async def test_add_pusher_config_success(self, client, mock_app, sample_pusher_config):
        mock_app.pusher_config_service.add_config.return_value = sample_pusher_config
        payload = {"config": sample_pusher_config.model_dump(mode='json')}
        response = client.post("/api/configs/pushers/new_pusher", json=payload)
        assert response.status_code == 201
        assert response.json() == {"id": "new_pusher", "config": sample_pusher_config.model_dump(mode='json')}
        mock_app.pusher_config_service.add_config.assert_called_once()

    @pytest.mark.asyncio
    async def test_add_pusher_config_conflict(self, client, mock_app, sample_pusher_config):
        mock_app.pusher_config_service.add_config.side_effect = ValueError("Pusher config already exists.")
        payload = {"config": sample_pusher_config.model_dump(mode='json')}
        response = client.post("/api/configs/pushers/existing_pusher", json=payload)
        assert response.status_code == 409
        assert "Pusher config already exists." in response.json()["detail"]

    @pytest.mark.asyncio
    async def test_delete_pusher_config_success(self, client, mock_app):
        response = client.delete("/api/configs/pushers/test_pusher")
        assert response.status_code == 204
        mock_app.pusher_config_service.delete_config.assert_called_once_with("test_pusher")

    @pytest.mark.asyncio
    async def test_delete_pusher_config_not_found(self, client, mock_app):
        mock_app.pusher_config_service.delete_config.side_effect = ValueError("Pusher config not found.")
        response = client.delete("/api/configs/pushers/non_existent")
        assert response.status_code == 404
        assert "Pusher config not found." in response.json()["detail"]

    @pytest.mark.asyncio
    async def test_cleanup_obsolete_pushers_success(self, client, mock_app):
        mock_app.pusher_config_service.cleanup_obsolete_configs.return_value = ["old_pusher1", "old_pusher2"]
        response = client.post("/api/configs/pushers/_actions/cleanup")
        assert response.status_code == 200
        assert response.json() == {
            "message": "Cleanup successful. Deleted 2 obsolete pusher configurations.",
            "deleted_count": 2,
            "deleted_ids": ["old_pusher1", "old_pusher2"]
        }
        mock_app.pusher_config_service.cleanup_obsolete_configs.assert_called_once()

    @pytest.mark.asyncio
    async def test_disable_pusher_config_success(self, client, mock_app):
        response = client.post("/api/configs/pushers/test_pusher/_actions/disable")
        assert response.status_code == 200
        assert response.json() == {"message": f"Pusher config 'test_pusher' disabled successfully."}
        mock_app.pusher_config_service.disable.assert_called_once_with("test_pusher")

    @pytest.mark.asyncio
    async def test_enable_pusher_config_success(self, client, mock_app):
        response = client.post("/api/configs/pushers/test_pusher/_actions/enable")
        assert response.status_code == 200
        assert response.json() == {"message": f"Pusher config 'test_pusher' enabled successfully."}
        mock_app.pusher_config_service.enable.assert_called_once_with("test_pusher")

    @pytest.mark.asyncio
    async def test_get_pusher_needed_fields(self, client, mock_app, sample_pusher_config):
        mock_app.pusher_config_service.get_config.return_value = sample_pusher_config
        mock_app.pusher_driver_service.get_needed_fields.return_value = {"field1": "type1"}
        # --- REFACTORED: Corrected the API path from '/configs' to '/drivers' ---
        response = client.get("/api/drivers/pushers/test_pusher/_actions/get_needed_fields")
        # --- END REFACTOR ---
        assert response.status_code == 200
        assert response.json() == {"field1": "type1"}
        mock_app.pusher_driver_service.get_needed_fields.assert_called_once_with("http", "http://localhost")

    @pytest.mark.asyncio
    async def test_get_pusher_driver_wizard(self, client, mock_app):
        mock_app.pusher_driver_service.get_wizard_definition_by_type.return_value = {"driver_param": "string"}
        response = client.get("/api/drivers/pushers/http/wizard")
        assert response.status_code == 200
        assert response.json() == {"driver_param": "string"}
        mock_app.pusher_driver_service.get_wizard_definition_by_type.assert_called_once_with("http")

    @pytest.mark.asyncio
    async def test_test_pusher_driver_connection(self, client, mock_app):
        mock_app.pusher_driver_service.test_connection.return_value = (True, "Connected")
        response = client.post("/api/drivers/pushers/http/_actions/test_connection", json={
            "endpoint": "http://test"
        })
        assert response.status_code == 200
        assert response.json() == {"success": True, "message": "Connected"}
        mock_app.pusher_driver_service.test_connection.assert_called_once()

    @pytest.mark.asyncio
    async def test_check_pusher_driver_privileges(self, client, mock_app):
        mock_app.pusher_driver_service.check_privileges.return_value = (True, "Privileges OK")
        response = client.post("/api/drivers/pushers/http/_actions/check_privileges", json={
            "endpoint": "http://test",
            "credential": {"user": "admin", "passwd": "pass"}
        })
        assert response.status_code == 200
        assert response.json() == {"success": True, "message": "Privileges OK"}
        mock_app.pusher_driver_service.check_privileges.assert_called_once()

    @pytest.mark.asyncio
    async def test_pusher_test_connection_saved_config(self, client, mock_app, sample_pusher_config):
        mock_app.pusher_config_service.get_config.return_value = sample_pusher_config
        mock_app.pusher_driver_service.test_connection.return_value = (True, "Connected")
        response = client.post("/api/configs/pushers/test_pusher/_actions/test_connection")
        assert response.status_code == 200
        assert response.json() == {"success": True, "message": "Connected"}
        mock_app.pusher_driver_service.test_connection.assert_called_once_with(driver_type='http', endpoint='http://localhost')

    @pytest.mark.asyncio
    async def test_pusher_check_privileges_saved_config(self, client, mock_app, sample_pusher_config):
        mock_app.pusher_config_service.get_config.return_value = sample_pusher_config
        mock_app.pusher_driver_service.check_privileges.return_value = (True, "Privileges OK")
        response = client.post("/api/configs/pushers/test_pusher/_actions/check_privileges")
        assert response.status_code == 200
        assert response.json() == {"success": True, "message": "Privileges OK"}
        mock_app.pusher_driver_service.check_privileges.assert_called_once_with(driver_type='http', endpoint='http://localhost', credential=sample_pusher_config.credential)