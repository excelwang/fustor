import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient
from unittest.mock import MagicMock, AsyncMock, patch
from fustor_agent.app import App
from fustor_agent.api.routes import web_app
from fustor_core.models.config import AppConfig, SourceConfig, PasswdCredential, SourceConfigDict, PusherConfigDict, SyncConfigDict
from fustor_core.exceptions import ConfigError, NotFoundError

@pytest.fixture
def mock_app():
    app = MagicMock(spec=App)
    app._app_config = MagicMock(spec=AppConfig)
    app.source_config_service = MagicMock()
    app.source_config_service.add_config = AsyncMock()
    app.source_config_service.delete_config = AsyncMock()
    app.source_config_service.cleanup_obsolete_configs = AsyncMock(return_value=[])
    app.source_config_service.disable = AsyncMock()
    app.source_config_service.enable = AsyncMock()
    app.source_config_service.get_config = MagicMock()
    app.source_config_service.discover_and_cache_fields = AsyncMock()
    app.source_config_service.update_schema_cached_status = AsyncMock()

    app.source_driver_service = MagicMock()
    app.source_driver_service.get_schema_by_type = AsyncMock()
    app.source_driver_service.test_connection = AsyncMock()
    app.source_driver_service.check_params = AsyncMock()
    app.source_driver_service.get_available_fields = AsyncMock()
    app.source_driver_service.create_agent_user = AsyncMock()
    app.source_driver_service.check_privileges = AsyncMock()

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
def sample_source_config():
    return SourceConfig(driver="mysql", uri="mysql://host", credential=PasswdCredential(user="u"), disabled=False)

class TestSourcesApi:
    def test_get_configs_includes_sources(self, client, mock_app, sample_source_config):
        mock_app._app_config = AppConfig(
            sources=SourceConfigDict(root={"test_source": sample_source_config}),
            pushers=PusherConfigDict(root={}),
            syncs=SyncConfigDict(root={})
        )
        response = client.get("/api/configs/")
        assert response.status_code == 200
        data = response.json()
        assert isinstance(data, dict)
        assert "sources" in data
        assert "test_source" in data["sources"]
        assert data["sources"]["test_source"]["driver"] == "mysql"

    @pytest.mark.asyncio
    async def test_add_source_config_success(self, client, mock_app, sample_source_config):
        mock_app.source_config_service.add_config.return_value = sample_source_config
        payload = {"config": sample_source_config.model_dump(mode='json'), "discovered_fields": {"col1": "int"}}
        response = client.post("/api/configs/sources/new_source", json=payload)
        assert response.status_code == 201
        assert response.json() == {"id": "new_source", "config": sample_source_config.model_dump(mode='json')}
        mock_app.source_config_service.add_config.assert_called_once()

    @pytest.mark.asyncio
    async def test_add_source_config_conflict(self, client, mock_app, sample_source_config):
        mock_app.source_config_service.add_config.side_effect = ValueError("Source config already exists.")
        payload = {"config": sample_source_config.model_dump(mode='json')}
        response = client.post("/api/configs/sources/existing_source", json=payload)
        assert response.status_code == 409
        assert "Source config already exists." in response.json()["detail"]

    @pytest.mark.asyncio
    async def test_delete_source_config_success(self, client, mock_app):
        response = client.delete("/api/configs/sources/test_source")
        assert response.status_code == 204
        mock_app.source_config_service.delete_config.assert_called_once_with("test_source")

    @pytest.mark.asyncio
    async def test_delete_source_config_not_found(self, client, mock_app):
        mock_app.source_config_service.delete_config.side_effect = ValueError("Source config not found.")
        response = client.delete("/api/configs/sources/non_existent")
        assert response.status_code == 404
        assert "Source config not found." in response.json()["detail"]

    @pytest.mark.asyncio
    async def test_cleanup_obsolete_sources_success(self, client, mock_app):
        mock_app.source_config_service.cleanup_obsolete_configs.return_value = ["old_source1", "old_source2"]
        response = client.post("/api/configs/sources/_actions/cleanup")
        assert response.status_code == 200
        assert response.json() == {
            "message": "Cleanup successful. Deleted 2 obsolete source configurations.",
            "deleted_count": 2,
            "deleted_ids": ["old_source1", "old_source2"]
        }
        mock_app.source_config_service.cleanup_obsolete_configs.assert_called_once()

    @pytest.mark.asyncio
    async def test_disable_source_config_success(self, client, mock_app):
        response = client.post("/api/configs/sources/test_source/_actions/disable")
        assert response.status_code == 200
        assert response.json() == {"message": "Source config 'test_source' disabled successfully."}
        mock_app.source_config_service.disable.assert_called_once_with("test_source")

    @pytest.mark.asyncio
    async def test_enable_source_config_success(self, client, mock_app):
        response = client.post("/api/configs/sources/test_source/_actions/enable")
        assert response.status_code == 200
        assert response.json() == {"message": "Source config 'test_source' enabled successfully."}
        mock_app.source_config_service.enable.assert_called_once_with("test_source")

    @pytest.mark.asyncio
    async def test_discover_and_cache_source_fields_success(self, client, mock_app, sample_source_config):
        mock_app.source_config_service.get_config.return_value = sample_source_config
        response = client.post("/api/configs/sources/test_source/_actions/discover_and_cache_fields", json={
            "admin_user": "admin",
            "admin_password": "pass"
        })
        assert response.status_code == 200
        assert response.json() == {"message": "Fields discovered and cached successfully."}
        mock_app.source_config_service.discover_and_cache_fields.assert_called_once_with("test_source", "admin", "pass")
        mock_app.source_config_service.update_schema_cached_status.assert_called_once_with("test_source", True)

    @pytest.mark.asyncio
    async def test_get_source_available_fields(self, client, mock_app):
        mock_app.source_config_service.get_config.return_value = MagicMock()
        with patch('fustor_agent.services.schema_cache.load_source_schema', return_value={"col1": "int"}) as mock_load_schema:
            response = client.get("/api/drivers/sources/test_source/_actions/get_available_fields")
            assert response.status_code == 200
            assert response.json() == {"col1": "int"}
            mock_load_schema.assert_called_once_with("test_source")

    @pytest.mark.asyncio
    async def test_get_source_driver_wizard(self, client, mock_app):
        mock_app.source_driver_service.get_wizard_definition_by_type = AsyncMock(return_value={"driver_param": "string"})
        response = client.get("/api/drivers/sources/mysql/wizard")
        assert response.status_code == 200
        assert response.json() == {"driver_param": "string"}
        mock_app.source_driver_service.get_wizard_definition_by_type.assert_called_once_with("mysql")

    @pytest.mark.asyncio
    async def test_test_source_driver_connection(self, client, mock_app):
        mock_app.source_driver_service.test_connection.return_value = (True, "Connected")
        response = client.post("/api/drivers/sources/mysql/_actions/test_connection", json={
            "uri": "mysql://test",
            "admin_creds": {"user": "admin", "passwd": "pass"}
        })
        assert response.status_code == 200
        assert response.json() == {"success": True, "message": "Connected"}
        mock_app.source_driver_service.test_connection.assert_called_once()

    @pytest.mark.asyncio
    async def test_check_source_driver_params(self, client, mock_app):
        mock_app.source_driver_service.check_params.return_value = (True, "Params OK")
        response = client.post("/api/drivers/sources/mysql/_actions/check_params", json={
            "uri": "mysql://test",
            "admin_creds": {"user": "admin", "passwd": "pass"}
        })
        assert response.status_code == 200
        assert response.json() == {"success": True, "message": "Params OK"}
        mock_app.source_driver_service.check_params.assert_called_once()

    @pytest.mark.asyncio
    async def test_discover_fields_no_cache(self, client, mock_app):
        mock_app.source_driver_service.get_available_fields.return_value = {"properties": {"discovered_field": "type"}}
        response = client.post("/api/drivers/sources/mysql/_actions/discover_fields_no_cache", json={
            "uri": "mysql://test",
            "admin_creds": {"user": "admin", "passwd": "pass"}
        })
        assert response.status_code == 200
        assert response.json() == {
            "success": True,
            "message": "成功发现 1 个可用字段。",
            "fields": {"properties": {"discovered_field": "type"}}
        }
        mock_app.source_driver_service.get_available_fields.assert_called_once()

    @pytest.mark.asyncio
    async def test_wizard_create_agent_user(self, client, mock_app):
        mock_app.source_driver_service.create_agent_user.return_value = (True, "User created")
        response = client.post("/api/drivers/sources/mysql/_actions/create_agent_user", json={
            "uri": "mysql://test",
            "admin_creds": {"user": "admin", "passwd": "pass"},
            "credential": {"user": "agent", "passwd": "agent_pass"}
        })
        assert response.status_code == 200
        assert response.json() == {"success": True, "message": "User created"}
        mock_app.source_driver_service.create_agent_user.assert_called_once()

    @pytest.mark.asyncio
    async def test_wizard_check_privileges(self, client, mock_app):
        mock_app.source_driver_service.check_privileges.return_value = (True, "Privileges OK")
        response = client.post("/api/drivers/sources/mysql/_actions/check_privileges", 
            json={
            "uri": "mysql://test",
            "admin_creds": {"user": "admin", "passwd": "pass"},
            "credential": {"user": "agent", "passwd": "agent_pass"}
        })
        assert response.status_code == 200
        assert response.json() == {"success": True, "message": "Privileges OK"}
        mock_app.source_driver_service.check_privileges.assert_called_once()