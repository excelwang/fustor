import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient
from unittest.mock import MagicMock, patch
from fustor_agent.app import App
from fustor_agent.api.routes import api_router
from fustor_core.models.log import LogEntry
from datetime import datetime

@pytest.fixture
def mock_app():
    app = MagicMock(spec=App)
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

@pytest.fixture
def mock_log_service():
    service = MagicMock()
    service.get_logs.return_value = [
        LogEntry(ts=datetime(2025, 7, 8, 10, 0, 0, 123000), level="INFO", source="fustor_agent.app", msg="App started", line_number=1),
        LogEntry(ts=datetime(2025, 7, 8, 10, 0, 1, 456000), level="DEBUG", source="fustor_agent.api", msg="API call", line_number=2)
    ]
    return service

class TestLogsHandlers:
    @patch('fustor_agent.api.logs_handlers.LogService')
    def test_list_logs_basic(self, MockLogService, client, mock_log_service):
        MockLogService.return_value = mock_log_service
        response = client.get("/logs/")
        assert response.status_code == 200
        assert len(response.json()) == 2
        assert response.json()[0]["msg"] == "App started"
        mock_log_service.get_logs.assert_called_once_with(limit=100, level=None, component=None, before_line=None)

    @patch('fustor_agent.api.logs_handlers.LogService')
    def test_list_logs_with_filters(self, MockLogService, client, mock_log_service):
        MockLogService.return_value = mock_log_service
        response = client.get("/logs/?limit=10&level=INFO&component=fustor_agent.app&before_line=50")
        assert response.status_code == 200
        mock_log_service.get_logs.assert_called_once_with(limit=10, level="INFO", component="fustor_agent.app", before_line=50)
