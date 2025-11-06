import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient
from unittest.mock import MagicMock, AsyncMock, patch
from fustor_agent.app import App
from fustor_agent.api.routes import api_router
from fustor_core.models.states import EventBusInstance, SyncInstanceDTO, EventBusState, SyncState
from fustor_core.models.config import SyncConfig

@pytest.fixture
def mock_app():
    app = MagicMock(spec=App)
    app.bus_service = MagicMock()
    app.sync_instance_service = MagicMock()
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

class TestMetricsApi:
    def test_get_metrics(self, client, mock_app):
        # Mock EventBusService
        mock_bus_instance = MagicMock()
        mock_bus_instance.source_id = "source_a"
        mock_bus_instance.get_dto.return_value.statistics = {
            "events_produced_total": 100,
            "bus_queue_size": 5,
        }
        mock_app.bus_service.list_instances.return_value = {"bus_1": mock_bus_instance}

        # Mock SyncInstanceService
        mock_sync_instance = MagicMock()
        mock_sync_instance.id = "sync_x"
        mock_sync_instance.config = SyncConfig(source="source_a", pusher="pusher_b", disabled=False)
        mock_sync_instance.get_dto.return_value.statistics = {
            "events_pushed_total": 50,
            "sync_push_latency_seconds": [0.1, 0.2, 0.3],
        }
        mock_app.sync_instance_service.list_instances.return_value = {"sync_x": mock_sync_instance}

        response = client.get("/metrics")
        assert response.status_code == 200
        assert response.headers["content-type"] == "text/plain; version=0.0.4; charset=utf-8"

        expected_metrics_lines = [
            '# HELP fustor_agent_events_produced_total Total number of events produced by source bus.',
            '# TYPE fustor_agent_events_produced_total counter',
            'fustor_agent_events_produced_total{source_id="source_a",bus_id="bus_1"} 100',
            '# HELP fustor_agent_bus_queue_size Current number of events in the bus queue.',
            '# TYPE fustor_agent_bus_queue_size gauge',
            'fustor_agent_bus_queue_size{source_id="source_a",bus_id="bus_1"} 5',
            '# HELP fustor_agent_events_pushed_total Total number of events successfully pushed to pusher.',
            '# TYPE fustor_agent_events_pushed_total counter',
            'fustor_agent_events_pushed_total{sync_id="sync_x",source_id="source_a",pusher_id="pusher_b"} 50',
            '# HELP fustor_agent_sync_push_latency_seconds Latency of pushing event batches to pushers.',
            '# TYPE fustor_agent_sync_push_latency_seconds summary',
            'fustor_agent_sync_push_latency_seconds_sum{sync_id="sync_x",source_id="source_a",pusher_id="pusher_b"} 0.6',
            'fustor_agent_sync_push_latency_seconds_count{sync_id="sync_x",source_id="source_a",pusher_id="pusher_b"} 3',
        ]

        # Convert response content to a set for order-independent comparison
        response_lines = set(response.text.strip().split('\n'))
        expected_lines = set(expected_metrics_lines)
        
        assert response_lines == expected_lines
