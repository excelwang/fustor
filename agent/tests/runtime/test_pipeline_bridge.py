# agent/tests/runtime/test_pipeline_bridge.py
"""
Tests for PipelineBridge - migration utility from SyncInstance to AgentPipeline.
"""
import pytest
import os
from unittest.mock import MagicMock, patch
from typing import Any, Dict

from fustor_agent.runtime import (
    PipelineBridge,
    create_pipeline_from_config,
    AgentPipeline,
)


class MockSourceDriver:
    """Mock source driver for testing."""
    
    require_schema_discovery = False
    
    def __init__(self, id: str, config: Any):
        self.id = id
        self.config = config
    
    def get_snapshot_iterator(self):
        return iter([])
    
    def get_message_iterator(self, **kwargs):
        return iter([])


class MockSenderDriver:
    """Mock sender driver for testing."""
    
    def __init__(self, sender_id: str, endpoint: str, credential: Dict, config: Dict):
        self.id = sender_id
        self.endpoint = endpoint
        self.credential = credential
        self.config = config


@pytest.fixture
def mock_sender_driver_service():
    service = MagicMock()
    service._get_driver_by_type.return_value = MockSenderDriver
    return service


@pytest.fixture
def mock_source_driver_service():
    service = MagicMock()
    service._get_driver_by_type.return_value = MockSourceDriver
    return service


@pytest.fixture
def mock_pipeline_config():
    config = MagicMock()
    config.source = "my-source"
    config.sender = "my-sender"
    config.batch_size = 50
    config.heartbeat_interval_sec = 5
    config.audit_interval_sec = 300
    config.sentinel_interval_sec = 60
    config.session_timeout_seconds = 30
    return config


@pytest.fixture
def mock_source_config():
    config = MagicMock()
    config.driver = "fs"
    config.uri = "/data"
    return config


@pytest.fixture
def mock_sender_config():
    config = MagicMock()
    config.driver = "http"
    config.endpoint = "http://fusion:8000"
    config.view_id = "1"
    config.credential = MagicMock()
    config.credential.api_key = "test-key"
    config.credential.secret = "test-secret"
    return config


@pytest.fixture
def pipeline_bridge(mock_sender_driver_service, mock_source_driver_service):
    return PipelineBridge(
        sender_driver_service=mock_sender_driver_service,
        source_driver_service=mock_source_driver_service
    )


class TestPipelineBridgeInit:
    """Test PipelineBridge initialization."""
    
    def test_init(self, mock_sender_driver_service, mock_source_driver_service):
        """Bridge should initialize with driver services."""
        bridge = PipelineBridge(
            sender_driver_service=mock_sender_driver_service,
            source_driver_service=mock_source_driver_service
        )
        
        assert bridge._sender_driver_service is mock_sender_driver_service
        assert bridge._source_driver_service is mock_source_driver_service


class TestPipelineBridgeCreatePipeline:
    """Test PipelineBridge.create_pipeline."""
    
    def test_create_pipeline_returns_agent_pipeline(
        self, pipeline_bridge, mock_pipeline_config, mock_source_config, mock_sender_config
    ):
        """create_pipelineshould return AgentPipeline instance."""
        pipeline = pipeline_bridge.create_pipeline(
            pipeline_id="test-pipeline",
            agent_id="agent-1",
            pipeline_config=mock_pipeline_config,
            source_config=mock_source_config,
            sender_config=mock_sender_config
        )
        
        assert isinstance(pipeline, AgentPipeline)
        assert pipeline.id == "test-pipeline"
        assert pipeline.task_id == "agent-1:test-pipeline"
    
    def test_pipeline_has_handlers(
        self, pipeline_bridge, mock_pipeline_config, mock_source_config, mock_sender_config
    ):
        """Pipeline should have source and sender handlers."""
        pipeline = pipeline_bridge.create_pipeline(
            pipeline_id="test-pipeline",
            agent_id="agent-1",
            pipeline_config=mock_pipeline_config,
            source_config=mock_source_config,
            sender_config=mock_sender_config
        )
        
        assert pipeline.source_handler is not None
        assert pipeline.sender_handler is not None
    
    def test_pipeline_config_from_pipeline_config(
        self, pipeline_bridge, mock_pipeline_config, mock_source_config, mock_sender_config
    ):
        """Pipeline config should come from pipeline config."""
        pipeline = pipeline_bridge.create_pipeline(
            pipeline_id="test-pipeline",
            agent_id="agent-1",
            pipeline_config=mock_pipeline_config,
            source_config=mock_source_config,
            sender_config=mock_sender_config
        )
        
        assert pipeline.batch_size == 50
        assert pipeline.heartbeat_interval_sec == 5
        assert pipeline.audit_interval_sec == 300
    
    def test_initial_statistics_passed(
        self, pipeline_bridge, mock_pipeline_config, mock_source_config, mock_sender_config
    ):
        """Initial statistics should be passed to pipeline."""
        initial_stats = {"events_pushed": 100}
        
        pipeline = pipeline_bridge.create_pipeline(
            pipeline_id="test-pipeline",
            agent_id="agent-1",
            pipeline_config=mock_pipeline_config,
            source_config=mock_source_config,
            sender_config=mock_sender_config,
            initial_statistics=initial_stats
        )
        
        assert pipeline.statistics["events_pushed"] == 100


class TestConvenienceFunction:
    """Test create_pipeline_from_config."""
    
    def test_convenience_function(
        self, mock_sender_driver_service, mock_source_driver_service,
        mock_pipeline_config, mock_source_config, mock_sender_config
    ):
        """Convenience function should work correctly."""
        pipeline = create_pipeline_from_config(
            pipeline_id="test-pipeline",
            agent_id="agent-1",
            pipeline_config=mock_pipeline_config,
            source_config=mock_source_config,
            sender_config=mock_sender_config,
            sender_driver_service=mock_sender_driver_service,
            source_driver_service=mock_source_driver_service
        )
        
        assert isinstance(pipeline, AgentPipeline)



