# agent/tests/test_pipelines_config.py
"""
Tests for Agent's YAML pipeline configuration loader.
"""
import pytest
import tempfile
from pathlib import Path
import yaml

from fustor_agent.config.validators import validate_url_safe_id
from fustor_agent.config.pipelines import PipelinesConfigLoader, AgentPipelineConfig


class TestPipelinesConfigLoader:
    """Tests for PipelinesConfigLoader."""
    
    def test_scan_directory(self, tmp_path):
        """Should scan directory and load all pipeline configs."""
        pipelines_dir = tmp_path / "agent-pipes-config"
        pipelines_dir.mkdir()
        
        (pipelines_dir / "pipe-a.yaml").write_text(yaml.dump({
            "id": "pipe-a",
            "source": "fs-source",
            "sender": "fusion-sender"
        }))
        
        (pipelines_dir / "pipe-b.yaml").write_text(yaml.dump({
            "id": "pipe-b",
            "source": "db-source",
            "sender": "fusion-sender",
            "disabled": True
        }))
        
        loader = PipelinesConfigLoader(pipelines_dir)
        pipelines = loader.scan()
        
        assert len(pipelines) == 2
        assert "pipe-a" in pipelines
        assert "pipe-b" in pipelines
        assert pipelines["pipe-a"].source == "fs-source"
        assert pipelines["pipe-b"].disabled == True
    
    def test_get_enabled(self, tmp_path):
        """Should return only enabled pipelines."""
        pipelines_dir = tmp_path / "agent-pipes-config"
        pipelines_dir.mkdir()
        
        (pipelines_dir / "enabled.yaml").write_text(yaml.dump({
            "id": "enabled",
            "source": "src",
            "sender": "push"
        }))
        
        (pipelines_dir / "disabled.yaml").write_text(yaml.dump({
            "id": "disabled",
            "source": "src",
            "sender": "push",
            "disabled": True
        }))
        
        loader = PipelinesConfigLoader(pipelines_dir)
        loader.scan()
        
        enabled = loader.get_enabled()
        assert len(enabled) == 1
        assert "enabled" in enabled
    
    def test_invalid_id_rejected(self, tmp_path):
        """Should reject pipeline config with invalid ID."""
        pipelines_dir = tmp_path / "agent-pipes-config"
        pipelines_dir.mkdir()
        
        (pipelines_dir / "invalid.yaml").write_text(yaml.dump({
            "id": "Invalid Pipeline",  # uppercase and space
            "source": "src",
            "sender": "push"
        }))
        
        loader = PipelinesConfigLoader(pipelines_dir)
        pipelines = loader.scan()
        
        # Should not load invalid config
        assert len(pipelines) == 0
    
    def test_default_intervals(self, tmp_path):
        """Should use default values for optional fields."""
        pipelines_dir = tmp_path / "agent-pipes-config"
        pipelines_dir.mkdir()
        
        (pipelines_dir / "minimal.yaml").write_text(yaml.dump({
            "id": "minimal",
            "source": "src",
            "sender": "push"
        }))
        
        loader = PipelinesConfigLoader(pipelines_dir)
        pipelines = loader.scan()
        
        config = pipelines["minimal"]
        assert config.audit_interval_sec == 600
        assert config.sentinel_interval_sec == 120
        assert config.heartbeat_interval_sec == 10
        assert config.disabled == False
