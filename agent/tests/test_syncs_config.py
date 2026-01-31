# agent/tests/test_syncs_config.py
"""
Tests for Agent's YAML sync configuration loader.
"""
import pytest
import tempfile
from pathlib import Path
import yaml

from fustor_agent.config.validators import validate_url_safe_id
from fustor_agent.config.syncs import SyncsConfigLoader, SyncConfigYaml


class TestSyncsConfigLoader:
    """Tests for SyncsConfigLoader."""
    
    def test_scan_directory(self, tmp_path):
        """Should scan directory and load all sync configs."""
        syncs_dir = tmp_path / "syncs-config"
        syncs_dir.mkdir()
        
        (syncs_dir / "sync-a.yaml").write_text(yaml.dump({
            "id": "sync-a",
            "source": "fs-source",
            "pusher": "fusion-pusher"
        }))
        
        (syncs_dir / "sync-b.yaml").write_text(yaml.dump({
            "id": "sync-b",
            "source": "db-source",
            "pusher": "fusion-pusher",
            "disabled": True
        }))
        
        loader = SyncsConfigLoader(syncs_dir)
        syncs = loader.scan()
        
        assert len(syncs) == 2
        assert "sync-a" in syncs
        assert "sync-b" in syncs
        assert syncs["sync-a"].source == "fs-source"
        assert syncs["sync-b"].disabled == True
    
    def test_get_enabled(self, tmp_path):
        """Should return only enabled syncs."""
        syncs_dir = tmp_path / "syncs-config"
        syncs_dir.mkdir()
        
        (syncs_dir / "enabled.yaml").write_text(yaml.dump({
            "id": "enabled",
            "source": "src",
            "pusher": "push"
        }))
        
        (syncs_dir / "disabled.yaml").write_text(yaml.dump({
            "id": "disabled",
            "source": "src",
            "pusher": "push",
            "disabled": True
        }))
        
        loader = SyncsConfigLoader(syncs_dir)
        loader.scan()
        
        enabled = loader.get_enabled()
        assert len(enabled) == 1
        assert "enabled" in enabled
    
    def test_invalid_id_rejected(self, tmp_path):
        """Should reject sync config with invalid ID."""
        syncs_dir = tmp_path / "syncs-config"
        syncs_dir.mkdir()
        
        (syncs_dir / "invalid.yaml").write_text(yaml.dump({
            "id": "Invalid Sync",  # uppercase and space
            "source": "src",
            "pusher": "push"
        }))
        
        loader = SyncsConfigLoader(syncs_dir)
        syncs = loader.scan()
        
        # Should not load invalid config
        assert len(syncs) == 0
    
    def test_default_intervals(self, tmp_path):
        """Should use default values for optional fields."""
        syncs_dir = tmp_path / "syncs-config"
        syncs_dir.mkdir()
        
        (syncs_dir / "minimal.yaml").write_text(yaml.dump({
            "id": "minimal",
            "source": "src",
            "pusher": "push"
        }))
        
        loader = SyncsConfigLoader(syncs_dir)
        syncs = loader.scan()
        
        config = syncs["minimal"]
        assert config.audit_interval_sec == 600
        assert config.sentinel_interval_sec == 120
        assert config.heartbeat_interval_sec == 10
        assert config.disabled == False
