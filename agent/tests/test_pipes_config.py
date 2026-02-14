# agent/tests/test_pipes_config.py
"""
Tests for pipe-specific functionality in Agent's Unified Configuration Loader.
Legacy pipes config tests adapted for unified loader.
"""
import pytest
import yaml
from pathlib import Path

from fustor_agent.config.unified import AgentConfigLoader

class TestUnifiedPipesConfig:
    
    @pytest.fixture
    def config_dir(self, tmp_path):
        d = tmp_path / "agent-config"
        d.mkdir()
        return d

    def test_default_intervals(self, config_dir):
        """Should use default values for optional fields."""
        (config_dir / "minimal.yaml").write_text(yaml.dump({
            "pipes": {
                "minimal": {
                    "source": "src",
                    "sender": "push"
                }
            }
        }))
        
        loader = AgentConfigLoader(config_dir)
        loader.load_all()
        
        config = loader.get_pipe("minimal")
        assert config.audit_interval_sec == 43200.0
        assert config.sentinel_interval_sec == 300.0
        assert config.disabled == False

    def test_get_enabled_pipes(self, config_dir):
        """Should return only enabled pipes."""
        (config_dir / "pipes.yaml").write_text(yaml.dump({
            "sources": {"s": {"driver": "src", "uri": "file:///tmp"}},
            "senders": {"d": {"driver": "dst", "uri": "http://localhost"}},
            "pipes": {
                "enabled": {"source": "s", "sender": "d"},
                "disabled": {"source": "s", "sender": "d", "disabled": True}
            }
        }))
        
        loader = AgentConfigLoader(config_dir)
        loader.load_all()
        
        enabled = loader.get_enabled_pipes()
        assert "enabled" in enabled
        assert "disabled" not in enabled
