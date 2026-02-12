"""
Test AgentConfigLoader.reload() correctly re-reads config from disk.
"""
import pytest
import yaml
from pathlib import Path
from fustor_agent.config.unified import AgentConfigLoader


@pytest.fixture
def config_dir(tmp_path):
    config_dir = tmp_path / "config"
    config_dir.mkdir()
    return config_dir


def _write_config(config_dir: Path, filename: str, content: dict):
    with open(config_dir / filename, 'w') as f:
        yaml.dump(content, f)


class TestConfigReload:

    def test_reload_picks_up_new_pipe(self, config_dir):
        """After reload(), newly added pipe YAML should be visible."""
        _write_config(config_dir, "default.yaml", {
            "sources": {"src-a": {"driver": "fs", "uri": "/data"}},
            "senders": {"sender-a": {"driver": "http", "uri": "http://localhost"}},
            "pipes": {"pipe-a": {"source": "src-a", "sender": "sender-a"}}
        })
        loader = AgentConfigLoader(config_dir)
        loader.load_all()

        assert "pipe-a" in loader.get_enabled_pipes()
        assert "pipe-b" not in loader.get_enabled_pipes()

        # Add a second pipe file
        _write_config(config_dir, "pipe-b.yaml", {
            "pipes": {"pipe-b": {"source": "src-a", "sender": "sender-a"}}
        })
        loader.reload()

        assert "pipe-a" in loader.get_enabled_pipes()
        assert "pipe-b" in loader.get_enabled_pipes()

    def test_reload_picks_up_removed_pipe(self, config_dir):
        """After reload(), deleted pipe YAML should no longer be visible."""
        _write_config(config_dir, "default.yaml", {
            "sources": {"src-a": {"driver": "fs", "uri": "/data"}},
            "senders": {"sender-a": {"driver": "http", "uri": "http://localhost"}},
            "pipes": {"pipe-a": {"source": "src-a", "sender": "sender-a"}}
        })
        _write_config(config_dir, "pipe-b.yaml", {
            "pipes": {"pipe-b": {"source": "src-a", "sender": "sender-a"}}
        })
        loader = AgentConfigLoader(config_dir)
        loader.load_all()

        assert "pipe-b" in loader.get_enabled_pipes()

        # Remove the pipe-b file
        (config_dir / "pipe-b.yaml").unlink()
        loader.reload()

        assert "pipe-a" in loader.get_enabled_pipes()
        assert "pipe-b" not in loader.get_enabled_pipes()

    def test_reload_clears_old_state(self, config_dir):
        """Reload should clear all previous config state before re-loading."""
        _write_config(config_dir, "default.yaml", {
            "sources": {
                "src-a": {"driver": "fs", "uri": "/data"},
                "src-b": {"driver": "fs", "uri": "/data2"},
            },
            "senders": {"sender-a": {"driver": "http", "uri": "http://localhost"}},
            "pipes": {
                "pipe-a": {"source": "src-a", "sender": "sender-a"},
                "pipe-b": {"source": "src-b", "sender": "sender-a"},
            }
        })
        loader = AgentConfigLoader(config_dir)
        loader.load_all()
        assert len(loader.get_enabled_pipes()) == 2

        # Rewrite with only one pipe
        _write_config(config_dir, "default.yaml", {
            "sources": {"src-a": {"driver": "fs", "uri": "/data"}},
            "senders": {"sender-a": {"driver": "http", "uri": "http://localhost"}},
            "pipes": {"pipe-a": {"source": "src-a", "sender": "sender-a"}}
        })
        loader.reload()
        
        enabled = loader.get_enabled_pipes()
        assert len(enabled) == 1
        assert "pipe-a" in enabled
        assert "pipe-b" not in enabled
