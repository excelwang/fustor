"""
Test AgentConfigLoader.get_diff() logic.
Verifies that the diff algorithm correctly identifies added and removed pipes.
"""
import pytest
import yaml
from pathlib import Path
from fustor_agent.config.unified import AgentConfigLoader


@pytest.fixture
def config_dir(tmp_path):
    """Create a config directory with a base configuration."""
    config_dir = tmp_path / "config"
    config_dir.mkdir()
    return config_dir


def _write_config(config_dir: Path, filename: str, content: dict):
    with open(config_dir / filename, 'w') as f:
        yaml.dump(content, f)


class TestConfigDiff:
    
    def test_no_changes(self, config_dir):
        """When running pipes match enabled pipes, diff should be empty."""
        _write_config(config_dir, "default.yaml", {
            "sources": {"src-a": {"driver": "fs", "uri": "/data"}},
            "senders": {"sender-a": {"driver": "http", "uri": "http://localhost"}},
            "pipes": {"pipe-a": {"source": "src-a", "sender": "sender-a"}}
        })
        loader = AgentConfigLoader(config_dir)
        loader.load_all()

        diff = loader.get_diff({"pipe-a"})
        assert diff["added"] == set()
        assert diff["removed"] == set()

    def test_pipe_added(self, config_dir):
        """New pipe in config should appear in 'added'."""
        _write_config(config_dir, "default.yaml", {
            "sources": {"src-a": {"driver": "fs", "uri": "/data"}},
            "senders": {"sender-a": {"driver": "http", "uri": "http://localhost"}},
            "pipes": {
                "pipe-a": {"source": "src-a", "sender": "sender-a"},
                "pipe-b": {"source": "src-a", "sender": "sender-a"},
            }
        })
        loader = AgentConfigLoader(config_dir)
        loader.load_all()

        diff = loader.get_diff({"pipe-a"})  # Only pipe-a was running
        assert diff["added"] == {"pipe-b"}
        assert diff["removed"] == set()

    def test_pipe_removed(self, config_dir):
        """Pipe removed from config should appear in 'removed'."""
        _write_config(config_dir, "default.yaml", {
            "sources": {"src-a": {"driver": "fs", "uri": "/data"}},
            "senders": {"sender-a": {"driver": "http", "uri": "http://localhost"}},
            "pipes": {"pipe-a": {"source": "src-a", "sender": "sender-a"}}
        })
        loader = AgentConfigLoader(config_dir)
        loader.load_all()

        # pipe-b was running but is no longer in config
        diff = loader.get_diff({"pipe-a", "pipe-b"})
        assert diff["added"] == set()
        assert diff["removed"] == {"pipe-b"}

    def test_pipe_disabled_counts_as_removed(self, config_dir):
        """Disabling a pipe should make it appear in 'removed'."""
        _write_config(config_dir, "default.yaml", {
            "sources": {"src-a": {"driver": "fs", "uri": "/data"}},
            "senders": {"sender-a": {"driver": "http", "uri": "http://localhost"}},
            "pipes": {
                "pipe-a": {"source": "src-a", "sender": "sender-a"},
                "pipe-b": {"source": "src-a", "sender": "sender-a", "disabled": True},
            }
        })
        loader = AgentConfigLoader(config_dir)
        loader.load_all()

        diff = loader.get_diff({"pipe-a", "pipe-b"})
        assert diff["added"] == set()
        assert diff["removed"] == {"pipe-b"}

    def test_source_disabled_removes_pipe(self, config_dir):
        """Disabling a source should remove all its pipes."""
        _write_config(config_dir, "default.yaml", {
            "sources": {"src-a": {"driver": "fs", "uri": "/data", "disabled": True}},
            "senders": {"sender-a": {"driver": "http", "uri": "http://localhost"}},
            "pipes": {"pipe-a": {"source": "src-a", "sender": "sender-a"}}
        })
        loader = AgentConfigLoader(config_dir)
        loader.load_all()

        diff = loader.get_diff({"pipe-a"})
        assert diff["added"] == set()
        assert diff["removed"] == {"pipe-a"}

    def test_modified_pipe_not_detected(self, config_dir):
        """Modifying config of existing pipe ID is NOT detected by diff (known limitation)."""
        _write_config(config_dir, "default.yaml", {
            "sources": {"src-a": {"driver": "fs", "uri": "/data"}},
            "senders": {"sender-a": {"driver": "http", "uri": "http://localhost"}},
            "pipes": {"pipe-a": {"source": "src-a", "sender": "sender-a", 
                                  "fields_mapping": [{"to": "path", "source": ["path:string"]}]}}
        })
        loader = AgentConfigLoader(config_dir)
        loader.load_all()

        diff = loader.get_diff({"pipe-a"})
        # Same ID = no diff detected
        assert diff["added"] == set()
        assert diff["removed"] == set()

    def test_empty_running_set(self, config_dir):
        """All enabled pipes should be 'added' when nothing is running."""
        _write_config(config_dir, "default.yaml", {
            "sources": {"src-a": {"driver": "fs", "uri": "/data"}},
            "senders": {"sender-a": {"driver": "http", "uri": "http://localhost"}},
            "pipes": {
                "pipe-a": {"source": "src-a", "sender": "sender-a"},
                "pipe-b": {"source": "src-a", "sender": "sender-a"},
            }
        })
        loader = AgentConfigLoader(config_dir)
        loader.load_all()

        diff = loader.get_diff(set())
        assert diff["added"] == {"pipe-a", "pipe-b"}
        assert diff["removed"] == set()
