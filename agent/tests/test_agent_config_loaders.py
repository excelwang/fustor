# agent/tests/test_agent_config_loaders.py
"""
Tests for Agent's YAML Source and Pusher configuration loaders.
"""
import pytest
import tempfile
from pathlib import Path
import yaml

from fustor_agent.config.sources import SourcesConfigLoader
from fustor_agent.config.senders import SendersConfigLoader as PushersConfigLoader


class TestSourcesConfigLoader:
    """Tests for SourcesConfigLoader."""
    
    def test_load_valid_config(self, tmp_path):
        """Should load valid source config with both flat and nested structures."""
        config_file = tmp_path / "sources-config.yaml"
        config_file.write_text(yaml.dump({
            "source-1": {
                "driver": "fs",
                "uri": "/tmp/1",
                "credential": {"user": "u", "passwd": "p"}
            },
            "source-2": {
                "driver": "s3",
                "uri": "s3://bucket",
                "credential": {"key": "k", "user": "u"}
            }
        }))
        
        loader = SourcesConfigLoader(config_file)
        loader.load()
        
        s1 = loader.get("source-1")
        assert s1 is not None
        assert s1.driver == "fs"
        
        s2 = loader.get("source-2")
        assert s2 is not None
        assert s2.driver == "s3"
    
    def test_invalid_id_rejected(self, tmp_path):
        """Should reject source config with URL-unsafe ID."""
        config_file = tmp_path / "sources-config.yaml"
        config_file.write_text(yaml.dump({
            "Invalid ID": {
                "driver": "fs",
                "uri": "/tmp",
                "credential": {"user": "u", "passwd": "p"}
            }
        }))
        
        loader = SourcesConfigLoader(config_file)
        loader.load()
        
        assert len(loader.get_all()) == 0

    def test_reload(self, tmp_path):
        """Should reload configuration."""
        config_file = tmp_path / "sources-config.yaml"
        config_file.write_text(yaml.dump({
            "s1": {
                "driver": "fs",
                "uri": "/v1",
                "credential": {"user": "u", "passwd": "p"}
            }
        }))
        
        loader = SourcesConfigLoader(config_file)
        loader.load()
        assert loader.get("s1").uri == "/v1"
        
        config_file.write_text(yaml.dump({
            "s1": {
                "driver": "fs",
                "uri": "/v2",
                "credential": {"user": "u", "passwd": "p"}
            }
        }))
        loader.reload()
        assert loader.get("s1").uri == "/v2"


class TestPushersConfigLoader:
    """Tests for PushersConfigLoader."""
    
    def test_load_valid_config(self, tmp_path):
        """Should load valid pusher config with both flat and nested structures."""
        config_file = tmp_path / "pushers-config.yaml"
        config_file.write_text(yaml.dump({
            "pusher-1": {
                "driver": "fusion",
                "endpoint": "http://1",
                "credential": {"key": "k"}
            },
            "pusher-2": {
                "driver": "fusion",
                "endpoint": "http://2",
                "credential": {"key": "k"}
            }
        }))
        
        loader = PushersConfigLoader(config_file)
        loader.load()
        
        p1 = loader.get("pusher-1")
        assert p1 is not None
        assert p1.endpoint == "http://1"
        
        p2 = loader.get("pusher-2")
        assert p2 is not None
        assert p2.endpoint == "http://2"
    
    def test_invalid_id_rejected(self, tmp_path):
        """Should reject pusher config with simple invalid ID."""
        config_file = tmp_path / "pushers-config.yaml"
        config_file.write_text(yaml.dump({
            "Invalid ID": {
                "driver": "fusion",
                "endpoint": "http://1",
                "credential": {"key": "k"}
            }
        }))
        
        loader = PushersConfigLoader(config_file)
        loader.load()
        
        assert len(loader.get_all()) == 0
