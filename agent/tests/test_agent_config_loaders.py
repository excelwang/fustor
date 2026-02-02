# agent/tests/test_agent_config_loaders.py
"""
Tests for Agent's YAML Source and Sender configuration loaders.
"""
import pytest
import tempfile
from pathlib import Path
import yaml

from fustor_agent.config.sources import SourcesConfigLoader
from fustor_agent.config.senders import SendersConfigLoader as SendersConfigLoader


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


class TestSendersConfigLoader:
    """Tests for SendersConfigLoader."""
    
    def test_load_valid_config(self, tmp_path):
        """Should load valid sender config with both flat and nested structures."""
        config_file = tmp_path / "senders-config.yaml"
        config_file.write_text(yaml.dump({
            "sender-1": {
                "driver": "fusion",
                "uri": "http://1",
                "credential": {"key": "k"}
            },
            "sender-2": {
                "driver": "fusion",
                "uri": "http://2",
                "credential": {"key": "k"}
            }
        }))
        
        loader = SendersConfigLoader(config_file)
        loader.load()
        
        p1 = loader.get("sender-1")
        assert p1 is not None
        assert p1.uri == "http://1"
        
        p2 = loader.get("sender-2")
        assert p2 is not None
        assert p2.uri == "http://2"
    
    def test_invalid_id_rejected(self, tmp_path):
        """Should reject sender config with simple invalid ID."""
        config_file = tmp_path / "senders-config.yaml"
        config_file.write_text(yaml.dump({
            "Invalid ID": {
                "driver": "fusion",
                "uri": "http://1",
                "credential": {"key": "k"}
            }
        }))
        
        loader = SendersConfigLoader(config_file)
        loader.load()
        
        assert len(loader.get_all()) == 0
