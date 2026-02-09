# agent/tests/test_agent_config_loaders.py
"""
Tests for Agent's Unified Configuration Loader (AgentConfigLoader).
Covers Sources, Senders, and Pipes loading in a shared namespace.
"""
import pytest
from pathlib import Path
import yaml

from fustor_agent.config.unified import AgentConfigLoader

class TestAgentConfigLoader:
    """Tests for AgentConfigLoader."""
    
    @pytest.fixture
    def config_dir(self, tmp_path):
        d = tmp_path / "agent-config"
        d.mkdir()
        return d
    
    def test_load_all_components(self, config_dir):
        """Should load sources, senders, and pipes from unified config."""
        (config_dir / "default.yaml").write_text(yaml.dump({
            "sources": {
                "s1": {
                    "driver": "fs", 
                    "paths": ["/tmp"],
                    "uri": "/tmp",
                    "credential": {"user": "u", "passwd": "p"}
                }
            },
            "senders": {
                "d1": {
                    "driver": "fusion", 
                    "uri": "http://localhost:8102",
                    "credential": {"key": "k"}
                }
            },
            "pipes": {
                "p1": {"source": "s1", "sender": "d1"}
            }
        }))
        
        loader = AgentConfigLoader(config_dir)
        loader.load_all()
        
        assert loader.get_source("s1").driver == "fs"
        assert loader.get_sender("d1").driver == "fusion"
        assert loader.get_pipe("p1").source == "s1"
    
    def test_cross_file_references(self, config_dir):
        """Should support referencing components across files."""
        (config_dir / "sources.yaml").write_text(yaml.dump({
            "sources": {
                "s1": {
                    "driver": "fs", 
                    "paths": ["/tmp"],
                    "uri": "/tmp",
                    "credential": {"user": "u", "passwd": "p"}
                }
            }
        }))
        
        (config_dir / "pipes.yaml").write_text(yaml.dump({
            "pipes": {
                "p1": {"source": "s1", "sender": "d1"}
            },
            "senders": {
                "d1": {
                    "driver": "fusion", 
                    "uri": "http://localhost:8102",
                    "credential": {"key": "k"}
                }
            }
        }))
        
        loader = AgentConfigLoader(config_dir)
        loader.load_all()
        
        pipe = loader.get_pipe("p1")
        assert pipe is not None
        assert pipe.source == "s1"
        assert loader.get_source(pipe.source) is not None

    def test_resolve_pipe_refs(self, config_dir):
        """Should resolve pipe references to source/sender objects."""
        (config_dir / "default.yaml").write_text(yaml.dump({
            "sources": {"s": {"driver": "fs", "paths": [], "uri": "/t", "credential": {"user": "u", "passwd": "p"}}},
            "senders": {"d": {"driver": "noop", "uri": "", "credential": {"key": "k"}}},
            "pipes": {"p": {"source": "s", "sender": "d"}}
        }))
        
        loader = AgentConfigLoader(config_dir)
        loader.load_all()
        
        resolved = loader.resolve_pipe_refs("p")
        assert resolved is not None
        assert resolved["source"].driver == "fs"
        assert resolved["sender"].driver == "noop"
        
    def test_get_pipes_from_file(self, config_dir):
        """Should return pipes defined in a specific file."""
        (config_dir / "file1.yaml").write_text(yaml.dump({
            "pipes": {"p1": {"source": "s", "sender": "d"}}
        }))
        (config_dir / "file2.yaml").write_text(yaml.dump({
            "pipes": {"p2": {"source": "s", "sender": "d"}}
        }))
        
        loader = AgentConfigLoader(config_dir)
        loader.load_all()
        
        pipes1 = loader.get_pipes_from_file("file1.yaml")
        assert "p1" in pipes1
        assert "p2" not in pipes1
        
        pipes2 = loader.get_pipes_from_file("file2.yaml")
        assert "p2" in pipes2

