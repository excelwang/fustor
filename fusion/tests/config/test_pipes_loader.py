import pytest
import yaml
from pathlib import Path
from fustor_fusion.config.pipes import PipesConfigLoader, FusionPipeConfig

@pytest.fixture
def config_dir(tmp_path):
    d = tmp_path / "config"
    d.mkdir()
    return d

def test_loader_empty_dir(config_dir):
    loader = PipesConfigLoader(config_dir)
    pipes = loader.scan()
    assert pipes == {}
    assert loader.get_default_list() == []

def test_loader_load_single_pipe(config_dir):
    pipe_data = {
        "id": "test-pipe",
        "receiver": {
            "driver": "http",
            "port": 9000,
            "api_keys": [{"key": "abc", "pipe_id": "p1"}]
        },
        "views": ["v1", "v2"],
        "enabled": True
    }
    with open(config_dir / "test.yaml", "w") as f:
        yaml.dump(pipe_data, f)
        
    loader = PipesConfigLoader(config_dir)
    pipes = loader.scan()
    assert "test-pipe" in pipes
    assert pipes["test-pipe"].receiver.port == 9000
    assert pipes["test-pipe"].views == ["v1", "v2"]

def test_loader_default_list(config_dir):
    with open(config_dir / "default.yaml", "w") as f:
        yaml.dump({"pipes": ["p1", "p2"]}, f)
        
    loader = PipesConfigLoader(config_dir)
    loader.scan()
    assert loader.get_default_list() == ["p1", "p2"]

def test_loader_invalid_id(config_dir):
    pipe_data = {
        "id": "Invalid ID!!",
        "receiver": {"driver": "http", "port": 9000}
    }
    with open(config_dir / "bad.yaml", "w") as f:
        yaml.dump(pipe_data, f)
        
    loader = PipesConfigLoader(config_dir)
    # Scan shouldn't crash, it should log and skip
    pipes = loader.scan()
    assert len(pipes) == 0

def test_loader_get_enabled(config_dir):
    p1 = {"id": "p1", "enabled": True, "receiver": {"driver": "h"}}
    p2 = {"id": "p2", "enabled": False, "receiver": {"driver": "h"}}
    
    with open(config_dir / "p1.yaml", "w") as f: yaml.dump(p1, f)
    with open(config_dir / "p2.yaml", "w") as f: yaml.dump(p2, f)
    
    loader = PipesConfigLoader(config_dir)
    enabled = loader.get_enabled()
    assert "p1" in enabled
    assert "p2" not in enabled

def test_load_from_path(config_dir):
    pipe_data = {"id": "manual", "receiver": {"driver": "http"}}
    path = config_dir / "manual.yaml"
    with open(path, "w") as f: yaml.dump(pipe_data, f)
    
    loader = PipesConfigLoader(config_dir)
    config = loader.load_from_path(path)
    assert config.id == "manual"
