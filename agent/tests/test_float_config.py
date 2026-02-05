
import pytest
import yaml
from fustor_agent.config.pipelines import AgentPipelineConfig

def test_pipeline_config_accepts_floats():
    config_yaml = """
    id: "test_pipeline"
    source: "local-fs"
    sender: "fusion-cloud"
    audit_interval_sec: 0.5
    sentinel_interval_sec: 1.2
    heartbeat_interval_sec: 0.1
    """
    config_dict = yaml.safe_load(config_yaml)
    config = AgentPipelineConfig(**config_dict)
    
    assert isinstance(config.audit_interval_sec, float)
    assert config.audit_interval_sec == 0.5
    assert config.sentinel_interval_sec == 1.2
    assert config.heartbeat_interval_sec == 0.1
