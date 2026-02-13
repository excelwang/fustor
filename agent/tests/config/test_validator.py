import pytest
from unittest.mock import MagicMock
from fustor_agent.config.validator import ConfigValidator
from fustor_agent.config.unified import AgentConfigLoader, SourceConfig, SenderConfig, AgentPipeConfig # Assuming these models are used internally

@pytest.fixture
def mock_loader():
    """Fixture for a mocked AgentConfigLoader."""
    loader = MagicMock(spec=AgentConfigLoader)
    loader.reload.return_value = None # Assume reload always works for now
    
    # Default valid state
    loader.agent_id = "test-agent-id"
    loader.get_all_sources.return_value = {
        "s1": SourceConfig(driver="fs", uri="/data")
    }
    loader.get_all_senders.return_value = {
        "se1": SenderConfig(driver="echo", uri="http://localhost")
    }
    loader.get_all_pipes.return_value = {
        "p1": AgentPipeConfig(source="s1", sender="se1", id="p1")
    }
    return loader

def test_validate_happy_path(mock_loader):
    """Test a valid configuration returns no errors."""
    validator = ConfigValidator(loader=mock_loader)
    is_valid, errors = validator.validate()
    assert is_valid is True
    assert errors == []

def test_validate_missing_agent_id(mock_loader):
    """Test validation fails if agent_id is missing."""
    mock_loader.agent_id = None
    validator = ConfigValidator(loader=mock_loader)
    is_valid, errors = validator.validate()
    assert is_valid is False
    assert "Global 'agent_id' is missing" in errors[0]

def test_validate_source_missing_driver(mock_loader):
    """Test validation fails if a source is missing its driver."""
    mock_loader.get_all_sources.return_value["s1"].driver = None
    validator = ConfigValidator(loader=mock_loader)
    is_valid, errors = validator.validate()
    assert is_valid is False
    assert "Source 's1' missing 'driver' field" in errors[0]

def test_validate_source_missing_uri(mock_loader):
    """Test validation fails if a source is missing its URI."""
    mock_loader.get_all_sources.return_value["s1"].uri = None
    validator = ConfigValidator(loader=mock_loader)
    is_valid, errors = validator.validate()
    assert is_valid is False
    assert "Source 's1' missing 'uri' field" in errors[0]

def test_validate_sender_missing_driver(mock_loader):
    """Test validation fails if a sender is missing its driver."""
    mock_loader.get_all_senders.return_value["se1"].driver = None
    validator = ConfigValidator(loader=mock_loader)
    is_valid, errors = validator.validate()
    assert is_valid is False
    assert "Sender 'se1' missing 'driver' field" in errors[0]

def test_validate_sender_missing_uri(mock_loader):
    """Test validation fails if a sender is missing its URI."""
    mock_loader.get_all_senders.return_value["se1"].uri = None
    validator = ConfigValidator(loader=mock_loader)
    is_valid, errors = validator.validate()
    assert is_valid is False
    assert "Sender 'se1' missing 'uri' field" in errors[0]

def test_validate_pipe_missing_source(mock_loader):
    """Test validation fails if a pipe is missing its source reference."""
    mock_loader.get_all_pipes.return_value["p1"].source = None
    validator = ConfigValidator(loader=mock_loader)
    is_valid, errors = validator.validate()
    assert is_valid is False
    assert "Pipe 'p1' missing 'source' reference" in errors[0]

def test_validate_pipe_unknown_source(mock_loader):
    """Test validation fails if a pipe references an unknown source."""
    mock_loader.get_all_pipes.return_value["p1"].source = "unknown_s"
    validator = ConfigValidator(loader=mock_loader)
    is_valid, errors = validator.validate()
    assert is_valid is False
    assert "Pipe 'p1' references unknown source 'unknown_s'" in errors[0]

def test_validate_pipe_missing_sender(mock_loader):
    """Test validation fails if a pipe is missing its sender reference."""
    mock_loader.get_all_pipes.return_value["p1"].sender = None
    validator = ConfigValidator(loader=mock_loader)
    is_valid, errors = validator.validate()
    assert is_valid is False
    assert "Pipe 'p1' missing 'sender' reference" in errors[0]

def test_validate_pipe_unknown_sender(mock_loader):
    """Test validation fails if a pipe references an unknown sender."""
    mock_loader.get_all_pipes.return_value["p1"].sender = "unknown_se"
    validator = ConfigValidator(loader=mock_loader)
    is_valid, errors = validator.validate()
    assert is_valid is False
    assert "Pipe 'p1' references unknown sender 'unknown_se'" in errors[0]

def test_validate_pipe_redundant_pair(mock_loader):
    """Test validation fails if two pipes use the same source-sender pair."""
    mock_loader.get_all_pipes.return_value["p2"] = AgentPipeConfig(source="s1", sender="se1", id="p2")
    validator = ConfigValidator(loader=mock_loader)
    is_valid, errors = validator.validate()
    assert is_valid is False
    assert "Redundant configuration: Pipe 'p2' uses the same (source, sender) pair as Pipe 'p1'" in errors[0]

def test_validate_config_dict_happy_path():
    """Test validate_config with a valid dictionary config."""
    config_dict = {
        "pipes": {
            "p1": {"source": "s1", "sender": "se1"}
        }
    }
    validator = ConfigValidator()
    is_valid, errors = validator.validate_config(config_dict)
    assert is_valid is True
    assert errors == []

def test_validate_config_dict_redundant_pair():
    """Test validate_config with a redundant (source, sender) pair."""
    config_dict = {
        "pipes": {
            "p1": {"source": "s1", "sender": "se1"},
            "p2": {"source": "s1", "sender": "se1"}
        }
    }
    validator = ConfigValidator()
    is_valid, errors = validator.validate_config(config_dict)
    assert is_valid is False
    assert "Redundant configuration: Pipe 'p2' uses the same (source, sender) pair as Pipe 'p1'." in errors[0]

def test_validate_config_dict_empty_pipes():
    """Test validate_config with an empty pipes dictionary."""
    config_dict = {
        "pipes": {}
    }
    validator = ConfigValidator()
    is_valid, errors = validator.validate_config(config_dict)
    assert is_valid is True
    assert errors == []

def test_validate_config_dict_empty_config():
    """Test validate_config with an empty config dictionary."""
    config_dict = {}
    validator = ConfigValidator()
    is_valid, errors = validator.validate_config(config_dict)
    assert is_valid is True
    assert errors == []

def test_validate_config_dict_pipes_not_dict():
    """Test validate_config fails if 'pipes' is not a dictionary."""
    config_dict = {
        "pipes": []
    }
    validator = ConfigValidator()
    is_valid, errors = validator.validate_config(config_dict)
    assert is_valid is False
    assert "'pipes' section must be a dictionary" in errors[0]

def test_validate_config_dict_pipe_missing_source_sender():
    """Test validate_config handles pipes missing source or sender."""
    config_dict = {
        "pipes": {
            "p1": {"sender": "se1"}, # Missing source
            "p2": {"source": "s1"}   # Missing sender
        }
    }
    validator = ConfigValidator()
    is_valid, errors = validator.validate_config(config_dict)
    # The validate_config_dict only checks for redundancy if both source and sender are present
    assert is_valid is True 
    assert errors == []

def test_validate_loader_reload_failure(mock_loader):
    """Test that validation correctly handles exceptions during loader reload."""
    mock_loader.reload.side_effect = Exception("Mock reload error")
    validator = ConfigValidator(loader=mock_loader)
    is_valid, errors = validator.validate()
    assert is_valid is False
    assert "Failed to load configuration files: Mock reload error" in errors[0]

def test_validate_empty_config_dir(mock_loader):
    """Test validation when config directory is empty (no sources, senders, pipes)."""
    mock_loader.get_all_sources.return_value = {}
    mock_loader.get_all_senders.return_value = {}
    mock_loader.get_all_pipes.return_value = {}
    mock_loader.agent_id = "test-agent-id" # Still needs agent_id, otherwise that error takes precedence
    
    validator = ConfigValidator(loader=mock_loader)
    is_valid, errors = validator.validate()
    assert is_valid is True
    assert errors == []
