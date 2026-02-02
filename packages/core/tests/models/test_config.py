import pytest
from fustor_core.models.config import (
    PasswdCredential, ApiKeyCredential, FieldMapping,
    SourceConfig, SenderConfig, PipelineConfig,
    AppConfig, ConfigError, NotFoundError
)

def test_passwd_credential_hash_and_eq():
    cred1 = PasswdCredential(user="testuser", passwd="testpass")
    cred2 = PasswdCredential(user="testuser", passwd="testpass")
    cred3 = PasswdCredential(user="anotheruser", passwd="testpass")

    assert cred1 == cred2
    assert hash(cred1) == hash(cred2)
    assert cred1 != cred3
    assert hash(cred1) != hash(cred3)

def test_api_key_credential_hash_and_eq():
    cred1 = ApiKeyCredential(user="testuser", key="apikey123")
    cred2 = ApiKeyCredential(user="testuser", key="apikey123")
    cred3 = ApiKeyCredential(user="anotheruser", key="anotherkey")

    assert cred1 == cred2
    assert hash(cred1) == hash(cred2)
    assert cred1 != cred3
    assert hash(cred1) != hash(cred3)

def test_sender_config_batch_size_validation():
    from pydantic import ValidationError
    with pytest.raises(ValidationError):
        SenderConfig(driver="test", uri="http://localhost", credential=PasswdCredential(user="u"), batch_size=0)
    with pytest.raises(ValidationError):
        SenderConfig(driver="test", uri="http://localhost", credential=PasswdCredential(user="u"), batch_size=-1)
    
    config = SenderConfig(driver="test", uri="http://localhost", credential=PasswdCredential(user="u"), batch_size=1)
    assert config.batch_size == 1

def test_app_config_add_get_delete_source():
    app_config = AppConfig()
    source_config = SourceConfig(driver="mysql", uri="mysql://host", credential=PasswdCredential(user="u"), disabled=False)

    # Add source
    app_config.add_source("my_source", source_config)
    assert app_config.get_source("my_source") == source_config

    # Add duplicate source
    with pytest.raises(ConfigError, match="Source config with name 'my_source' already exists."):
        app_config.add_source("my_source", source_config)

    # Delete source
    deleted_source = app_config.delete_source("my_source")
    assert deleted_source == source_config
    assert app_config.get_source("my_source") is None

    # Delete non-existent source
    with pytest.raises(NotFoundError, match="Source config with id 'non_existent' not found."):
        app_config.delete_source("non_existent")

def test_app_config_add_get_delete_sender():
    app_config = AppConfig()
    sender_config = SenderConfig(driver="http", uri="http://localhost", credential=PasswdCredential(user="u"), disabled=False)

    # Add pusher
    app_config.add_sender("my_sender", sender_config)
    assert app_config.get_sender("my_sender") == sender_config

    # Add duplicate pusher
    with pytest.raises(ConfigError, match="Sender config with name 'my_sender' already exists."):
        app_config.add_sender("my_sender", sender_config)

    # Delete sender
    deleted_sender = app_config.delete_sender("my_sender")
    assert deleted_sender == sender_config
    assert app_config.get_sender("my_sender") is None

    # Delete non-existent sender
    with pytest.raises(NotFoundError, match="Sender config with id 'non_existent' not found."):
        app_config.delete_sender("non_existent")

def test_app_config_add_get_delete_pipeline():
    app_config = AppConfig()
    source_config = SourceConfig(driver="mysql", uri="mysql://host", credential=PasswdCredential(user="u"), disabled=False)
    sender_config = SenderConfig(driver="http", uri="http://localhost", credential=PasswdCredential(user="u"), disabled=False)
    pipeline_config = PipelineConfig(source="my_source", sender="my_sender", disabled=False)

    # Add pipeline without dependencies
    with pytest.raises(NotFoundError, match="Dependency source 'my_source' not found."):
        app_config.add_pipeline("my_pipeline", pipeline_config)
    
    app_config.add_source("my_source", source_config)
    with pytest.raises(NotFoundError, match="Dependency sender 'my_sender' not found."):
        app_config.add_pipeline("my_pipeline", pipeline_config)

    app_config.add_sender("my_sender", sender_config)
    app_config.add_pipeline("my_pipeline", pipeline_config)
    assert app_config.get_pipeline("my_pipeline") == pipeline_config

    # Add duplicate pipeline
    with pytest.raises(ConfigError, match="Pipeline config with id 'my_pipeline' already exists."):
        app_config.add_pipeline("my_pipeline", pipeline_config)

    # Delete pipeline
    deleted_pipeline = app_config.delete_pipeline("my_pipeline")
    assert deleted_pipeline == pipeline_config
    assert app_config.get_pipeline("my_pipeline") is None

    # Delete non-existent pipeline
    with pytest.raises(NotFoundError, match="Pipeline config with id 'non_existent' not found."):
        app_config.delete_pipeline("non_existent")

def test_app_config_delete_source_with_dependent_pipelines():
    app_config = AppConfig()
    source_config = SourceConfig(driver="mysql", uri="mysql://host", credential=PasswdCredential(user="u"), disabled=False)
    sender_config = SenderConfig(driver="http", uri="http://localhost", credential=PasswdCredential(user="u"), disabled=False)
    pipeline_config1 = PipelineConfig(source="my_source", sender="my_sender", disabled=False)
    pipeline_config2 = PipelineConfig(source="my_source", sender="my_sender", disabled=False) # Another pipeline using the same source

    app_config.add_source("my_source", source_config)
    app_config.add_sender("my_sender", sender_config)
    app_config.add_pipeline("pipeline1", pipeline_config1)
    app_config.add_pipeline("pipeline2", pipeline_config2)

    assert app_config.get_pipeline("pipeline1") is not None
    assert app_config.get_pipeline("pipeline2") is not None

    app_config.delete_source("my_source")

    assert app_config.get_source("my_source") is None
    assert app_config.get_pipeline("pipeline1") is None
    assert app_config.get_pipeline("pipeline2") is None

def test_app_config_delete_sender_with_dependent_pipelines():
    app_config = AppConfig()
    source_config = SourceConfig(driver="mysql", uri="mysql://host", credential=PasswdCredential(user="u"), disabled=False)
    sender_config = SenderConfig(driver="http", uri="http://localhost", credential=PasswdCredential(user="u"), disabled=False)
    pipeline_config1 = PipelineConfig(source="my_source", sender="my_sender", disabled=False)
    pipeline_config2 = PipelineConfig(source="my_source", sender="my_sender", disabled=False) # Another pipeline using the same sender

    app_config.add_source("my_source", source_config)
    app_config.add_sender("my_sender", sender_config)
    app_config.add_pipeline("pipeline1", pipeline_config1)
    app_config.add_pipeline("pipeline2", pipeline_config2)

    assert app_config.get_pipeline("pipeline1") is not None
    assert app_config.get_pipeline("pipeline2") is not None

    app_config.delete_sender("my_sender")

    assert app_config.get_sender("my_sender") is None
    assert app_config.get_pipeline("pipeline1") is None
    assert app_config.get_pipeline("pipeline2") is None

def test_app_config_check_pipeline_is_disabled():
    app_config = AppConfig()
    source_config_enabled = SourceConfig(driver="mysql", uri="mysql://host", credential=PasswdCredential(user="u"), disabled=False)
    source_config_disabled = SourceConfig(driver="mysql", uri="mysql://host", credential=PasswdCredential(user="u"), disabled=True)
    sender_config_enabled = SenderConfig(driver="http", uri="http://localhost", credential=PasswdCredential(user="u"), disabled=False)
    sender_config_disabled = SenderConfig(driver="http", uri="http://localhost", credential=PasswdCredential(user="u"), disabled=True)

    app_config.add_source("source_e", source_config_enabled)
    app_config.add_source("source_d", source_config_disabled)
    app_config.add_sender("sender_e", sender_config_enabled)
    app_config.add_sender("sender_d", sender_config_disabled)

    # Pipeline itself disabled
    pipeline_disabled = PipelineConfig(source="source_e", sender="sender_e", disabled=True)
    app_config.add_pipeline("pipeline_d", pipeline_disabled)
    assert app_config.check_pipeline_is_disabled("pipeline_d") is True

    # Source disabled
    pipeline_source_disabled = PipelineConfig(source="source_d", sender="sender_e", disabled=False)
    app_config.add_pipeline("pipeline_source_d", pipeline_source_disabled)
    assert app_config.check_pipeline_is_disabled("pipeline_source_d") is True

    # Sender disabled
    pipeline_sender_disabled = PipelineConfig(source="source_e", sender="sender_d", disabled=False)
    app_config.add_pipeline("pipeline_sender_d", pipeline_sender_disabled)
    assert app_config.check_pipeline_is_disabled("pipeline_sender_d") is True

    # All enabled
    pipeline_enabled = PipelineConfig(source="source_e", sender="sender_e", disabled=False)
    app_config.add_pipeline("pipeline_e", pipeline_enabled)
    assert app_config.check_pipeline_is_disabled("pipeline_e") is False

    # Non-existent pipeline
    with pytest.raises(NotFoundError, match="Pipeline with id 'non_existent' not found."):
        app_config.check_pipeline_is_disabled("non_existent")

    # Missing source dependency
    pipeline_missing_source = PipelineConfig(source="non_existent_source", sender="sender_e", disabled=False)
    with pytest.raises(NotFoundError, match="Dependency source 'non_existent_source' not found."):
        app_config.add_pipeline("pipeline_missing_source", pipeline_missing_source)

    # Missing sender dependency
    pipeline_missing_sender = PipelineConfig(source="source_e", sender="non_existent_sender", disabled=False)
    with pytest.raises(NotFoundError, match="Dependency sender 'non_existent_sender' not found."):
        app_config.add_pipeline("pipeline_missing_sender", pipeline_missing_sender)
