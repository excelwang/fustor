import pytest
from fustor_core.models.config import (
    PasswdCredential, ApiKeyCredential, FieldMapping,
    SourceConfig, SenderConfig, SyncConfig,
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

    # Delete pusher
    deleted_pusher = app_config.delete_sender("my_sender")
    assert deleted_pusher == sender_config
    assert app_config.get_sender("my_sender") is None

    # Delete non-existent pusher
    with pytest.raises(NotFoundError, match="Sender config with id 'non_existent' not found."):
        app_config.delete_sender("non_existent")

def test_app_config_add_get_delete_sync():
    app_config = AppConfig()
    source_config = SourceConfig(driver="mysql", uri="mysql://host", credential=PasswdCredential(user="u"), disabled=False)
    sender_config = SenderConfig(driver="http", uri="http://localhost", credential=PasswdCredential(user="u"), disabled=False)
    sync_config = SyncConfig(source="my_source", sender="my_sender", disabled=False)

    # Add sync without dependencies
    with pytest.raises(NotFoundError, match="Dependency source 'my_source' not found."):
        app_config.add_sync("my_sync", sync_config)
    
    app_config.add_source("my_source", source_config)
    with pytest.raises(NotFoundError, match="Dependency sender 'my_sender' not found."):
        app_config.add_sync("my_sync", sync_config)

    app_config.add_sender("my_sender", sender_config)
    app_config.add_sync("my_sync", sync_config)
    assert app_config.get_sync("my_sync") == sync_config

    # Add duplicate sync
    with pytest.raises(ConfigError, match="Sync config with id 'my_sync' already exists."):
        app_config.add_sync("my_sync", sync_config)

    # Delete sync
    deleted_sync = app_config.delete_sync("my_sync")
    assert deleted_sync == sync_config
    assert app_config.get_sync("my_sync") is None

    # Delete non-existent sync
    with pytest.raises(NotFoundError, match="Sync config with id 'non_existent' not found."):
        app_config.delete_sync("non_existent")

def test_app_config_delete_source_with_dependent_syncs():
    app_config = AppConfig()
    source_config = SourceConfig(driver="mysql", uri="mysql://host", credential=PasswdCredential(user="u"), disabled=False)
    sender_config = SenderConfig(driver="http", uri="http://localhost", credential=PasswdCredential(user="u"), disabled=False)
    sync_config1 = SyncConfig(source="my_source", sender="my_sender", disabled=False)
    sync_config2 = SyncConfig(source="my_source", sender="my_sender", disabled=False) # Another sync using the same source

    app_config.add_source("my_source", source_config)
    app_config.add_sender("my_sender", sender_config)
    app_config.add_sync("sync1", sync_config1)
    app_config.add_sync("sync2", sync_config2)

    assert app_config.get_sync("sync1") is not None
    assert app_config.get_sync("sync2") is not None

    app_config.delete_source("my_source")

    assert app_config.get_source("my_source") is None
    assert app_config.get_sync("sync1") is None
    assert app_config.get_sync("sync2") is None

def test_app_config_delete_sender_with_dependent_syncs():
    app_config = AppConfig()
    source_config = SourceConfig(driver="mysql", uri="mysql://host", credential=PasswdCredential(user="u"), disabled=False)
    sender_config = SenderConfig(driver="http", uri="http://localhost", credential=PasswdCredential(user="u"), disabled=False)
    sync_config1 = SyncConfig(source="my_source", sender="my_sender", disabled=False)
    sync_config2 = SyncConfig(source="my_source", sender="my_sender", disabled=False) # Another sync using the same pusher

    app_config.add_source("my_source", source_config)
    app_config.add_sender("my_sender", sender_config)
    app_config.add_sync("sync1", sync_config1)
    app_config.add_sync("sync2", sync_config2)

    assert app_config.get_sync("sync1") is not None
    assert app_config.get_sync("sync2") is not None

    app_config.delete_sender("my_sender")

    assert app_config.get_sender("my_sender") is None
    assert app_config.get_sync("sync1") is None
    assert app_config.get_sync("sync2") is None

def test_app_config_check_sync_is_disabled():
    app_config = AppConfig()
    source_config_enabled = SourceConfig(driver="mysql", uri="mysql://host", credential=PasswdCredential(user="u"), disabled=False)
    source_config_disabled = SourceConfig(driver="mysql", uri="mysql://host", credential=PasswdCredential(user="u"), disabled=True)
    sender_config_enabled = SenderConfig(driver="http", uri="http://localhost", credential=PasswdCredential(user="u"), disabled=False)
    sender_config_disabled = SenderConfig(driver="http", uri="http://localhost", credential=PasswdCredential(user="u"), disabled=True)

    app_config.add_source("source_e", source_config_enabled)
    app_config.add_source("source_d", source_config_disabled)
    app_config.add_sender("sender_e", sender_config_enabled)
    app_config.add_sender("sender_d", sender_config_disabled)

    # Sync itself disabled
    sync_disabled = SyncConfig(source="source_e", sender="sender_e", disabled=True)
    app_config.add_sync("sync_d", sync_disabled)
    assert app_config.check_sync_is_disabled("sync_d") is True

    # Source disabled
    sync_source_disabled = SyncConfig(source="source_d", sender="sender_e", disabled=False)
    app_config.add_sync("sync_source_d", sync_source_disabled)
    assert app_config.check_sync_is_disabled("sync_source_d") is True

    # Pusher disabled
    sync_sender_disabled = SyncConfig(source="source_e", sender="sender_d", disabled=False)
    app_config.add_sync("sync_sender_d", sync_sender_disabled)
    assert app_config.check_sync_is_disabled("sync_sender_d") is True

    # All enabled
    sync_enabled = SyncConfig(source="source_e", sender="sender_e", disabled=False)
    app_config.add_sync("sync_e", sync_enabled)
    assert app_config.check_sync_is_disabled("sync_e") is False

    # Non-existent sync
    with pytest.raises(NotFoundError, match="Sync with id 'non_existent' not found."):
        app_config.check_sync_is_disabled("non_existent")

    # Missing source dependency
    sync_missing_source = SyncConfig(source="non_existent_source", sender="sender_e", disabled=False)
    with pytest.raises(NotFoundError, match="Dependency source 'non_existent_source' not found."):
        app_config.add_sync("sync_missing_source", sync_missing_source)

    # Missing pusher dependency
    sync_missing_pusher = SyncConfig(source="source_e", sender="non_existent_sender", disabled=False)
    with pytest.raises(NotFoundError, match="Dependency sender 'non_existent_sender' not found."):
        app_config.add_sync("sync_missing_pusher", sync_missing_pusher)
