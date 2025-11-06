import pytest
import pytest_asyncio
import asyncio
import os
from unittest.mock import MagicMock, AsyncMock
import yaml

from fustor_agent.app import App
from fustor_core.models.config import PusherConfig, SyncConfig, PasswdCredential, FieldMapping, SourceConfig
from fustor_core.models.event import EventBase, InsertEvent

@pytest.fixture(scope="function")
def test_app_instance(tmp_path):
    config_dir = tmp_path / ".conf"
    config_dir.mkdir()
    # ... (rest of the fixture is unchanged)
    config_file = config_dir / "config.yaml"
    config_content = {
        "sources": {
            "test-test": {
                "driver": "mysql",
                "uri": "localhost:3306",
                "credential": {"user": "fustor_agent", "passwd": ""},
                "disabled": False,
                "driver_params": {"stability_interval": 0.5}
            }
        },
        "pushers": {},
        "syncs": {},
    }
    with open(config_file, 'w') as f:
        yaml.dump(config_content, f)

    app = App(config_dir=str(config_dir))
    return app

@pytest_asyncio.fixture
async def snapshot_phase_test_setup(test_app_instance: App, mocker):
    # ... (this fixture is unchanged)
    sync_id = "test-snapshot-sync"
    source_id = "test-snapshot-source"
    pusher_id = "test-snapshot-pusher"

    mock_pusher_driver = MagicMock()
    mock_pusher_driver.push = AsyncMock()
    mocker.patch.object(test_app_instance.pusher_driver_service, 'get_latest_committed_index', AsyncMock(return_value=0))
    # --- REFACTORED: Provide a schema to guide the mapping logic ---
    mock_schema = {
        "properties": {
            "events.content": {
                "type": "object"
            }
        }
    }
    mocker.patch.object(test_app_instance.pusher_driver_service, 'get_needed_fields', AsyncMock(return_value=mock_schema))
    mocker.patch.object(test_app_instance.pusher_driver_service, '_get_driver_by_type', return_value=mock_pusher_driver)

    snapshot_data = [{'id': 1, 'name': 'record_1'}, {'id': 2, 'name': 'record_2'}]
    snapshot_end_position = 12345

    async def snapshot_side_effect(process_batch_callback, **kwargs):
        # FIX: Run the callback in a separate thread to simulate the real driver behavior
        # and avoid deadlocking the main event loop.
        await asyncio.to_thread(process_batch_callback, snapshot_data)
        return snapshot_end_position

    spy_get_snapshot_iterator = mocker.patch.object(
        test_app_instance.source_driver_service,
        'get_snapshot_iterator',
        side_effect=snapshot_side_effect
    )
    mocker.patch.object(test_app_instance.source_driver_service, 'get_message_iterator', return_value=iter([]))

    source_config = SourceConfig(driver="mock-mysql", uri="mock-uri", credential=PasswdCredential(user="mock"), disabled=False)
    await test_app_instance.source_config_service.add_config(source_id, source_config)

    await test_app_instance.pusher_config_service.add_config(pusher_id, PusherConfig(
        driver="mock-driver", endpoint="mock-endpoint", credential=PasswdCredential(user="mock"), disabled=False
    ))
    
    await test_app_instance.sync_config_service.add_config(sync_id, SyncConfig(
        source=source_id,
        pusher=pusher_id,
        disabled=False,
        fields_mapping=[
            FieldMapping(to="events.content", source=["mock_db.mock_table.id:0", "mock_db.mock_table.name:1"])
        ]
    ))

    class Setup:
        def __init__(self):
            self.sync_id = sync_id
            self.mock_pusher_driver = mock_pusher_driver
            self.spy_get_snapshot_iterator = spy_get_snapshot_iterator
            self.snapshot_data = snapshot_data

    yield Setup()

    await test_app_instance.sync_config_service.delete_config(sync_id)
    await test_app_instance.source_config_service.delete_config(source_id)
    await test_app_instance.pusher_config_service.delete_config(pusher_id)

@pytest_asyncio.fixture
async def message_phase_test_setup(test_app_instance: App, mocker):
    sync_id = "test-message-sync"
    source_id = "test-message-source"
    pusher_id = "test-message-pusher"
    start_position = 99999

    mock_pusher_driver = MagicMock()
    mock_pusher_driver.push = AsyncMock()
    mocker.patch.object(test_app_instance.pusher_driver_service, 'get_latest_committed_index', AsyncMock(return_value=start_position))
    # --- REFACTORED: Provide a schema to guide the mapping logic ---
    mock_schema = {
        "properties": {
            "events.content": {
                "type": "object"
            }
        }
    }
    mocker.patch.object(test_app_instance.pusher_driver_service, 'get_needed_fields', AsyncMock(return_value=mock_schema))
    # --- END REFACTOR ---
    mocker.patch.object(test_app_instance.pusher_driver_service, '_get_driver_by_type', return_value=mock_pusher_driver)

    message_data = [
        InsertEvent(schema='mock_db', table='mock_table', rows=[{'id': 101, 'name': 'realtime_1'}], index=start_position + 1),
        InsertEvent(schema='mock_db', table='mock_table', rows=[{'id': 102, 'name': 'realtime_2'}], index=start_position + 2),
    ]
    spy_get_message_iterator = mocker.patch.object(
        test_app_instance.source_driver_service,
        'get_message_iterator',
        return_value=iter(message_data)
    )
    spy_get_snapshot_iterator = mocker.patch.object(
        test_app_instance.source_driver_service,
        'get_snapshot_iterator',
        side_effect=AsyncMock()
    )

    source_config = SourceConfig(driver="mock-mysql", uri="mock-uri", credential=PasswdCredential(user="mock"), disabled=False)
    await test_app_instance.source_config_service.add_config(source_id, source_config)

    await test_app_instance.pusher_config_service.add_config(pusher_id, PusherConfig(
        driver="mock-driver", endpoint="mock-endpoint", credential=PasswdCredential(user="mock"), disabled=False
    ))

    await test_app_instance.sync_config_service.add_config(sync_id, SyncConfig(
        source=source_id,
        pusher=pusher_id,
        disabled=False,
        fields_mapping=[
            FieldMapping(to="events.content", source=["mock_db.mock_table.id:0", "mock_db.mock_table.name:1"])
        ]
    ))

    class Setup:
        def __init__(self):
            self.sync_id = sync_id
            self.start_position = start_position
            self.mock_pusher_driver = mock_pusher_driver
            self.spy_get_message_iterator = spy_get_message_iterator
            self.spy_get_snapshot_iterator = spy_get_snapshot_iterator
            self.message_data = message_data

    yield Setup()

    await test_app_instance.sync_config_service.delete_config(sync_id)
    await test_app_instance.source_config_service.delete_config(source_id)
    await test_app_instance.pusher_config_service.delete_config(pusher_id)
