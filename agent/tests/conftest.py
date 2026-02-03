import pytest
import pytest_asyncio
import asyncio
import os
from unittest.mock import MagicMock, AsyncMock
import yaml

from fustor_agent.app import App
from fustor_core.models.config import SenderConfig, PipelineConfig, PasswdCredential, FieldMapping, SourceConfig
from fustor_core.event import EventBase, InsertEvent

@pytest.fixture(scope="function")
def test_app_instance(tmp_path):
    config_dir = tmp_path / ".conf"
    config_dir.mkdir()
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
        "senders": {},
        "pipelines": {},
    }
    with open(config_dir / "agent-config.yaml", 'w') as f:
        yaml.dump(config_content, f)

    app = App(config_dir=str(config_dir))
    return app

@pytest_asyncio.fixture
async def snapshot_sync_test_setup(test_app_instance: App, mocker):
    # ... (this fixture is unchanged)
    pipeline_id = "test-snapshot-phase"
    source_id = "test-snapshot-source"
    sender_id = "test-snapshot-sender"

    mock_sender_driver = MagicMock()
    mock_sender_driver.send_events = AsyncMock()
    mocker.patch.object(test_app_instance.sender_driver_service, 'get_latest_committed_index', AsyncMock(return_value=0))
    # --- REFACTORED: Provide a schema to guide the mapping logic ---
    mock_schema = {
        "properties": {
            "events.content": {
                "type": "object"
            }
        }
    }
    mocker.patch.object(test_app_instance.sender_driver_service, 'get_needed_fields', AsyncMock(return_value=mock_schema))
    mocker.patch.object(test_app_instance.sender_driver_service, '_get_driver_by_type', return_value=mock_sender_driver)

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

    await test_app_instance.sender_config_service.add_config(sender_id, SenderConfig(
        driver="mock-driver", uri="mock-endpoint", credential=PasswdCredential(user="mock"), disabled=False
    ))
    
    await test_app_instance.pipeline_config_service.add_config(pipeline_id, PipelineConfig(
        source=source_id,
        sender=sender_id,
        disabled=False,
        fields_mapping=[
            FieldMapping(to="events.content", source=["mock_db.mock_table.id:0", "mock_db.mock_table.name:1"])
        ]
    ))

    class Setup:
        def __init__(self):
            self.pipeline_id = pipeline_id
            self.mock_sender_driver = mock_sender_driver
            self.spy_get_snapshot_iterator = spy_get_snapshot_iterator
            self.snapshot_data = snapshot_data

    yield Setup()

    await test_app_instance.pipeline_config_service.delete_config(pipeline_id)
    await test_app_instance.source_config_service.delete_config(source_id)
    await test_app_instance.sender_config_service.delete_config(sender_id)

@pytest_asyncio.fixture
async def message_sync_test_setup(test_app_instance: App, mocker):
    pipeline_id = "test-message-sync"
    source_id = "test-message-source"
    sender_id = "test-message-sender"
    start_position = 99999

    mock_sender_driver = MagicMock()
    mock_sender_driver.send_events = AsyncMock()
    mocker.patch.object(test_app_instance.sender_driver_service, 'get_latest_committed_index', AsyncMock(return_value=start_position))
    # --- REFACTORED: Provide a schema to guide the mapping logic ---
    mock_schema = {
        "properties": {
            "events.content": {
                "type": "object"
            }
        }
    }
    mocker.patch.object(test_app_instance.sender_driver_service, 'get_needed_fields', AsyncMock(return_value=mock_schema))
    # --- END REFACTOR ---
    mocker.patch.object(test_app_instance.sender_driver_service, '_get_driver_by_type', return_value=mock_sender_driver)

    message_data = [
        InsertEvent(event_schema='mock_db', table='mock_table', rows=[{'id': 101, 'name': 'realtime_1'}], index=start_position + 1),
        InsertEvent(event_schema='mock_db', table='mock_table', rows=[{'id': 102, 'name': 'realtime_2'}], index=start_position + 2),
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

    await test_app_instance.sender_config_service.add_config(sender_id, SenderConfig(
        driver="mock-driver", uri="mock-endpoint", credential=PasswdCredential(user="mock"), disabled=False
    ))

    await test_app_instance.pipeline_config_service.add_config(pipeline_id, PipelineConfig(
        source=source_id,
        sender=sender_id,
        disabled=False,
        fields_mapping=[
            FieldMapping(to="events.content", source=["mock_db.mock_table.id:0", "mock_db.mock_table.name:1"])
        ]
    ))

    class Setup:
        def __init__(self):
            self.pipeline_id = pipeline_id
            self.start_position = start_position
            self.mock_sender_driver = mock_sender_driver
            self.spy_get_message_iterator = spy_get_message_iterator
            self.spy_get_snapshot_iterator = spy_get_snapshot_iterator
            self.message_data = message_data

    yield Setup()

    await test_app_instance.pipeline_config_service.delete_config(pipeline_id)
    await test_app_instance.source_config_service.delete_config(source_id)
    await test_app_instance.sender_config_service.delete_config(sender_id)
