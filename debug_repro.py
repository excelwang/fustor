
import asyncio
import logging
from typing import Dict, Any

# Mocking parts to avoid full environment setup
from unittest.mock import MagicMock, AsyncMock

# Adjust path
import sys
import os
sys.path.insert(0, os.getcwd())
sys.path.insert(0, os.path.join(os.getcwd(), 'agent/src'))
sys.path.insert(0, os.path.join(os.getcwd(), 'packages/core/src'))
sys.path.insert(0, os.path.join(os.getcwd(), 'packages/source-fs/src'))
sys.path.insert(0, os.path.join(os.getcwd(), 'packages/sender-http/src'))
# Also need agent-sdk, fusion-sdk, view-fs, etc if imported
sys.path.insert(0, os.path.join(os.getcwd(), 'packages/agent-sdk/src'))
sys.path.insert(0, os.path.join(os.getcwd(), 'packages/fusion-sdk/src'))
sys.path.insert(0, os.path.join(os.getcwd(), 'packages/view-fs/src'))
sys.path.insert(0, os.path.join(os.getcwd(), 'packages/receiver-http/src'))
sys.path.insert(0, os.path.join(os.getcwd(), 'packages/sender-echo/src'))


from fustor_core.models.config import SourceConfig, PasswdCredential, PipelineConfig
from fustor_agent.services.instances.pipeline import PipelineInstanceService
from fustor_agent.services.instances.bus import EventBusService
from fustor_agent.services.drivers.source_driver import SourceDriverService
from fustor_agent.services.drivers.sender_driver import SenderDriverService

async def main():
    logging.basicConfig(level=logging.DEBUG)
    
    # Mock configs
    source_config = SourceConfig(
        driver="fs",
        uri="/tmp",
        credential=PasswdCredential(user="test"),
        driver_params={"throttle_interval_sec": 5.0} # Ensure dict
    )
    
    pipeline_config = PipelineConfig(
        source="my-source",
        sender="my-sender",
        disabled=False,
        audit_interval_sec=600.0,
        sentinel_interval_sec=120.0,
        heartbeat_interval_sec=10.0
    )

    # Mock Services
    pipeline_cfg_svc = MagicMock()
    pipeline_cfg_svc.get_config.return_value = pipeline_config
    
    source_cfg_svc = MagicMock()
    source_cfg_svc.get_config.return_value = source_config
    
    sender_cfg_svc = MagicMock()
    sender_cfg_svc.get_config.return_value = MagicMock() # Sender config not the issue?
    # Ensure sender config has attributes needed
    sender_config_mock = MagicMock()
    sender_config_mock.driver = "echo" # use echo driver
    sender_config_mock.uri = "http://localhost"
    sender_config_mock.credential = None
    sender_config_mock.batch_size = 100
    sender_config_mock.timeout_sec = 30
    sender_config_mock.driver_params = {}
    sender_cfg_svc.get_config.return_value = sender_config_mock
    
    source_driver_svc = SourceDriverService()
    sender_driver_svc = SenderDriverService() # Will need to find drivers?
    # We might need to mock _get_driver_by_type if entrypoints don't work
    
    # Try to load FSDriver manually if entrypoints fail
    try:
        from fustor_source_fs.driver import FSDriver
        source_driver_svc._driver_cache["fs"] = FSDriver
    except ImportError as e:
        print(f"Failed to import FSDriver: {e}")

    try:
        from fustor_sender_echo.driver import EchoSender
        sender_driver_svc._driver_cache["echo"] = EchoSender
    except ImportError:
        pass

    bus_svc = EventBusService({ "my-source": source_config }, source_driver_svc)
    
    svc = PipelineInstanceService(
        pipeline_config_service=pipeline_cfg_svc,
        source_config_service=source_cfg_svc,
        sender_config_service=sender_cfg_svc,
        bus_service=bus_svc,
        sender_driver_service=sender_driver_svc,
        source_driver_service=source_driver_svc,
        agent_id="agent-A"
    )
    
    bus_svc.set_dependencies(svc)

    print("Starting pipeline...")
    try:
        await svc.start_one("my-pipeline")
        print("Success")
    except Exception as e:
        print(f"Caught expected exception: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    asyncio.run(main())
