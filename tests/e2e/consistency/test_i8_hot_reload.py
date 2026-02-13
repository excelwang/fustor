
import time
import yaml
import logging
import pytest
from utils import docker_manager
from fixtures.constants import (
    CONTAINER_CLIENT_A, 
    CONTAINER_FUSION, 
    MOUNT_POINT,
    TEST_VIEW_ID
)

logger = logging.getLogger("fustor_test")

@pytest.mark.asyncio
async def test_i8_hot_reload_add_source(reset_fusion_state, setup_agents, fusion_client):
    """
    Test the Dynamic Scaling / Hot Reload workflow:
    1. Start with standard Agent A (Source 1) -> Fusion (View 1).
    2. Hot-add a Multi-FS view aggregating [View 1].
    3. Hot-add a new Source 2 on Agent A.
    4. Hot-add View 2 (for Source 2) on Fusion and add to Multi-FS view.
    5. Verify Source 2 data appears in Multi-FS view.
    """
    env = setup_agents
    api_key_base = env["api_key"]
    
    # 0. Prepare new source directory on Agent A
    new_mount_point = "/tmp/extra_source"
    docker_manager.exec_in_container(CONTAINER_CLIENT_A, ["mkdir", "-p", new_mount_point])
    new_file_path = f"{new_mount_point}/new_file.txt"
    docker_manager.create_file_in_container(CONTAINER_CLIENT_A, new_file_path, content="hello dynamic world")
    
    # helper to read/write yaml in container
    def update_yaml_in_container(container, path, updater_func):
        # Read
        res = docker_manager.exec_in_container(container, ["cat", path])
        if res.returncode != 0:
            raise RuntimeError(f"Failed to read {path}")
        data = yaml.safe_load(res.stdout)
        
        # Update
        updater_func(data)
        
        # Write back
        new_content = yaml.dump(data)
        docker_manager.create_file_in_container(container, path, content=new_content)
        
    logger.info("=== Step 1: Modifying Fusion Config ===")
    
    def update_fusion(data):
        # 1. Define new View for the new source
        data.setdefault("views", {})
        data["views"]["view-extra"] = {
            "driver": "fs",
            "driver_params": {
                "hot_file_threshold": 60.0
            }
        }
        
        # 2. Define/Update Multi-FS View
        # If it doesn't exist, create it. If it exists, append to members.
        if "global-multi-fs" not in data["views"]:
            data["views"]["global-multi-fs"] = {
                "driver": "multi-fs",
                "api_keys": ["query-multi-key"],
                "driver_params": {
                    "members": [TEST_VIEW_ID, "view-extra"]
                }
            }
        else:
            # Should not happen in fresh env, but handle anyway
            members = data["views"]["global-multi-fs"]["driver_params"]["members"]
            if "view-extra" not in members:
                members.append("view-extra")

        # 3. Define new Pipe
        data.setdefault("pipes", {})
        data["pipes"]["pipe-extra"] = {
            "receiver": "http-main",
            "views": ["view-extra"]
        }
        
        # 4. Add key to receiver
        # We assume 'http-main' exists from default config
        receivers = data.get("receivers", {})
        if "http-main" in receivers:
             keys = receivers["http-main"].get("api_keys", [])
             keys.append({
                 "key": "extra-api-key",
                 "pipe_id": "pipe-extra"
             })
             receivers["http-main"]["api_keys"] = keys
    
    update_yaml_in_container(CONTAINER_FUSION, "/root/.fustor/fusion.yaml", update_fusion)
    
    logger.info("=== Step 2: Reloading Fusion ===")
    docker_manager.exec_in_container(CONTAINER_FUSION, ["fustor-fusion", "reload"])
    
    # Wait a bit for fusion to reload and register new views
    time.sleep(2)
    
    # Verify Multi-FS view is active (but 'view-extra' is empty/disconnected yet)
    # We query the multi-fs view
    # Note: fusion_client default view is TEST_VIEW_ID. We need to query global-multi-fs.
    # The fixture client might be bound to specific view, let's make a raw request or use client support
    # The fixture helper `fusion_client` is bound to `TEST_VIEW_ID`. We can use its internal client to query other views if needed
    # or just rely on the fact that we can change view_id in the request if the method supports it?
    # FusionClient methods usually take view_id from init. specialized methods might need override.
    # Let's perform a raw check via curl in container or similar, OR use the client if it allows flexible path.
    # actually `fusion_client` class has `view_id` as instance var.
    
    logger.info("=== Step 3: Modifying Agent Config ===")
    
    def update_agent(data):
        # 1. New Source
        data.setdefault("sources", {})
        data["sources"]["source-extra"] = {
            "driver": "fs",
            "uri": new_mount_point
        }
        
        # 2. New Sender
        data.setdefault("senders", {})
        data["senders"]["sender-extra"] = {
            "driver": "fusion",
            # We assume FUSION_ENDPOINT env var was used, but in yaml it's resolved.
            # We can copy uri from existing sender
            "uri": data["senders"]["fusion-main"]["uri"],
            "credential": {
                "key": "extra-api-key"
            }
        }
        
        # 3. New Pipe
        data.setdefault("pipes", {})
        data["pipes"]["pipe-extra"] = {
            "source": "source-extra",
            "sender": "sender-extra",
            "audit_interval_sec": 5, # Fast audit for test
        }
        
    update_yaml_in_container(CONTAINER_CLIENT_A, "/root/.fustor/agent-config/default.yaml", update_agent)
    
    logger.info("=== Step 4: Reloading Agent ===")
    docker_manager.exec_in_container(CONTAINER_CLIENT_A, ["fustor-agent", "reload"])
    
    # Wait for agent to connect and push data
    time.sleep(5)
    
    logger.info("=== Step 5: Verification ===")
    
    # We need to query `global-multi-fs`. 
    # Since existing fusion_client is bound to TEST_VIEW_ID, let's instantiate a temporary one or hack it.
    # But wait, api_keys for multi-fs is "query-multi-key".
    # The generic client might fail auth if we don't supply that key.
    
    from utils import FusionClient
    multi_client = FusionClient(base_url="http://localhost:18102", view_id="global-multi-fs")
    multi_client.set_api_key("query-multi-key")

    # Retry verification loop
    found = False
    for i in range(10):
        try:
            # Query root tree
            tree = multi_client.get_tree("/")
            # Expect members view-extra to have data
            members = tree.get("members", {})
            
            # Check if view-extra is present and healthy
            if "view-extra" in members:
                view_data = members["view-extra"]
                if view_data.get("status") == "ok":
                    children = view_data.get("data", {}).get("children", [])
                    file_names = [c["name"] for c in children]
                    if "new_file.txt" in file_names:
                        logger.info("Found new_file.txt in Multi-FS view via view-extra!")
                        found = True
                        break
            
            logger.info(f"Waiting for sync... (Attempt {i+1})")
        except Exception as e:
            logger.warning(f"Query failed: {e}")
            
        time.sleep(2)
        
    assert found, "Failed to find 'new_file.txt' in dynamically added source via Multi-FS view"
