
import pytest
import time
import json
import logging
import os
# Fix import path - access constants directly from fixtures/constants or use absolute import
from ..fixtures.constants import MOUNT_POINT
from ..conftest import CONTAINER_CLIENT_A
from ..utils import docker_manager

logger = logging.getLogger("fustor_test")

class TestDebugTree:
    def test_dump_tree(self, docker_env, fusion_client, setup_agents):
        """Dump the full file tree to debug path issues."""
        
        # Create a file via Realtime (Agent A)
        realtime_file = f"{MOUNT_POINT}/debug_realtime.txt"
        docker_manager.create_file_in_container(
            CONTAINER_CLIENT_A, realtime_file, "realtime content"
        )
        
        # Wait a bit for sync
        time.sleep(5)
        
        # Dump Tree
        logger.info("Dumping full tree...")
        try:
            tree = fusion_client.get_tree(path="/", max_depth=-1)
            print("\n=== FULL TREE DUMP ===")
            print(json.dumps(tree, indent=2))
            print("======================\n")
            
            # Check for Realtime File
            # Check Absolute
            abs_found = fusion_client._find_in_tree(tree, realtime_file)
            print(f"Searching Absolute '{realtime_file}': {'FOUND' if abs_found else 'NOT FOUND'}")
            
            # Check Relative
            import os
            rel_path = os.path.relpath(realtime_file, MOUNT_POINT)
            rel_found = fusion_client._find_in_tree(tree, rel_path)
            print(f"Searching Relative '{rel_path}': {'FOUND' if rel_found else 'NOT FOUND'}")
            
        except Exception as e:
            logger.error(f"Failed to dump tree: {e}")
            raise
