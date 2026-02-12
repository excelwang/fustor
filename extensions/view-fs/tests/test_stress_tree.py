import pytest
import asyncio
import random
import os
from fustor_view_fs.state import FSState
from fustor_view_fs.tree import TreeManager

class TestTreeStress:
    """
    Chaos/Stress Test for TreeManager Concurrency.
    Goal: Ensure no deadlocks or crashes under high load.
    """

    @pytest.mark.asyncio
    async def test_high_concurrency_ops(self):
        # Setup
        # Use unlimited nodes to avoid OOM during stress test
        config = {"limits": {"max_nodes": 0}}
        state = FSState("stress-view", config)
        tree = TreeManager(state)
        
        # Parameters
        NUM_WORKERS = 50
        OPS_PER_WORKER = 100
        
        exceptions = []

        async def worker(worker_id):
            try:
                for i in range(OPS_PER_WORKER):
                    op_type = random.choice(['create_file', 'create_dir', 'delete', 'ensure_parent'])
                    path = f"/stress/Worker-{worker_id}/Item-{i}"
                    parent_path = os.path.dirname(path)
                    
                    if op_type == 'create_file':
                        payload = {"size": 100, "modified_time": 1000.0, "is_dir": False}
                        await tree.update_node(payload, path)
                    elif op_type == 'create_dir':
                        payload = {"modified_time": 1000.0, "is_dir": True}
                        await tree.update_node(payload, path)
                    elif op_type == 'delete':
                        # Delete something random from own history (approx)
                        target = f"/stress/Worker-{worker_id}/Item-{random.randint(0, i)}"
                        await tree.delete_node(target)
                    elif op_type == 'ensure_parent':
                        # Deep path creation
                        deep_path = f"{parent_path}/deep/nested/path"
                        tree._ensure_parent_chain(deep_path)
                    
                    # Small yield to allow context switch
                    if i % 10 == 0:
                        await asyncio.sleep(0)
                        
            except Exception as e:
                exceptions.append(e)

        # Run workers concurrently
        tasks = [worker(i) for i in range(NUM_WORKERS)]
        
        # Set timeout to detect deadlocks (e.g. 10 seconds for 5000 ops)
        try:
            await asyncio.wait_for(asyncio.gather(*tasks), timeout=20.0)
        except asyncio.TimeoutError:
            pytest.fail("Stress test TIMED OUT! Possible Deadlock detected.")
        
        if exceptions:
            pytest.fail(f"Stress test failed with {len(exceptions)} exceptions: {exceptions[0]}")
            
        # Basic sanity check
        assert "/" in state.directory_path_map
        assert len(state.directory_path_map) > 1
