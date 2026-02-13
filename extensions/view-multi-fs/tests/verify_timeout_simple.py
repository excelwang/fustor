import asyncio
import sys
import logging
from typing import Dict, Any, Optional

# Basic logger config
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("test_multi_fs_timeout")

# Simple mock for testing without full Fustor stack
class MockDriver:
    async def get_subtree_stats(self, path: str):
        # Base method if called directly
        return {"file_count": 0}

class SimpleMultiFSViewDriver:
    # Mimics MultiFSViewDriver structure enough to test timeout logic
    def __init__(self):
        self.members = ["slow-member", "fast-member"]
        self.query_timeout = 2.0  # Default long timeout

    async def get_subtree_stats_agg(self, path: str, timeout: Optional[float] = None) -> Dict[str, Any]:
        results = []
        
        async def fetch_stats(vid):
            try:
                effective_timeout = timeout if timeout is not None else self.query_timeout
                return await asyncio.wait_for(self._fetch_stats_internal(vid, path), timeout=effective_timeout)
            except asyncio.TimeoutError:
                return {"view_id": vid, "status": "error", "error": "Timeout"}
            except Exception as e:
                return {"view_id": vid, "status": "error", "error": str(e)}

        if self.members:
            # Fixed: Use asyncio.gather correctly
            tasks = [fetch_stats(vid) for vid in self.members]
            results_list = await asyncio.gather(*tasks)
            results = list(results_list)
        
        return {
            "path": path,
            "members": results
        }

    async def _fetch_stats_internal(self, vid: str, path: str) -> Dict[str, Any]:
        if vid == "slow-member":
            await asyncio.sleep(1.0) # Takes 1s
        else:
            await asyncio.sleep(0.1) # Takes 0.1s
        return {"view_id": vid, "status": "ok", "file_count": 10}

async def verify_timeout():
    driver = SimpleMultiFSViewDriver()
    
    print("\n--- Test 1: Default timeout (2.0s) ---")
    # Both members take < 2.0s to complete
    result = await driver.get_subtree_stats_agg("/", timeout=None)
    members = result["members"]
    
    slow = next(m for m in members if m["view_id"] == "slow-member")
    fast = next(m for m in members if m["view_id"] == "fast-member")
    
    assert slow["status"] == "ok", f"Slow should be OK, got {slow}"
    assert fast["status"] == "ok", f"Fast should be OK, got {fast}"
    print("✓ Passed: Both members succeeded within default timeout")

    print("\n--- Test 2: Short timeout (0.5s) ---")
    # Slow takes 1.0s, so it should Time Out
    result = await driver.get_subtree_stats_agg("/", timeout=0.5)
    members = result["members"]
    
    slow = next(m for m in members if m["view_id"] == "slow-member")
    fast = next(m for m in members if m["view_id"] == "fast-member")

    assert slow["status"] == "error", f"Slow member should be error, got {slow['status']}"
    assert slow["error"] == "Timeout", f"Error should be Timeout, got {slow.get('error')}"
    assert fast["status"] == "ok", "Fast member should be ok"
    print("✓ Passed: Slow member timed out as expected")

if __name__ == "__main__":
    asyncio.run(verify_timeout())
