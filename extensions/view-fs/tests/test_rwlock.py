import asyncio
import logging
import sys
import unittest
import os

# Add extensions/view-fs to path if running from root
import os
import sys

# If running from root, add extensions/view-fs/src
sys.path.insert(0, os.path.abspath("extensions/view-fs/src"))
# If running relative, adjust? No, pytest runs from root in monorepo usually.
from fustor_view_fs.rwlock import AsyncRWLock

class TestRWLockDeadlock(unittest.IsolatedAsyncioTestCase):
    async def test_recursive_read_deadlock_with_pending_writer(self):
        lock = AsyncRWLock()
        
        print("1. Acquire Read Lock (Outer)")
        async with lock.read_lock():
            print("   Outer Lock Acquired")
            
            # Start a background writer that will block waiting for readers
            async def writer_task():
                print("   [Writer] Trying to acquire Write Lock...")
                async with lock.write_lock():
                    print("   [Writer] Write Lock Acquired!")
                print("   [Writer] Write Lock Released")

            w_task = asyncio.create_task(writer_task())
            
            # Give writer a moment to set _writer_waiting = True
            await asyncio.sleep(0.1)
            
            print("2. Attempt Acquire Read Lock (Inner) - SHOULD DEADLOCK if buggy")
            try:
                # Use a timeout to detect deadlock
                async with asyncio.timeout(2.0):
                    async with lock.read_lock():
                        print("   Inner Lock Acquired!")
            except asyncio.TimeoutError:
                print("!!! DEADLOCK DETECTED !!!")
                # Clean up writer
                w_task.cancel()
                raise

        await w_task

if __name__ == "__main__":
    unittest.main()
