"""
Async Reader-Writer Lock for asyncio.

Multiple concurrent readers, exclusive writer.
Replaces the inefficient semaphore×N pattern in FSViewBase.
"""
import asyncio
from contextlib import asynccontextmanager


class AsyncRWLock:
    """Async Reader-Writer Lock with writer priority.
    
    - Multiple readers can hold the lock concurrently.
    - A writer gets exclusive access (no readers, no other writers).
    - Writers have priority: once a writer is waiting, new readers block.
    """
    
    def __init__(self):
        self._readers = 0
        self._writer_active = False
        self._writer_waiting = False
        self._lock = asyncio.Lock()           # Protects internal state
        self._can_read = asyncio.Event()      # Signaled when readers can proceed
        self._can_write = asyncio.Event()     # Signaled when writer can proceed
        self._can_read.set()
        self._can_write.set()
    
    @asynccontextmanager
    async def read_lock(self):
        """Acquire shared read access."""
        while True:
            await self._can_read.wait()       # Wait if writer is active or waiting
            async with self._lock:
                if not self._writer_active and not self._writer_waiting:
                    self._readers += 1
                    if self._readers == 1:
                        self._can_write.clear()  # Block writers while readers active
                    break
            # If a writer snuck in, loop back and wait again
        try:
            yield
        finally:
            async with self._lock:
                self._readers -= 1
                if self._readers == 0:
                    self._can_write.set()     # Allow writers when no readers
    
    @asynccontextmanager
    async def write_lock(self):
        """Acquire exclusive write access."""
        async with self._lock:
            self._writer_waiting = True
            self._can_read.clear()            # Block new readers
        
        await self._can_write.wait()          # Wait for all readers to finish
        
        async with self._lock:
            self._writer_active = True
            self._writer_waiting = False
            self._can_write.clear()           # Block other writers
        
        try:
            yield
        finally:
            async with self._lock:
                self._writer_active = False
                self._can_read.set()          # Allow readers again
                self._can_write.set()         # Allow next writer
