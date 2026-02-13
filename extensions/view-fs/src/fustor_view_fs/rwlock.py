"""
Async Reader-Writer Lock for asyncio.

Multiple concurrent readers, exclusive writer.
Uses asyncio primitives only — no threads, no spinning.

Spec: 06-CONCURRENCY_PERFORMANCE requires that read operations
(queries, process_event) do not block each other, while write
operations (audit_start/end, reset) must be exclusive.
"""
import asyncio
from contextlib import asynccontextmanager
from collections import defaultdict


class AsyncRWLock:
    """Async Reader-Writer Lock: multiple concurrent readers, exclusive writer.
    
    Guarantees:
    - Multiple readers can hold the lock simultaneously.
    - A writer blocks until all readers release, then holds exclusively.
    - New readers are blocked while a writer is waiting (prevents writer starvation).
    """

    def __init__(self):
        self._readers = 0
        self._writer_active = False
        self._writer_waiting = False # Flag to indicate if a writer is waiting, used to block new readers
        self._lock = asyncio.Lock()          # Protects internal state
        self._readers_cond = asyncio.Condition(self._lock) # Condition for readers to wait on
        self._writer_cond = asyncio.Condition(self._lock)  # Condition for writers to wait on
        
        # Track recursive read locks per task
        self._reader_tasks = defaultdict(int)

    @asynccontextmanager
    async def read_lock(self):
        """Acquire read access (concurrent with other readers)."""
        task = asyncio.current_task()
        
        async with self._lock:
            # If this task already holds a read lock, it's a recursive read.
            # Allow it immediately without waiting, as it doesn't block a writer.
            if self._reader_tasks[task] > 0:
                self._reader_tasks[task] += 1
                try:
                    yield
                finally:
                    async with self._lock:
                        self._reader_tasks[task] -= 1
                        if self._reader_tasks[task] == 0:
                            del self._reader_tasks[task]
                return

            # Normal read acquisition: wait if a writer is active or waiting.
            while self._writer_active or self._writer_waiting:
                await self._readers_cond.wait()
            
            self._readers += 1
            self._reader_tasks[task] = 1 # Mark initial acquisition

        try:
            yield
        finally:
            async with self._lock:
                self._readers -= 1
                self._reader_tasks[task] -= 1
                if self._reader_tasks[task] == 0:
                    del self._reader_tasks[task]
                
                # If no more readers and a writer is waiting, signal the writer
                if self._readers == 0 and self._writer_waiting:
                    self._writer_cond.notify()


    @asynccontextmanager
    async def write_lock(self):
        """Acquire write access (exclusive — blocks readers and writers)."""
        task = asyncio.current_task()
        async with self._lock:
            # If any part of this task holds a read lock, it's a recursive deadlock potential
            # The current impl doesn't support recursive write locks, or write while holding read.
            # This would block itself. For now, we assume non-recursiveness.
            if self._reader_tasks[task] > 0:
                raise RuntimeError("Cannot acquire write_lock while holding read_lock in the same task.")

            self._writer_waiting = True
            # Wait until no other writer is active and no readers
            while self._writer_active or self._readers > 0:
                await self._writer_cond.wait()
            
            self._writer_active = True
            self._writer_waiting = False
            # Block new readers now that a writer is active.
            # No need to notify _readers_cond, as readers will wait on _writer_active.

        try:
            yield
        finally:
            async with self._lock:
                self._writer_active = False
                # Notify all waiting readers and writers.
                # Readers will re-check _writer_active and _writer_waiting.
                # Writers will re-check _writer_active and _readers > 0.
                self._readers_cond.notify_all()
                self._writer_cond.notify_all() # To potentially wake up another waiting writer
