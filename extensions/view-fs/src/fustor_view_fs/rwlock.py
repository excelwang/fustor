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
        self._writer_waiting = False
        self._lock = asyncio.Lock()          # Protects internal state
        self._readers_done = asyncio.Event()  # Signaled when readers == 0
        self._writer_done = asyncio.Event()   # Signaled when writer releases
        self._readers_done.set()
        self._writer_done.set()
        
        # Track recursive read locks per task
        self._reader_tasks = defaultdict(int)

    @asynccontextmanager
    async def read_lock(self):
        """Acquire read access (concurrent with other readers)."""
        task = asyncio.current_task()
        
        # Check if this task already holds the lock (Recursive Read)
        # If so, we can proceed immediately without checking writer status
        # to avoid self-deadlock when a writer is waiting.
        is_recursive = False
        async with self._lock:
            if self._reader_tasks[task] > 0:
                is_recursive = True
                self._reader_tasks[task] += 1
                # Readers count already incremented for this task's first lock
        
        if not is_recursive:
            # Normal acquisition for fresh reader
            # Wait if a writer is active or waiting
            while True:
                async with self._lock:
                    if not self._writer_active and not self._writer_waiting:
                        self._readers += 1
                        self._reader_tasks[task] = 1
                        if self._readers == 1:
                            self._readers_done.clear()
                        break
                # Writer is active/waiting — wait for it to finish
                await self._writer_done.wait()

        try:
            yield
        finally:
            if is_recursive:
                async with self._lock:
                    self._reader_tasks[task] -= 1
            else:
                async with self._lock:
                    self._readers -= 1
                    self._reader_tasks[task] -= 1
                    if self._reader_tasks[task] == 0:
                        del self._reader_tasks[task]
                    
                    if self._readers == 0:
                        self._readers_done.set()

    @asynccontextmanager
    async def write_lock(self):
        """Acquire write access (exclusive — blocks readers and writers)."""
        # Signal that a writer is waiting (blocks new readers)
        async with self._lock:
            self._writer_waiting = True
            self._writer_done.clear()

        # Wait for existing readers to drain
        await self._readers_done.wait()

        async with self._lock:
            self._writer_active = True
            self._writer_waiting = False

        try:
            yield
        finally:
            async with self._lock:
                self._writer_active = False
                self._writer_done.set()
