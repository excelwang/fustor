import collections
import logging
import os
import threading
import time
import dataclasses
import heapq
from typing import Dict, List, Optional, Set, Tuple
from fustor_core.exceptions import DriverError # NEW IMPORT

# Use the low-level inotify wrapper and high-level event types
from watchdog.observers.inotify_c import Inotify
from watchdog.events import (
    FileClosedEvent,
    FileCreatedEvent,
    FileDeletedEvent,
    FileModifiedEvent,
    FileMovedEvent,
    DirCreatedEvent,
    DirDeletedEvent,
    DirModifiedEvent,
    DirMovedEvent,
)

logger = logging.getLogger("fustor_agent.driver.fs")

def contains_surrogate_characters(path: str) -> bool:
    """Checks if a string contains surrogate characters."""
    try:
        path.encode('utf-8')
        return False
    except UnicodeEncodeError:
        return True

def safe_path_encode(path: str) -> bytes:
    """Safely encodes a path to bytes, handling surrogate characters using filesystem encoding."""
    try:
        return os.fsencode(path)
    except Exception:
        # Fallback for extreme cases or non-string inputs, though fsencode is robust
        return path.encode('utf-8', errors='replace')

def safe_path_handling(path: str) -> str:
    """Safely handles path strings, normalizing surrogate characters if present."""
    if contains_surrogate_characters(path):
        # Replace surrogate characters with underscores or question marks
        # by encoding with replacement and decoding back
        return path.encode('utf-8', errors='replace').decode('utf-8')
    return path

@dataclasses.dataclass(frozen=True)
class WatchEntry:
    """Simplified entry for the LRU cache, just holds the timestamp."""
    timestamp: float

class _LRUCache:
    """A custom cache that evicts the item with the oldest timestamp (smallest value)."""
    def __init__(self, capacity: int):
        self.capacity = capacity
        self.cache: Dict[str, WatchEntry] = {}  # path -> WatchEntry
        self.min_heap: List[Tuple[float, str]] = []  # (timestamp, path)
        self.removed_from_heap: Set[str] = set()

    def _clean_heap(self):
        """Removes stale entries from the top of the min_heap."""
        while self.min_heap:
            timestamp, path = self.min_heap[0]
            if path in self.removed_from_heap:
                heapq.heappop(self.min_heap)
                self.removed_from_heap.remove(path)
            elif path not in self.cache:
                heapq.heappop(self.min_heap)
            else:
                break

    def get(self, key: str) -> Optional[WatchEntry]:
        return self.cache.get(key)

    def put(self, key: str, value: WatchEntry):
        if key in self.cache:
            self.removed_from_heap.add(key)
        self.cache[key] = value
        heapq.heappush(self.min_heap, (value.timestamp, key))

    def evict(self) -> Optional[Tuple[str, WatchEntry]]:
        """Removes and returns the item with the oldest timestamp."""
        if not self.cache:
            return None
        self._clean_heap()
        if not self.min_heap:
            return None
        _timestamp, oldest_key = heapq.heappop(self.min_heap)
        self.removed_from_heap.discard(oldest_key)
        oldest_entry = self.cache.pop(oldest_key)
        return oldest_key, oldest_entry

    def get_oldest(self) -> Optional[Tuple[str, WatchEntry]]:
        """Returns the item with the oldest timestamp without removing it."""
        if not self.cache:
            return None
        self._clean_heap()
        if not self.min_heap:
            return None
        _timestamp, oldest_key = self.min_heap[0]
        return oldest_key, self.cache[oldest_key]

    def remove(self, key: str):
        if key in self.cache:
            del self.cache[key]
            self.removed_from_heap.add(key)

    def __contains__(self, key: str) -> bool:
        return key in self.cache

    def __len__(self) -> int:
        return len(self.cache)


class _WatchManager:
    """
    Manages a single inotify instance and its watches, including LRU pruning.
    This is a more resource-efficient implementation.
    """
    def __init__(self, root_path: str, event_handler, min_monitoring_window_days: float = 30.0, stop_driver_event: threading.Event = None, throttle_interval: float = 5.0):
        logger.info(f"Creating a new Inotify instance for root path {root_path}.")
        self.watch_limit = 10000000  # This now only limits watches, not instances.
        self.lru_cache = _LRUCache(self.watch_limit)
        self.event_handler = event_handler
        self.root_path = root_path
        self._lock = threading.RLock()
        self.min_monitoring_window_days = min_monitoring_window_days
        self.stop_driver_event = stop_driver_event 
        self.throttle_interval = throttle_interval

        self.inotify = None
        self._ensure_inotify()
        
        self._stop_event = threading.Event()
        self.inotify_thread = None
    
    def _ensure_inotify(self):
        """Ensure inotify instance is created and valid."""
        if self.inotify is None:
            with self._lock:
                if self.inotify is None:
                    # Directly use the low-level Inotify class
                    # We watch the root path non-recursively just to initialize the instance.
                    self.inotify = Inotify(safe_path_encode(self.root_path), recursive=False)

    def _get_current_time(self) -> float:
        """Returns the current time (Agent physical time)."""
        return time.time()

    def _event_processing_loop(self):
        """
        The core event loop that reads from inotify and dispatches events.
        """
        while not self._stop_event.is_set():
            try:
                raw_events = self.inotify.read_events()

                # Pre-process to identify paired moves and avoid duplicate events.
                paired_move_from_paths = set()
                for event in raw_events:
                    if event.is_moved_to:
                        src_path_from = self.inotify.source_for_move(event)
                        if src_path_from:
                            paired_move_from_paths.add(os.fsdecode(src_path_from))

                for event in raw_events:
                        # Use fsdecode to safely decode bytes to str, it handles surrogates correctly
                        src_path_str = os.fsdecode(event.src_path)

                        # Handle paired moves (MOVED_FROM + MOVED_TO)
                        if event.is_moved_to:
                            src_path_from = self.inotify.source_for_move(event)
                            if src_path_from:
                                src_path_from_str = os.fsdecode(src_path_from)
                                if event.is_directory:
                                    self.event_handler.on_moved(DirMovedEvent(src_path_from_str, src_path_str))
                                else:
                                    self.event_handler.on_moved(FileMovedEvent(src_path_from_str, src_path_str))
                            else:
                                # Unmatched MOVED_TO: treat as creation
                                if event.is_directory:
                                    self.event_handler.on_created(DirCreatedEvent(src_path_str))
                                else:
                                    self.event_handler.on_created(FileCreatedEvent(src_path_str))
                        
                        # Handle unmatched MOVED_FROM (treat as deletion)
                        elif event.is_moved_from:
                            if src_path_str in paired_move_from_paths:
                                continue # Already processed as part of a move
                            
                            if event.is_directory:
                                self.event_handler.on_deleted(DirDeletedEvent(src_path_str))
                            else:
                                self.event_handler.on_deleted(FileDeletedEvent(src_path_str))

                        # Handle creation events
                        elif event.is_create:
                            if event.is_directory:
                                self.event_handler.on_created(DirCreatedEvent(src_path_str))
                            else:
                                self.event_handler.on_created(FileCreatedEvent(src_path_str))

                        # Handle deletion events
                        elif event.is_delete:
                            if event.is_directory:
                                self.event_handler.on_deleted(DirDeletedEvent(src_path_str))
                            else:
                                self.event_handler.on_deleted(FileDeletedEvent(src_path_str))

                        # Handle modification events (attrib or modify)
                        elif event.is_attrib or event.is_modify:
                            if event.is_directory:
                                self.event_handler.on_modified(DirModifiedEvent(src_path_str))
                            else:
                                self.event_handler.on_modified(FileModifiedEvent(src_path_str))

                        # Handle file closed after write (definitive modification)
                        elif event.is_close_write:
                            if event.is_directory:
                                 self.event_handler.on_closed(DirModifiedEvent(src_path_str))
                            else:
                                 self.event_handler.on_closed(FileClosedEvent(src_path_str))

                        # Handle ignored events (watch removed)
                        elif event.is_ignored:
                            with self._lock:
                                if src_path_str in self.lru_cache:
                                    self.lru_cache.remove(src_path_str)
                                    logger.debug(f"Removed watch for '{safe_path_handling(src_path_str)}' from LRU cache due to IGNORED event.")

            except KeyError as e:
                logger.debug(f"Ignoring event for untracked watch descriptor: {str(e)}")

    def schedule(self, path: str, timestamp: Optional[float] = None):
        with self._lock:
            timestamp_to_use = timestamp if timestamp is not None else self._get_current_time()
            if path in self.lru_cache:
                existing_entry = self.lru_cache.get(path)
                if existing_entry and existing_entry.timestamp < timestamp_to_use:
                    self.lru_cache.put(path, WatchEntry(timestamp_to_use))
                return

            is_eviction_needed = len(self.lru_cache) >= self.watch_limit
            
            oldest = self.lru_cache.get_oldest()
            if oldest and oldest[1].timestamp >= timestamp_to_use and is_eviction_needed:
                # If the new watch is older than the oldest in a FULL cache, 
                # we skip it to prevent thrashing.
                logger.debug(f"New watch for {safe_path_handling(path)} (ts {timestamp_to_use:.2f}) is older than oldest in cache (ts {oldest[1].timestamp:.2f}). Skipping.")
                return

            if is_eviction_needed:
                evicted_item = self.lru_cache.evict()
                if evicted_item:
                    evicted_path, evicted_entry = evicted_item
                    relative_age_days = (self._get_current_time() - evicted_entry.timestamp) / 86400
                    
                    if relative_age_days < self.min_monitoring_window_days:
                        error_msg = (
                            f"Watch limit reached and an active watch for {evicted_path} "
                            f"(relative age: {relative_age_days:.2f} days) is about to be evicted. "
                            f"This is below the configured min_monitoring_window_days ({self.min_monitoring_window_days} days). "
                            f"Stopping driver to prevent data loss."
                        )
                        logger.error(error_msg)
                        if self.stop_driver_event:
                            self.stop_driver_event.set()
                        raise DriverError(error_msg)

                    logger.info(f"Watch limit reached. Evicting watch for {safe_path_handling(evicted_path)} (relative age: {relative_age_days:.2f} days).")
                    
                    # 1. Explicitly remove the evicted watch handle
                    try:
                        self.inotify.remove_watch(safe_path_encode(evicted_path))
                    except (KeyError, OSError) as e:
                        logger.warning(f"Error removing evicted watch for {safe_path_handling(evicted_path)}: {e}")
                    
                    # 2. Recursively unschedule any children that might still be in the cache
                    self.unschedule_recursive(evicted_path)
                else:
                    logger.warning(f"Watch limit of {self.watch_limit} reached, but LRU cache is empty. Cannot schedule new watch for {safe_path_handling(path)}.")
                    return

            try:
                self._ensure_inotify()
                self.inotify.add_watch(safe_path_encode(path))
                self.lru_cache.put(path, WatchEntry(timestamp_to_use))
            except OSError as e:
                # Catch ENOENT (2) - File not found, likely deleted before we could watch it
                # Catch ENOTDIR (20) - Not a directory (can happen if a file replaced a dir)
                # Catch EACCES (13) - Permission denied
                # Catch EINVAL (22) - Invalid argument, which can happen with special filesystems or very long paths
                if e.errno == 2: # ENOENT
                    if os.path.exists(path):
                        # Path exists, but inotify reports ENOENT. This is problematic for inotify.
                        logger.warning(f"[fs] Could not schedule watch for {safe_path_handling(path)} (errno={e.errno}), path exists but inotify rejected it. (Consider renaming if possible).")
                    else:
                        # Path truly does not exist.
                        logger.warning(f"[fs] Could not schedule watch for {safe_path_handling(path)} (errno={e.errno}), it may strictly no longer exist or be inaccessible.")
                    return
                if e.errno in (20, 13): # ENOTDIR, EACCES
                     logger.warning(f"[fs] Could not schedule watch for {safe_path_handling(path)} (errno={e.errno}), it may strictly no longer exist or be inaccessible.")
                     return
                if e.errno == 22: # EINVAL - Invalid argument
                    logger.warning(f"[fs] Could not schedule watch for {safe_path_handling(path)} (errno={e.errno}), invalid argument. This can happen with special filesystems, bind mounts, or unusual path characters.")
                    return

                if e.errno == 28:
                    new_limit = len(self.lru_cache)
                    relative_age_days = (self._get_current_time() - timestamp_to_use) / 86400
                    if relative_age_days < self.min_monitoring_window_days:
                        error_msg = (
                            f"System inotify watch limit hit. The new watch for {path} "
                            f"(relative age: {relative_age_days:.2f} days) is about to be skipped. "
                            f"This is below the configured min_monitoring_window_days ({self.min_monitoring_window_days} days). "
                            f"Stopping driver to prevent data loss."
                        )
                        logger.error(error_msg)
                        if self.stop_driver_event:
                            self.stop_driver_event.set()
                        raise DriverError(error_msg)

                    logger.warning(
                        f"System inotify watch limit hit. Dynamically adjusting watch_limit from "
                        f"{self.watch_limit} to {new_limit}. The new watch for {path} (relative age: {relative_age_days:.2f} days) will be skipped. "

                        f"Consider increasing 'fs.inotify.max_user_watchs'."
                    )
                    self.watch_limit = new_limit
                    return self.schedule(path, timestamp_to_use) # Retry the schedule call after adjusting the limit
                else:
                    logger.error(f"OS error scheduling watch for {safe_path_handling(path)}: {e}", exc_info=True)
                    raise

    def unschedule_recursive(self, path: str):
        with self._lock:
            paths_to_remove_from_lru = [p for p in list(self.lru_cache.cache.keys()) if p == path or p.startswith(path + os.sep)]
            for p in paths_to_remove_from_lru:
                try:
                    self.inotify.remove_watch(safe_path_encode(p))
                except (KeyError, OSError) as e:
                    logger.warning(f"Error removing watch during recursive unschedule for {p}: {e}")
                self.lru_cache.remove(p)

    def touch(self, path: str, timestamp: Optional[float] = None, is_recursive_upward: bool = True):
        with self._lock:
            current_path = path
            while True:
                try:
                    if os.path.isdir(current_path):
                        self.schedule(current_path, timestamp)
                except (OSError, PermissionError) as e:
                    logger.warning(f"[fs] Error accessing path during touch: {safe_path_handling(current_path)} - {str(e)}")
                
                if not is_recursive_upward or len(current_path) <= len(self.root_path):
                    break
                
                current_path = os.path.dirname(current_path)

    def start(self):
        with self._lock:
            if self.inotify_thread and self.inotify_thread.is_alive():
                logger.warning("WatchManager: already running")
                return

            logger.info(f"WatchManager: Starting inotify for root path {self.root_path}")
            
            # Ensure inotify is valid (recreate if stopped)
            if self._stop_event.is_set():
                self.inotify = None
                self._ensure_inotify()
                
                # Re-add existing watches from LRU cache if we had to recreate inotify
                for path in self.lru_cache.cache.keys():
                    try:
                        self.inotify.add_watch(safe_path_encode(path))
                    except OSError as e:
                        logger.warning(f"Failed to re-add watch for {path}: {e}")

            self._stop_event.clear()
            self.inotify_thread = threading.Thread(target=self._event_processing_loop, daemon=True)
            self.inotify_thread.start()
            logger.info("WatchManager: Inotify event thread started.")

    def stop(self):
        with self._lock:
            logger.info("WatchManager: Stopping inotify event thread.")
            self._stop_event.set()
            # We don't join here to avoid potential deadlock with the event processing loop 
            # if it's waiting for a lock we're holding.
            # The daemon status ensures it will be cleaned up if the main process exits.
        self.inotify.close()  # This will interrupt the blocking read_events() call
        if self.inotify_thread.is_alive():
            self.inotify_thread.join(timeout=5.0)
        logger.info("WatchManager: Inotify event thread stopped.")