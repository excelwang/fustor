"""
Fuagent source driver for the file system.

This driver implements a 'Smart Dynamic Monitoring' strategy to efficiently
monitor large directory structures without exhausting system resources.
"""
import os
import queue
import time
import datetime
import logging
import uuid
import getpass
import fnmatch
import threading
import multiprocessing
from typing import Any, Dict, Iterator, List, Tuple, Optional, Set
from concurrent.futures import ThreadPoolExecutor
from fustor_core.drivers import SourceDriver
from fustor_core.models.config import SourceConfig
from fustor_core.event import EventBase, UpdateEvent, DeleteEvent, MessageSource

from .components import _WatchManager, safe_path_handling
from .event_handler import OptimizedWatchEventHandler, get_file_metadata, _get_relative_path
from .scanner import FSScanner

logger = logging.getLogger("fustor_agent.driver.fs")
            
class FSDriver(SourceDriver):
    _instances: Dict[str, 'FSDriver'] = {}
    _lock = threading.Lock()
    
    # Schema metadata
    schema_name = "fs"
    
    # FS driver doesn't require discovery as fields are fixed metadata.
    require_schema_discovery = False

    @property
    def is_transient(self) -> bool:
        """
        FS driver is transient - events will be lost if not processed immediately.
        """
        return True
    
    def __new__(cls, id: str, config: SourceConfig):
        # Generate unique signature based on URI and credentials to ensure permission isolation
        signature = f"{config.uri}#{hash(str(config.credential))}"
        
        with FSDriver._lock:
            if signature not in FSDriver._instances:
                # Create new instance
                instance = super().__new__(cls)
                FSDriver._instances[signature] = instance
            return FSDriver._instances[signature]
    
    def __init__(self, id: str, config: SourceConfig):
        # Prevent re-initialization of shared instances
        if hasattr(self, '_initialized'):
            return
        
        super().__init__(id, config)
        self.uri = self.config.uri
        self.event_queue: queue.Queue[EventBase] = queue.Queue()
        self.drift_from_nfs = 0.0
        self._stop_driver_event = threading.Event()
        
        min_monitoring_window_days = self.config.driver_params.get("min_monitoring_window_days", 30.0)
        throttle_interval = self.config.driver_params.get("throttle_interval_sec", 5.0)
        self.watch_manager = _WatchManager(
            self.uri, 
            event_handler=None, 
            min_monitoring_window_days=min_monitoring_window_days, 
            stop_driver_event=self._stop_driver_event, 
            throttle_interval=throttle_interval
        )
        self.event_handler = OptimizedWatchEventHandler(
            self.event_queue, 
            self.watch_manager,
            event_schema=self.schema_name
        )
        self.watch_manager.event_handler = self.event_handler
        self._pre_scan_completed = False
        self._pre_scan_lock = threading.Lock()
        
        # Thread pool for parallel scanning
        # Issue 7 Fix: Use a more conservative default (min(4, cpu_count)) to prevent NFS IOPS spikes.
        default_workers = min(4, multiprocessing.cpu_count())
        max_workers = self.config.driver_params.get("max_scan_workers", default_workers)
        self._executor = ThreadPoolExecutor(max_workers=max_workers)
        logger.debug(f"[fs] Driver initialized with {max_workers} scan workers (default was {default_workers}).")
        
        self._initialized = True

    def _perform_pre_scan_and_schedule(self):
        """
        Performs a one-time scan of the directory to populate the watch manager
        with a capacity-aware, hierarchy-complete set of the most active directories.
        """
        with self._pre_scan_lock:
            if self._pre_scan_completed:
                return

            logger.debug(f"[fs] Performing initial parallel directory scan for: {self.uri}")
            
            dir_mtime_map: Dict[str, float] = {}
            dir_children_map: Dict[str, List[str]] = {}
            total_entries = 0
            scanner = FSScanner(self.uri, num_workers=self._executor._max_workers)
            
            # Execute pre-scan
            dir_mtime_map, dir_children_map, total_entries, error_count = scanner.perform_pre_scan()

            # Bottom-up pass to calculate recursive subtree mtimes
            logger.info(f"[fs] Parallel scan finished. Calculating recursive mtimes for {len(dir_mtime_map)} directories...")
            all_dirs = sorted(dir_mtime_map.keys(), key=lambda x: x.count(os.sep), reverse=True)
            
            for path in all_dirs:
                latest = dir_mtime_map[path]
                for subdir in dir_children_map.get(path, []):
                    latest = max(latest, dir_mtime_map.get(subdir, 0))
                dir_mtime_map[path] = latest
            
            # Establish Shadow Reference Frame for Watch Scheduling
            if dir_mtime_map:
                mtimes = sorted(dir_mtime_map.values())
                # Fix: Use int(len * 0.99) to ensure it works for small sample sizes.
                # N=1 -> idx=0 (max)
                # N=100 -> idx=99 (max)
                # N=101 -> idx=99 (filters 1)
                # This ensures we filter out the top 1% of outliers.
                p99_idx = max(0, int(len(mtimes) * 0.99) - 1)
                latest_mtime_stable = mtimes[p99_idx]
                
                self.drift_from_nfs = latest_mtime_stable - time.time()
                self.watch_manager.drift_from_nfs = self.drift_from_nfs
                drift_from_nfs = self.drift_from_nfs
                
                root_recursive_mtime = dir_mtime_map.get(self.uri, 0.0)
                newest_relative_age = latest_mtime_stable - root_recursive_mtime
                
                logger.info(
                    "[fs] Pre-scan completed: processed %d entries, "
                    "errors: %d, newest_relative_age: %.2f days, drift: %.2fs",
                    total_entries, error_count, newest_relative_age/86400, drift_from_nfs
                )

            logger.info(f"[fs] Found {len(dir_mtime_map)} total directories. Building capacity-aware, hierarchy-complete watch set...")
            sorted_dirs = sorted(dir_mtime_map.items(), key=lambda item: item[1], reverse=True)[:self.watch_manager.watch_limit]
            drift_from_nfs = getattr(self, 'drift_from_nfs', 0.0)
            old_limit = self.watch_manager.watch_limit

            for path, server_mtime in sorted_dirs:
                lru_timestamp = server_mtime - drift_from_nfs
                self.watch_manager.schedule(path, lru_timestamp)
                if self.watch_manager.watch_limit < old_limit:
                    break
            
            logger.info(f"[fs] Final watch set constructed. Total paths to watch: {len(self.watch_manager.lru_cache)}.")
            self._pre_scan_completed = True


    def get_snapshot_iterator(self, **kwargs) -> Iterator[EventBase]:
        """
        Parallel Snapshot Sync Phase.
        """
        stream_id = f"snapshot-fs-{uuid.uuid4().hex[:6]}"
        logger.info(f"[{stream_id}] Starting Parallel Snapshot Scan: {self.uri}")

        driver_params = self.config.driver_params
        if driver_params.get("startup_mode") == "message-only":
            return
            
        file_pattern = driver_params.get("file_pattern", "*")
        batch_size = kwargs.get("batch_size", 100)

        scanner = FSScanner(self.uri, num_workers=self._executor._max_workers, 
                           file_pattern=file_pattern, drift_from_nfs=self.drift_from_nfs)

        def touch_callback(path, relative_mtime):
            self.watch_manager.touch(path, relative_mtime, is_recursive_upward=False)

        yield from scanner.scan_snapshot(self.schema_name, batch_size=batch_size, callback=touch_callback)
        
        logger.info(f"[{stream_id}] Snapshot scan completed.")

    def get_message_iterator(self, start_position: int=-1, **kwargs) -> Iterator[EventBase]:
        
        # Perform pre-scan to populate watches before starting the observer.
        # This is essential for the message-first architecture and must block
        # until completion to prevent race conditions downstream.
        
        self._perform_pre_scan_and_schedule()

        def _iterator_func() -> Iterator[EventBase]:
            # After pre-scan is complete, any new events should be considered "starting from now"
            # If start_position is provided, use it; otherwise, start from current time
            
            stream_id = f"message-fs-{uuid.uuid4().hex[:6]}"
            
            stop_event = kwargs.get("stop_event")
            self.watch_manager.start()
            logger.info(f"[{stream_id}] WatchManager started.")

            try:
                # Process events normally, but use the effective start position
                while not (stop_event and stop_event.is_set()):
                    try:
                        max_sync_delay_seconds = self.config.driver_params.get("max_sync_delay_seconds", 1.0)
                        event = self.event_queue.get(timeout=max_sync_delay_seconds)
                        
                        if start_position!=-1 and event.index < start_position:
                            logger.debug(f"[{stream_id}] Skipping old event: {event.event_type} index={event.index} < start_position={start_position}")
                            continue
                        
                        yield event

                    except queue.Empty:
                        continue
            finally:
                self.watch_manager.stop()
                logger.info(f"[{stream_id}] Stopped real-time monitoring for: {self.uri}")

        return _iterator_func()

    def get_audit_iterator(self, mtime_cache: Dict[str, float] = None, **kwargs) -> Iterator[Tuple[Optional[EventBase], Dict[str, float]]]:
        """
        Parallel Audit Sync: Uses a task queue to scan subtrees in parallel.
        Correctly implements mtime-based 'True Silence'.
        """
        stream_id = f"audit-fs-{uuid.uuid4().hex[:6]}"
        logger.info(f"[{stream_id}] Starting Parallel Audit Scan: {self.uri}")
        
        mtime_cache = mtime_cache or {}
        batch_size = kwargs.get("batch_size", 100)
        file_pattern = self.config.driver_params.get("file_pattern", "*")

        scanner = FSScanner(self.uri, num_workers=self._executor._max_workers, file_pattern=file_pattern)
        yield from scanner.scan_audit(self.schema_name, mtime_cache or {}, batch_size=batch_size)
        logger.info(f"[{stream_id}] Audit scan completed.")

    def scan_path(self, path: str, recursive: bool = True) -> Iterator[EventBase]:
        """
        On-demand scan of a specific path.
        Returns an iterator of events.
        """
        stream_id = f"od-scan-{uuid.uuid4().hex[:6]}"
        logger.info(f"[{stream_id}] Starting On-Demand Scan: {path} (recursive={recursive})")
        
        # Ensure path is within the source URI
        if not path.startswith(self.uri):
            # Try joining, assuming relative path
            full_path = os.path.join(self.uri, path.lstrip("/"))
            if not full_path.startswith(self.uri):
                logger.error(f"[{stream_id}] Path {path} is outside of source URI {self.uri}")
                return iter([])
            path = full_path

        # Configure scanner
        # Use Fewer workers for on-demand scan to avoid impact
        num_workers = max(1, self._executor._max_workers // 2)
        
        # We use a custom file pattern if needed, but default to driver config
        file_pattern = self.config.driver_params.get("file_pattern", "*")
        
        scanner = FSScanner(self.uri, num_workers=num_workers, 
                           file_pattern=file_pattern, drift_from_nfs=self.drift_from_nfs)

        def touch_callback(p, relative_mtime):
            # Update watch manager to keep it fresh
            self.watch_manager.touch(p, relative_mtime, is_recursive_upward=False)

        # Handle single file case or directory
        logger.debug(f"[{stream_id}] Checking path on disk: {path}")
        if os.path.isfile(path):
            logger.debug(f"[{stream_id}] Path is a file, yielding single event")
            try:
                stat = os.stat(path)
                meta = get_file_metadata(path, root_path=self.uri, stat_info=stat)
                relative_path = _get_relative_path(path, self.uri)
                self.watch_manager.touch(path, stat.st_mtime - self.drift_from_nfs, is_recursive_upward=False)
                yield UpdateEvent(
                    event_schema=self.schema_name,
                    table="files",
                    rows=[meta],
                    fields=list(meta.keys()),
                    message_source=MessageSource.ON_DEMAND_JOB,
                    index=int((time.time() + self.drift_from_nfs) * 1000)
                )
            except Exception as e:
                logger.warning(f"[{stream_id}] Failed to scan single file {path}: {e}")
            return
        
        if not os.path.exists(path):
            logger.warning(f"[{stream_id}] Path does not exist on disk: {path}")
            return

        if recursive:
            yield from scanner.scan_snapshot(self.schema_name, batch_size=100, callback=touch_callback, initial_path=path, message_source=MessageSource.ON_DEMAND_JOB)
        else:
            # Non-recursive: only return directory metadata itself
            try:
                stat = os.stat(path)
                meta = get_file_metadata(path, root_path=self.uri, stat_info=stat)
                yield UpdateEvent(
                    event_schema=self.schema_name,
                    table="files",
                    rows=[meta],
                    fields=list(meta.keys()),
                    message_source=MessageSource.ON_DEMAND_JOB,
                    index=int((time.time() + self.drift_from_nfs) * 1000)
                )
            except Exception as e:
                logger.error(f"[{stream_id}] Error scanning path: {e}")
        
        logger.info(f"[{stream_id}] On-Demand scan completed.")

    def perform_sentinel_check(self, task_batch: Dict[str, Any]) -> Dict[str, Any]:
        """
        Implements generic sentinel check.
        Supported types: 'suspect_check'
        """
        task_type = task_batch.get('type')
        if task_type == 'suspect_check':
             paths = task_batch.get('paths', [])
             results = self.verify_files(paths)
             return {'type': 'suspect_update', 'updates': results}
        return {}

    def verify_files(self, paths: List[str]) -> List[Dict[str, Any]]:
        """
        Verifies the existence and mtime of the given file paths.
        Used for Sentinel Sweep.
        """
        def verify_single(rel_path):
            full_path = os.path.join(self.uri, rel_path.lstrip("/"))
            try:
                stat_info = os.stat(full_path)
                return {
                    "path": rel_path,
                    "mtime": stat_info.st_mtime,
                    "size": stat_info.st_size,
                    "status": "exists"
                }
            except FileNotFoundError:
                return {
                    "path": rel_path,
                    "mtime": 0.0,
                    "status": "missing"
                }
            except Exception as e:
                logger.warning(f"[fs] Error verifying file {full_path} (rel: {rel_path}): {e}")
                return None

        # Process in parallel using the driver's executor
        results = list(self._executor.map(verify_single, paths))
        # Filter out failed ones
        return [r for r in results if r is not None]

    @classmethod
    async def get_available_fields(cls, **kwargs) -> Dict[str, Any]:
        return {"properties": {
            "path": {"type": "string", "description": "The full, absolute path to the file.", "column_index": 0},
            "size": {"type": "integer", "description": "The size of the file in bytes.", "column_index": 1},
            "modified_time": {"type": "number", "description": "The last modification time as a Unix timestamp (float).", "column_index": 2},
            "created_time": {"type": "number", "description": "The creation time as a Unix timestamp (float).", "column_index": 3},
            "is_dir": {"type": "boolean", "description": "True if the path is a directory.", "column_index": 4},
        }}

    @classmethod
    async def test_connection(cls, **kwargs) -> Tuple[bool, str]:
        path = kwargs.get("uri")
        if not path or not isinstance(path, str):
            return (False, "路径未提供或格式不正确。")
        if not os.path.exists(path):
            return (False, f"路径不存在: {path}")
        if not os.path.isdir(path):
            return (False, f"路径不是一个目录: {path}")
        if not os.access(path, os.R_OK):
            return (False, f"没有读取权限: {path}")
        return (True, "连接成功，路径有效且可读。")

    @classmethod
    async def check_privileges(cls, **kwargs) -> Tuple[bool, str]:
        path = kwargs.get("uri")
        if not path:
            return (False, "Path not provided in arguments.")

        try:
            user = getpass.getuser()
        except Exception:
            user = "unknown"

        logger.info(f"[fs] Checking permissions for user '{user}' on path: {safe_path_handling(path)}")
        
        if not os.path.exists(path):
            return (False, f"路径不存在: {path}")
        if not os.path.isdir(path):
            return (False, f"路径不是一个目录: {path}")

        can_read = os.access(path, os.R_OK)
        can_execute = os.access(path, os.X_OK)

        if can_read and can_execute:
            return (True, f"权限充足：当前用户 '{user}' 可以监控该目录。")
        
        missing_perms = []
        if not can_read:
            missing_perms.append("读取")
        if not can_execute:
            missing_perms.append("执行(进入)")
        
        return (False, f"权限不足：当前用户 '{user}' 缺少 {' 和 '.join(missing_perms)} 权限。")

    async def close(self):
        """
        Close the file system watcher, stop monitoring, and remove from singleton cache.
        After close(), creating a new FSDriver with the same URI will produce a fresh instance.
        """
        logger.info(f"[fs] Closing file system watcher for {self.uri}")
        
        # Stop the watch manager if it's running
        if hasattr(self, 'watch_manager') and self.watch_manager:
            self.watch_manager.stop()
        
        # Set the stop event to ensure any active monitoring stops
        if hasattr(self, '_stop_driver_event') and self._stop_driver_event:
            self._stop_driver_event.set()
        
        # Shutdown thread pool
        if hasattr(self, '_executor') and self._executor:
            self._executor.shutdown(wait=False)
        
        # Remove from singleton cache so next creation gets a fresh instance
        with FSDriver._lock:
            signature = f"{self.config.uri}#{hash(str(self.config.credential))}"
            FSDriver._instances.pop(signature, None)
        
        # Clear initialized flag
        if hasattr(self, '_initialized'):
            del self._initialized
        
        logger.info(f"[fs] Closed file system watcher for {self.uri}")

    @classmethod
    def invalidate(cls, uri: str, credential: str = "") -> bool:
        """
        Remove a cached instance by URI+credential signature.
        Returns True if an instance was removed.
        """
        signature = f"{uri}#{hash(str(credential))}"
        with cls._lock:
            return cls._instances.pop(signature, None) is not None

