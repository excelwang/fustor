import os
import queue
import time
import logging
import stat
from typing import Any, Dict, Optional

from watchdog.events import FileSystemEventHandler, FileSystemEvent
from fustor_event_model.models import UpdateEvent, DeleteEvent

from .components import _WatchManager

logger = logging.getLogger("fustor_agent.driver.fs")

def get_file_metadata(path: str, stat_info: Optional[os.stat_result] = None) -> Optional[Dict[str, Any]]:
    """Get file metadata, returning mtime and ctime as float timestamps."""
    try:
        if stat_info is None:
            stat_info = os.stat(path)
        
        is_dir = stat.S_ISDIR(stat_info.st_mode)
        
        return {
            "file_path": path,
            "size": stat_info.st_size,
            "modified_time": stat_info.st_mtime,
            "created_time": stat_info.st_ctime,
            "is_dir": is_dir
        }
    except FileNotFoundError:
        logger.warning(f"[fs] Could not stat file, it may have been deleted before processing: {path}")
        return None


class OptimizedWatchEventHandler(FileSystemEventHandler):
    """
    Event handler that processes watchdog events immediately using dedicated
    on_* methods, which is the idiomatic way to use watchdog.
    """
    def __init__(self, event_queue: queue.Queue, watch_manager: _WatchManager, logical_clock=None):
        super().__init__()
        self.event_queue = event_queue
        self.watch_manager = watch_manager
        self.logical_clock = logical_clock
        # Path -> last_sent_time mapping to throttle on_modified for files
        self.last_modified_sent: Dict[str, float] = {}
        self.throttle_interval = float(getattr(watch_manager, 'throttle_interval', 5.0))

    def _get_index(self, mtime=None):
        if self.logical_clock:
            if mtime is not None:
                 # Problem 2 Fix: Strictly advance local logical clock with observed mtime
                 self.logical_clock.update(float(mtime))
            
            # Index is ALWAYS the logic watermark * 1000 (ms)
            return int(self.logical_clock.get_watermark() * 1000)
        
        # Fallback if no logical clock provided (should not happen in production)
        if mtime is not None:
            return int(mtime * 1000)
        return int(time.time() * 1000)

    def _touch_recursive_bottom_up(self, path: str):
        """Recursively touches a directory and its contents from bottom-up."""
        if not os.path.exists(path): return

        # First, touch all files and subdirectories
        for dirpath, dirnames, _ in os.walk(path, topdown=False):
            for dirname in dirnames:
                subdir_path = os.path.join(dirpath, dirname)
                self.watch_manager.touch(subdir_path, is_recursive_upward=False)
        
        # Finally, touch the root of the path itself
        self.watch_manager.touch(path, is_recursive_upward=False)

    def _generate_move_events_recursive(self, from_path: str, to_path: str):
        """Generates DeleteEvents for inferred old paths and UpdateEvents for new paths within a moved subtree."""
        if not os.path.exists(to_path): return

        for dirpath, dirnames, filenames in os.walk(to_path, topdown=False):
            for filename in filenames:
                add_path = os.path.join(dirpath, filename)
                del_path = add_path.replace(to_path, from_path, 1)
                
                # Generate DeleteEvent for the old path
                row = {"file_path": del_path}
                delete_event = DeleteEvent(
                    event_schema=self.watch_manager.root_path,
                    table="files",
                    rows=[row],
                    fields=list(row.keys()),
                    index=self._get_index()
                )
                self.event_queue.put(delete_event)
                
                # Generate UpdateEvent for the new path
                metadata = get_file_metadata(add_path)
                if metadata:
                    update_event = UpdateEvent(
                        event_schema=self.watch_manager.root_path,
                        table="files",
                        rows=[metadata],
                        fields=list(metadata.keys()),
                        index=self._get_index(mtime=metadata['modified_time'])
                    )
                    self.event_queue.put(update_event)
            
            for dirname in dirnames:
                subdir_add_path = os.path.join(dirpath, dirname)
                subdir_del_path = subdir_add_path.replace(to_path, from_path, 1)

                # Generate DeleteEvent for the old directory path
                row = {"file_path": subdir_del_path}
                delete_event = DeleteEvent(
                    event_schema=self.watch_manager.root_path,
                    table="files",
                    rows=[row],
                    fields=list(row.keys()),
                    index=self._get_index()
                )
                self.event_queue.put(delete_event)
                # Generate UpdateEvent for the new path
                metadata = get_file_metadata(subdir_add_path)
                if metadata:
                    update_event = UpdateEvent(
                        event_schema=self.watch_manager.root_path,
                        table="files",
                        rows=[metadata],
                        fields=list(metadata.keys()),
                        index=self._get_index(mtime=metadata['modified_time'])
                    )
                    self.event_queue.put(update_event)

    def on_created(self, event: FileSystemEvent):
        """Called when a file or directory is created."""
        try:
            if event.is_directory:
                metadata = get_file_metadata(event.src_path)
                if metadata:
                    update_event = UpdateEvent(
                        event_schema=self.watch_manager.root_path,
                        table="files",
                        rows=[metadata],
                        fields=list(metadata.keys()),
                        index=self._get_index(mtime=metadata['modified_time'])
                    )
                    self.event_queue.put(update_event)
                self.watch_manager.touch(event.src_path)
        except Exception as e:
            logger.warning(f"[fs] Error processing file creation event for {event.src_path}: {str(e)}")

    def on_deleted(self, event: FileSystemEvent):
        """Called when a file or directory is deleted."""
        try:
            # For a deleted path, we should not attempt to touch/schedule a watch.
            # Instead, we unschedule and generate a delete event.

            if event.is_directory:
                self.watch_manager.unschedule_recursive(event.src_path)
            row = {"file_path": event.src_path}
            delete_event = DeleteEvent(
                event_schema=self.watch_manager.root_path,
                table="files",
                rows=[row],
                fields=list(row.keys()),
                index=self._get_index()
            )
            self.event_queue.put(delete_event)
            
            # A deletion is an activity, touch the parent path to update its timestamp.
            # We assume the parent is always a directory.
            self.watch_manager.touch(os.path.dirname(event.src_path))
            
            # Clean up throttle cache
            self.last_modified_sent.pop(event.src_path, None)
        except Exception as e:
            logger.warning(f"[fs] Error processing file deletion event for {event.src_path}: {str(e)}")

    def on_moved(self, event: FileSystemEvent):
        """Called when a file or a directory is moved or renamed."""
        try:
            # Touch the parent of the source path to update its timestamp (something disappeared).
            self.watch_manager.touch(os.path.dirname(event.src_path))
            # Touch the parent of the destination path to update its timestamp (something appeared).
            self.watch_manager.touch(os.path.dirname(event.dest_path))
            
            # Create and queue the delete event for the old location
            delete_row = {"file_path": event.src_path}
            delete_event = DeleteEvent(
                event_schema=self.watch_manager.root_path,
                table="files",
                rows=[delete_row],
                fields=list(delete_row.keys()),
                index=self._get_index()
            )
            self.event_queue.put(delete_event)
            
            # Handle the creation/update event for the new location
            if event.is_directory:
                # For directories, process recursively
                self._generate_move_events_recursive(event.src_path, event.dest_path)
                # Recursively touch all contents at the new destination to ensure watches are updated/scheduled.
                self._touch_recursive_bottom_up(event.dest_path)
                # Unschedule the old path recursively
                self.watch_manager.unschedule_recursive(event.src_path)
            else:
                # For files, create update event for new location
                metadata = get_file_metadata(event.dest_path)
                if metadata:
                    update_event = UpdateEvent(
                        event_schema=self.watch_manager.root_path,
                        table="files",
                        rows=[metadata],
                        fields=list(metadata.keys()),
                        index=self._get_index(mtime=metadata['modified_time'])
                    )
                    self.event_queue.put(update_event)
                # Touch the file itself at its new destination
                self.watch_manager.touch(event.dest_path)
                
                # Clean up throttle cache for old path
                self.last_modified_sent.pop(event.src_path, None)
        except Exception as e:
            logger.warning(f"[fs] Error processing file move event for {event.src_path} -> {event.dest_path}: {str(e)}")
            # Note: If we get here, the delete_event may already be in the queue
            # This is an inherent issue with partial failure in distributed systems,
            # but we prevent the entire system from crashing

    def on_modified(self, event: FileSystemEvent):
        """
        Called when a file or directory is modified.
        """
        try:
            self.watch_manager.touch(event.src_path)
            if not event.is_directory:
                now = time.time()
                last_sent = self.last_modified_sent.get(event.src_path, 0)
                
                if now - last_sent < self.throttle_interval:
                    return # Throttled
                
                metadata = get_file_metadata(event.src_path)
                if metadata:
                    update_event = UpdateEvent(
                        event_schema=self.watch_manager.root_path,
                        table="files",
                        rows=[metadata],
                        fields=list(metadata.keys()),
                        index=self._get_index(mtime=metadata['modified_time'])
                    )
                    self.event_queue.put(update_event)
                    self.last_modified_sent[event.src_path] = now
        except Exception as e:
            logger.warning(f"[fs] Error processing file modification event for {event.src_path}: {str(e)}")

    def on_closed(self, event: FileSystemEvent):
        """
        Called when a file opened for writing is closed.
        """
        try:
            self.watch_manager.touch(event.src_path)
            if not event.is_directory:
                metadata = get_file_metadata(event.src_path)
                if metadata:
                    update_event = UpdateEvent(
                        event_schema=self.watch_manager.root_path,
                        table="files",
                        rows=[metadata],
                        fields=list(metadata.keys()),
                        index=self._get_index(mtime=metadata['modified_time'])
                    )
                    self.event_queue.put(update_event)
                    # Clear throttle cache on close to ensure final state is sent
                    self.last_modified_sent.pop(event.src_path, None)
        except Exception as e:
            logger.warning(f"[fs] Error processing file closed event for {event.src_path}: {str(e)}")