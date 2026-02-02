"""
Background processing utilities for the view manager module.
Handles processing of events in the background to maintain consistent views.
"""
import asyncio
import logging
from typing import Dict, Any, Optional
from .manager import get_cached_view_manager
from ..in_memory_queue import memory_event_queue
from datetime import datetime


logger = logging.getLogger(__name__)


def _extract_timestamp(event_content: Dict[str, Any]) -> Optional[datetime]:
    """Extracts and safely parses a timestamp from an event."""
    ts = event_content.get('modified_time') or event_content.get('created_time')
    if ts:
        try:
            if isinstance(ts, (int, float)):
                return datetime.fromtimestamp(ts)
            elif isinstance(ts, str):
                return datetime.fromisoformat(ts.replace('Z', '+00:00'))
        except (ValueError, TypeError):
            logger.warning(f"Could not parse timestamp: {ts}")
    return None


class BackgroundTaskStatus:
    """Tracks the status of background view processing tasks"""
    def __init__(self):
        self.status = {}
    
    def update_status(self, view_id: int, task_name: str, status: str, details: Optional[Dict[str, Any]] = None):
        """Update status for a specific task and view"""
        if view_id not in self.status:
            self.status[view_id] = {}
        
        self.status[view_id][task_name] = {
            'status': status,
            'last_updated': asyncio.get_event_loop().time(),
            'details': details or {}
        }
    
    def get_status(self, view_id: int, task_name: str = None):
        """Get status for a task or all tasks for a view"""
        if view_id not in self.status:
            return None
        
        if task_name:
            return self.status[view_id].get(task_name)
        else:
            return self.status[view_id]
    
    def get_all_status(self):
        """Get status for all views and tasks"""
        return self.status


# Global instance to track background task status
task_status = BackgroundTaskStatus()


async def get_background_task_status(view_id: int = None, task_name: str = None):
    """
    Get the status of background view tasks.
    """
    if view_id is not None:
        return task_status.get_status(view_id, task_name)
    else:
        return task_status.get_all_status()


async def process_view_events_loop(view_id: int):
    """
    Background loop to process events for a specific view.
    This is called by the main processing loop in Fusion.
    """
    # Note: The actual batching/polling logic is now in main.py
    # and calls view_manager.process_event individually.
    pass