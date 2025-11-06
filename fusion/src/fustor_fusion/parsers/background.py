"""
Background processing utilities for the parsers module.
Handles processing of events in the background, tracking consumption positions.
"""
import asyncio
import logging
from typing import Dict, Any, Optional
from sqlalchemy.ext.asyncio import AsyncSession
from .main import get_parser_with_db
from ..ingestor.queue_integration import get_events_from_queue
from datetime import datetime


logger = logging.getLogger(__name__)


def _extract_timestamp(event_content: Dict[str, Any]) -> Optional[datetime]:
    """Extracts and safely parses a timestamp from an event."""
    # Adapt this part based on the actual structure of your events
    ts = event_content.get('modified_time') or event_content.get('created_time')
    if ts:
        try:
            # Handle Unix timestamps (float/int)
            if isinstance(ts, (int, float)):
                return datetime.fromtimestamp(ts)
            # Handle ISO 8601 format strings
            elif isinstance(ts, str):
                return datetime.fromisoformat(ts.replace('Z', '+00:00'))
        except (ValueError, TypeError):
            logger.warning(f"Could not parse timestamp: {ts}")
    return None


class BackgroundTaskStatus:
    """Tracks the status of background parsing tasks"""
    def __init__(self):
        self.status = {}
    
    def update_status(self, datastore_id: int, task_name: str, status: str, details: Optional[Dict[str, Any]] = None):
        """Update status for a specific task and datastore"""
        if datastore_id not in self.status:
            self.status[datastore_id] = {}
        
        self.status[datastore_id][task_name] = {
            'status': status,
            'last_updated': asyncio.get_event_loop().time(),
            'details': details or {}
        }
    
    def get_status(self, datastore_id: int, task_name: str = None):
        """Get status for a task or all tasks for a datastore"""
        if datastore_id not in self.status:
            return None
        
        if task_name:
            return self.status[datastore_id].get(task_name)
        else:
            return self.status[datastore_id]
    
    def get_all_status(self):
        """Get status for all datastores and tasks"""
        return self.status


# Global instance to track background task status
task_status = BackgroundTaskStatus()


async def get_background_task_status(datastore_id: int = None, task_name: str = None):
    """
    Get the status of background parsing tasks.
    
    Args:
        datastore_id: Specific datastore ID to query (None for all)
        task_name: Specific task name to query (None for all tasks for the datastore)
    
    Returns:
        Status information for the requested tasks
    """
    if datastore_id is not None:
        return task_status.get_status(datastore_id, task_name)
    else:
        return task_status.get_all_status()


async def process_events_batch(
    datastore_id: int,
    db_session: AsyncSession,
    batch_size: int = 500  # Default batch size
) -> Dict[str, Any]:
    """
    Process a batch of events from the in-memory queue.
    
    Args:
        datastore_id: ID of the datastore to process
        db_session: The SQLAlchemy async session to use
        batch_size: Number of events to process in this batch
        
    Returns:
        Dictionary with processing results
    """
    import sys
    task_name = f"process_events_batch_{datastore_id}"
    
    # Log immediately when function is called
    logger.debug(f"process_events_batch called for datastore {datastore_id}")
    
    try:
        # Update status to indicate processing started
        task_status.update_status(datastore_id, task_name, "PROCESSING", {
            "message": "Starting event batch processing",
            "datastore_id": datastore_id
        })
        
        logger.info(f"Starting event batch processing for datastore {datastore_id}")
        
        # Get events from the in-memory queue
        queued_events = await get_events_from_queue(datastore_id, batch_size)
        
        if not queued_events:
            logger.debug(f"No new events to process for datastore {datastore_id}")
            return {"processed_count": 0, "error_count": 0, "total_events_in_query": 0}
        
        logger.info(f"Retrieved {len(queued_events)} events from queue for datastore {datastore_id}")
        
        # Sort the current batch by timestamp
        sorted_events = sorted(
            queued_events,
            key=lambda e: _extract_timestamp(e.content) or datetime.min
        )

        processed_count = 0
        error_count = 0
        parser_manager = await get_parser_with_db(db_session, datastore_id)

        for event in sorted_events:
            try:
                await parser_manager.process_event(event.content)
                processed_count += 1
            except Exception as e:
                logger.error(f"Error applying event from queue: {e}", exc_info=True)
                error_count += 1
                # Continue processing other events in the batch despite this error

        logger.info(f"Completed event batch processing for datastore {datastore_id}: "
                   f"processed={processed_count}, errors={error_count}")
        
        # Update status with results
        task_status.update_status(datastore_id, task_name, "COMPLETED", {
            "message": f"Successfully processed batch for datastore {datastore_id}",
            "processed_count": processed_count,
            "error_count": error_count,
            "total_events_in_query": len(queued_events)
        })
        
        return {
            "processed_count": processed_count,
            "error_count": error_count,
            "total_events_in_query": len(queued_events)
        }
    
    except Exception as e:
        error_msg = f"Error processing event batch for datastore {datastore_id}: {e}"
        logger.error(error_msg, exc_info=True)
        
        # Update status to indicate error
        task_status.update_status(datastore_id, task_name, "ERROR", {
            "message": str(e),
            "datastore_id": datastore_id
        })
        
        result = {
            "processed_count": 0,
            "error_count": 1,
            "total_events_in_query": 0,
            "error": str(e)
        }
        
        return result