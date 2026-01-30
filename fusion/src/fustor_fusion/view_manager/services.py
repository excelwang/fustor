"""
Service module for the view management functionality.
Provides methods for view-related status operations.
"""
from typing import Dict, Any, Optional
import logging

logger = logging.getLogger(__name__)

async def get_view_status(datastore_id: int) -> Dict[str, Any]:
    """
    Get the current status of all views for a specific datastore.
    """
    from .manager import get_directory_stats

    try:
        # Get stats from the consistent view provider
        stats = await get_directory_stats(datastore_id=datastore_id)
        return {
            "datastore_id": datastore_id,
            "status": "active",
            "total_directory_entries": stats.get("total_files", 0) + stats.get("total_directories", 0) if stats else 0,
            "memory_stats": stats
        }
    except Exception as e:
        logger.error(f"Error getting view status for datastore {datastore_id}: {e}", exc_info=True)
        return {
            "datastore_id": datastore_id,
            "status": "error",
            "error": str(e)
        }