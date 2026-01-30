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
    from .manager import get_cached_view_manager

    try:
        # Get stats from the consistent view manager
        manager = await get_cached_view_manager(datastore_id)
        stats = await manager.get_aggregated_stats()
        
        return {
            "datastore_id": datastore_id,
            "status": "active",
            "total_items": stats.get("total_volume", 0),
            "memory_stats": stats
        }

    except Exception as e:
        logger.error(f"Error getting view status for datastore {datastore_id}: {e}", exc_info=True)
        return {
            "datastore_id": datastore_id,
            "status": "error",
            "error": str(e)
        }