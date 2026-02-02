"""
Service module for the view management functionality.
Provides methods for view-related status operations.
"""
from typing import Dict, Any, Optional
import logging

logger = logging.getLogger(__name__)

async def get_view_status(view_id: str) -> Dict[str, Any]:
    """
    Get the current status of all views for a specific view ID.
    """
    from .manager import get_cached_view_manager

    try:
        # Get stats from the consistent view manager
        manager = await get_cached_view_manager(view_id)
        stats = await manager.get_aggregated_stats()
        
        return {
            "view_id": view_id,
            "status": "active",
            "total_items": stats.get("total_volume", 0),
            "memory_stats": stats
        }

    except Exception as e:
        logger.error(f"Error getting view status for {view_id}: {e}", exc_info=True)
        return {
            "view_id": view_id,
            "status": "error",
            "error": str(e)
        }