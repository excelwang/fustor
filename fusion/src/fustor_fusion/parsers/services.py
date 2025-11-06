"""
Service module for the parsers functionality.
Provides methods for parser-related operations.
"""
from typing import Dict, Any, Optional
import logging

logger = logging.getLogger(__name__)

async def get_parser_status(datastore_id: int) -> Dict[str, Any]:
    """
    Get the current status of all parsers for a specific datastore.
    """
    # Return simplified status without database access
    from .manager import get_cached_parser_manager, get_directory_stats

    try:
        # Try to get stats from the memory-based parser
        stats = await get_directory_stats(datastore_id=datastore_id)
        return {
            "datastore_id": datastore_id,
            "status": "active",
            "total_directory_entries": stats.get("total_files", 0) + stats.get("total_directories", 0) if stats else 0,
            "memory_stats": stats
        }
    except Exception as e:
        logger.error(f"Error getting parser status for datastore {datastore_id}: {e}", exc_info=True)
        return {
            "datastore_id": datastore_id,
            "status": "error",
            "error": str(e)
        }