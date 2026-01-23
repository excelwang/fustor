"""
API endpoints for the parsers module.
Provides REST endpoints to access parsed data views.
"""
from fastapi import APIRouter, Query, Header, Depends, status, HTTPException
from fastapi.responses import ORJSONResponse
import logging
from typing import Dict, Any, Optional

from ..parsers.manager import get_directory_tree, search_files, get_directory_stats, reset_directory_tree
from ..auth.dependencies import get_datastore_id_from_api_key
from ..datastore_state_manager import datastore_state_manager
from ..in_memory_queue import memory_event_queue
from ..processing_manager import processing_manager

logger = logging.getLogger(__name__)

parser_router = APIRouter(tags=["Parsers - Data Views"])

async def check_snapshot_status(datastore_id: int):
    """Checks if the initial snapshot sync is complete for the datastore."""
    is_signal_complete = await datastore_state_manager.is_snapshot_complete(datastore_id)
    
    # Check queue size AND inflight (currently processing) count
    queue_size = memory_event_queue.get_queue_size(datastore_id)
    inflight_count = processing_manager.get_inflight_count(datastore_id)
    
    if not is_signal_complete or queue_size > 0 or inflight_count > 0:
        detail = "Initial snapshot sync in progress. Service temporarily unavailable for this datastore."
        if is_signal_complete and (queue_size > 0 or inflight_count > 0):
            detail = f"Sync signal received, but still processing ingested data: queue={queue_size}, inflight={inflight_count}."
            logger.info(f"Datastore {datastore_id} {detail}")
            
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail=detail
        )

@parser_router.get("/fs/tree", summary="Get directory tree structure", response_class=ORJSONResponse)
async def get_directory_tree_api(
    path: str = Query("/", description="Directory path to retrieve (default: '/')"),
    recursive: bool = Query(True, description="Whether to recursively retrieve the entire subtree"),
    max_depth: Optional[int] = Query(None, description="Maximum depth of recursion"),
    only_path: bool = Query(False, description="Return only paths, excluding metadata like size and timestamps"),
    dry_run: bool = Query(False, description="If true, skips processing to measure network/framework latency"),
    datastore_id: int = Depends(get_datastore_id_from_api_key)
) -> Optional[Dict[str, Any]]:
    """Get the directory structure tree starting from the specified path."""
    await check_snapshot_status(datastore_id)
    
    if dry_run:
        return ORJSONResponse(content={"message": "dry-run", "datastore_id": datastore_id})

    effective_recursive = recursive if max_depth is None else True
    logger.debug(f"API request for directory tree: path={path}, recursive={effective_recursive}, max_depth={max_depth}, only_path={only_path}, datastore_id={datastore_id}")
    result = await get_directory_tree(path, datastore_id=datastore_id, recursive=effective_recursive, max_depth=max_depth, only_path=only_path)
    
    if result is None:
        return ORJSONResponse(content={"detail": "Not found"}, status_code=404)
    return ORJSONResponse(content=result)

@parser_router.get("/fs/search", summary="Search for files by pattern")
async def search_files_api(
    pattern: str = Query(..., description="Search pattern to match in file paths"),
    datastore_id: int = Depends(get_datastore_id_from_api_key)
) -> list:
    """Search for files matching the specified pattern."""
    await check_snapshot_status(datastore_id)
    logger.info(f"API request for file search: pattern={pattern}, datastore_id={datastore_id}")
    result = await search_files(pattern, datastore_id=datastore_id)
    logger.info(f"File search result for pattern '{pattern}': found {len(result)} files")
    return result


@parser_router.get("/fs/stats", summary="Get statistics about the directory structure")
async def get_directory_stats_api(
    datastore_id: int = Depends(get_datastore_id_from_api_key)
) -> Dict[str, Any]:
    """Get statistics about the current directory structure."""
    await check_snapshot_status(datastore_id)
    logger.info(f"API request for directory stats: datastore_id={datastore_id}")
    result = await get_directory_stats(datastore_id=datastore_id)
    logger.info(f"Directory stats result: {result}")
    return result


@parser_router.delete("/fs/reset", 
    summary="Reset directory tree structure",
    description="Clear all directory entries for a specific datastore",
    status_code=status.HTTP_204_NO_CONTENT
)
async def reset_directory_tree_api(
    datastore_id: int = Depends(get_datastore_id_from_api_key)
) -> None:
    """
    Reset the directory tree structure by clearing all entries for a specific datastore.
    """
    await check_snapshot_status(datastore_id)
    logger.info(f"API request to reset directory tree for datastore {datastore_id}")
    success = await reset_directory_tree(datastore_id)
    
    if not success:
        logger.error(f"Failed to reset directory tree for datastore {datastore_id}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to reset directory tree"
        )
    logger.info(f"Successfully reset directory tree for datastore {datastore_id}")


# === View-Specific Consistency APIs ===

from ..parsers.manager import get_cached_parser_manager
from pydantic import BaseModel
from typing import List

class SuspectUpdateRequest(BaseModel):
    """Request body for updating suspect file mtimes."""
    updates: List[Dict[str, Any]]  # List of {"path": str, "mtime": float}


@parser_router.get("/fs/suspect-list", 
    summary="Get Suspect List for Sentinel Sweep",
    description="Returns files that might still be under active write"
)
async def get_suspect_list_api(
    datastore_id: int = Depends(get_datastore_id_from_api_key)
) -> Dict[str, Any]:
    """
    Get the current Suspect List for this datastore.
    Used by Leader Agent for Sentinel Sweep.
    """
    logger.info(f"API request for suspect list: datastore_id={datastore_id}")
    manager = await get_cached_parser_manager(datastore_id)
    parser = await manager.get_file_directory_parser()
    suspect_list = await parser.get_suspect_list()
    return {
        "datastore_id": datastore_id,
        "suspect_count": len(suspect_list),
        "suspects": [{"path": path, "suspect_until": ts} for path, ts in suspect_list.items()]
    }


@parser_router.put("/fs/suspect-list",
    summary="Update Suspect List from Sentinel Sweep",
    description="Agent reports current mtimes of suspect files"
)
async def update_suspect_list_api(
    request: SuspectUpdateRequest,
    datastore_id: int = Depends(get_datastore_id_from_api_key)
) -> Dict[str, Any]:
    """
    Update suspect files with their current mtimes from Agent's Sentinel Sweep.
    """
    logger.info(f"API request to update suspect list: datastore_id={datastore_id}, count={len(request.updates)}")
    manager = await get_cached_parser_manager(datastore_id)
    parser = await manager.get_file_directory_parser()
    
    updated_count = 0
    for item in request.updates:
        path = item.get("path")
        mtime = item.get("mtime")
        if path and mtime is not None:
            await parser.update_suspect(path, mtime)
            updated_count += 1
    
    return {
        "datastore_id": datastore_id,
        "updated_count": updated_count
    }


@parser_router.post("/fs/audit-start",
    summary="Signal Audit Start",
    description="Called by Leader Agent when starting an Audit cycle"
)
async def audit_start_api(
    datastore_id: int = Depends(get_datastore_id_from_api_key)
) -> Dict[str, str]:
    """Signal the start of an Audit cycle."""
    logger.info(f"Audit start signal received for datastore {datastore_id}")
    manager = await get_cached_parser_manager(datastore_id)
    parser = await manager.get_file_directory_parser()
    await parser.handle_audit_start()
    return {"status": "ok", "message": "Audit started"}


@parser_router.post("/fs/audit-end",
    summary="Signal Audit End",
    description="Called by Leader Agent when completing an Audit cycle"
)
async def audit_end_api(
    datastore_id: int = Depends(get_datastore_id_from_api_key)
) -> Dict[str, str]:
    """Signal the end of an Audit cycle. Triggers Tombstone cleanup."""
    logger.info(f"Audit end signal received for datastore {datastore_id}")
    manager = await get_cached_parser_manager(datastore_id)
    parser = await manager.get_file_directory_parser()
    await parser.handle_audit_end()
    return {"status": "ok", "message": "Audit ended, tombstones cleaned"}