"""
API endpoints for the data views.
Provides REST endpoints to access consistent data views.
"""
from fastapi import APIRouter, Query, Depends, status, HTTPException
from fastapi.responses import ORJSONResponse
import logging
from typing import Dict, Any, Optional, List
from pydantic import BaseModel

from ..view_manager.manager import (
    get_directory_tree, 
    search_files, 
    get_directory_stats, 
    reset_directory_tree,
    get_cached_view_manager
)
from ..auth.dependencies import get_datastore_id_from_api_key
from ..datastore_state_manager import datastore_state_manager
from ..in_memory_queue import memory_event_queue
from ..processing_manager import processing_manager

logger = logging.getLogger(__name__)

view_router = APIRouter(tags=["Data Views"])

async def check_snapshot_status(datastore_id: int):
    """Checks if the initial snapshot sync is complete for the datastore."""
    from ..core.session_manager import session_manager
    from ..auth.datastore_cache import datastore_config_cache
    
    config = datastore_config_cache.get_datastore_config(datastore_id)
    if config and config.meta and config.meta.get('type') == 'live':
        sessions = await session_manager.get_datastore_sessions(datastore_id)
        if not sessions:
            raise HTTPException(
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                detail="No active sessions for this live datastore. Service temporarily unavailable."
            )

    is_signal_complete = await datastore_state_manager.is_snapshot_complete(datastore_id)
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

@view_router.get("/fs/tree", 
    summary="获取文件系统树结构", 
    response_class=ORJSONResponse
)
async def get_directory_tree_api(
    path: str = Query("/", description="要检索的目录路径 (默认: '/')"),
    recursive: bool = Query(True, description="是否递归检索子目录"),
    max_depth: Optional[int] = Query(None, description="最大递归深度 (1 表示仅当前目录及其直接子级)"),
    only_path: bool = Query(False, description="是否仅返回路径结构，排除元数据"),
    dry_run: bool = Query(False, description="压测模式：跳过逻辑处理以测量框架延迟"),
    datastore_id: int = Depends(get_datastore_id_from_api_key)
) -> Optional[Dict[str, Any]]:
    """获取指定路径起始的目录结构树。"""
    await check_snapshot_status(datastore_id)
    
    if dry_run:
        return ORJSONResponse(content={"message": "dry-run", "datastore_id": datastore_id})

    effective_recursive = recursive if max_depth is None else True
    result = await get_directory_tree(path, datastore_id=datastore_id, recursive=effective_recursive, max_depth=max_depth, only_path=only_path)
    
    if result is None:
        return ORJSONResponse(content={"detail": "路径未找到或尚未同步"}, status_code=404)
    return ORJSONResponse(content=result)

@view_router.get("/fs/search", 
    summary="基于模式搜索文件"
)
async def search_files_api(
    pattern: str = Query(..., description="要搜索的路径匹配模式 (例如: '*.log' 或 '/data/res*')"),
    datastore_id: int = Depends(get_datastore_id_from_api_key)
) -> list:
    """搜索匹配模式的文件。"""
    await check_snapshot_status(datastore_id)
    return await search_files(pattern, datastore_id=datastore_id)

@view_router.get("/fs/stats", 
    summary="获取文件系统统计指标"
)
async def get_directory_stats_api(
    datastore_id: int = Depends(get_datastore_id_from_api_key)
) -> Dict[str, Any]:
    """获取当前目录结构的统计信息。"""
    await check_snapshot_status(datastore_id)
    return await get_directory_stats(datastore_id=datastore_id)

@view_router.delete("/fs/reset", 
    summary="Reset directory tree structure",
    status_code=status.HTTP_204_NO_CONTENT
)
async def reset_directory_tree_api(
    datastore_id: int = Depends(get_datastore_id_from_api_key)
) -> None:
    """Reset the directory tree structure by clearing all entries for a specific datastore."""
    success = await reset_directory_tree(datastore_id)
    if not success:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to reset directory tree"
        )

# === Consistency APIs ===

class SuspectUpdateRequest(BaseModel):
    """Request body for updating suspect file mtimes."""
    updates: List[Dict[str, Any]]  # List of {"path": str, "mtime": float}

@view_router.get("/fs/suspect-list", 
    summary="Get Suspect List for Sentinel Sweep"
)
async def get_suspect_list_api(
    datastore_id: int = Depends(get_datastore_id_from_api_key)
) -> List[Dict[str, Any]]:
    """Get the current Suspect List for this datastore."""
    manager = await get_cached_view_manager(datastore_id)
    provider = await manager.get_file_directory_provider()
    suspect_list = await provider.get_suspect_list()
    return [{"path": path, "suspect_until": ts} for path, ts in suspect_list.items()]

@view_router.put("/fs/suspect-list",
    summary="Update Suspect List from Sentinel Sweep"
)
async def update_suspect_list_api(
    request: SuspectUpdateRequest,
    datastore_id: int = Depends(get_datastore_id_from_api_key)
) -> Dict[str, Any]:
    """Update suspect files with their current mtimes from Agent's Sentinel Sweep."""
    manager = await get_cached_view_manager(datastore_id)
    provider = await manager.get_file_directory_provider()
    
    updated_count = 0
    for item in request.updates:
        path = item.get("path")
        mtime = item.get("mtime") or item.get("current_mtime")
        if path and mtime is not None:
            await provider.update_suspect(path, float(mtime))
            updated_count += 1
    
    return {"datastore_id": datastore_id, "updated_count": updated_count, "status": "ok"}

@view_router.get("/fs/blind-spots", 
    summary="Get Blind-spot Information"
)
async def get_blind_spots_api(
    datastore_id: int = Depends(get_datastore_id_from_api_key)
) -> Dict[str, Any]:
    """Get the current Blind-spot List for this datastore."""
    manager = await get_cached_view_manager(datastore_id)
    provider = await manager.get_file_directory_provider()
    if not provider:
        return {"error": "Provider not initialized"}
    return await provider.get_blind_spot_list()
