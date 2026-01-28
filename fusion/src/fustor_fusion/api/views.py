"""
API endpoints for the parsers module.
Provides REST endpoints to access parsed data views.
"""
from fastapi import APIRouter, Query, Header, Depends, status, HTTPException
from fastapi.responses import ORJSONResponse
import logging
import asyncio
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

@parser_router.get("/fs/tree", 
    summary="获取文件系统树结构", 
    description="""
    获取指定路径下的目录树结构。支持高度自定义的查询参数：
    
    *   **path**: 目标路径，默认为根目录 ('/')。
    *   **recursive**: 是否递归获取子目录。
    *   **max_depth**: 限制递归深度。若设置，将忽略 recursive 参数并执行带深度限制的递归。
    *   **only_path**: 极简模式，仅返回路径结构，剔除 size, timestamps 等元数据，适用于大规模数据快速检索。
    *   **dry_run**: 压力测试模式，不执行查询逻辑，直接返回 200，用于测量网络开销。
    
    **注意**: 在 Datastore 初始快照同步完成前，此接口返回 503。
    """,
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
    logger.debug(f"API request for directory tree: path={path}, recursive={effective_recursive}, max_depth={max_depth}, only_path={only_path}, datastore_id={datastore_id}")
    result = await get_directory_tree(path, datastore_id=datastore_id, recursive=effective_recursive, max_depth=max_depth, only_path=only_path)
    
    if result is None:
        return ORJSONResponse(content={"detail": "路径未找到或尚未同步"}, status_code=404)
    return ORJSONResponse(content=result)

@parser_router.get("/fs/search", 
    summary="基于模式搜索文件", 
    description="""
    在当前 Datastore 的内存树中搜索匹配给定模式的文件。
    
    *   支持包含星号 (*) 的模糊匹配。
    *   搜索是全局性的，不局限于当前目录。
    *   返回包含完整元数据的文件列表。
    """
)
async def search_files_api(
    pattern: str = Query(..., description="要搜索的路径匹配模式 (例如: '*.log' 或 '/data/res*')"),
    datastore_id: int = Depends(get_datastore_id_from_api_key)
) -> list:
    """搜索匹配模式的文件。"""
    await check_snapshot_status(datastore_id)
    logger.info(f"API request for file search: pattern={pattern}, datastore_id={datastore_id}")
    result = await search_files(pattern, datastore_id=datastore_id)
    logger.info(f"File search result for pattern '{pattern}': found {len(result)} files")
    return result


@parser_router.get("/fs/stats", 
    summary="获取文件系统统计指标", 
    description="""
    获取当前汇总的存储统计信息，包括：
    
    *   **total_files**: 总文件数。
    *   **total_directories**: 总目录数。
    *   **total_size**: 累计文件大小。
    *   **last_event_latency_ms**: 最近处理的一个事件从产生到解析完成的物理延迟。
    *   **oldest_directory**: 包含最陈旧数据的目录信息（用于评估同步延迟）。
    """
)
async def get_directory_stats_api(
    datastore_id: int = Depends(get_datastore_id_from_api_key)
) -> Dict[str, Any]:
    """获取当前目录结构的统计信息。"""
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
    # No check_snapshot_status here to allow resetting during/after failures
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
) -> List[Dict[str, Any]]:
    """
    Get the current Suspect List for this datastore.
    Used by Leader Agent for Sentinel Sweep.
    """
    logger.info(f"API request for suspect list: datastore_id={datastore_id}")
    manager = await get_cached_parser_manager(datastore_id)
    parser = await manager.get_file_directory_parser()
    suspect_list = await parser.get_suspect_list()
    return [{"path": path, "suspect_until": ts} for path, ts in suspect_list.items()]


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
        mtime = item.get("mtime") or item.get("current_mtime")
        if path and mtime is not None:
            await parser.update_suspect(path, float(mtime))
            updated_count += 1
    
    return {
        "datastore_id": datastore_id,
        "updated_count": updated_count,
        "status": "ok"
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


    return {"status": "ok", "message": "Audit started"}


async def _process_audit_end_background(datastore_id: int):
    """Background task to wait for queue drain and process audit end."""
    logger.info(f"Background task: Waiting for queue drain for audit end (datastore {datastore_id})")
    
    # Wait for queue to drain
    max_wait = 10.0
    wait_interval = 0.1
    elapsed = 0.0
    
    while elapsed < max_wait:
        queue_size = memory_event_queue.get_queue_size(datastore_id)
        inflight = processing_manager.get_inflight_count(datastore_id)
        if queue_size == 0 and inflight == 0:
            break
        await asyncio.sleep(wait_interval)
        elapsed += wait_interval
        
    if elapsed >= max_wait:
        logger.warning(f"Audit end timeout waiting for queue: queue={memory_event_queue.get_queue_size(datastore_id)}, inflight={processing_manager.get_inflight_count(datastore_id)}")
    
    manager = await get_cached_parser_manager(datastore_id)
    parser = await manager.get_file_directory_parser()
    await parser.handle_audit_end()
    logger.info(f"Background task: Audit end processing complete for datastore {datastore_id}")


@parser_router.post("/fs/audit-end",
    summary="Signal Audit End",
    description="Called by Leader Agent when completing an Audit cycle"
)
async def audit_end_api(
    datastore_id: int = Depends(get_datastore_id_from_api_key)
) -> Dict[str, str]:
    """Signal the end of an Audit cycle. Performs Tombstone cleanup and missing detection."""
    logger.info(f"Audit end signal received for datastore {datastore_id}. Processing...")
    
    await _process_audit_end_background(datastore_id)
    
    return {"status": "ok", "message": "Audit end processed"}


@parser_router.get("/fs/blind-spots", 
    summary="Get Blind-spot Information",
    description="Returns files that exist on disk but were not reported by any Agent (Realtime/Snapshot)"
)
async def get_blind_spots_api(
    datastore_id: int = Depends(get_datastore_id_from_api_key)
) -> Dict[str, Any]:
    """
    Get the current Blind-spot List for this datastore.
    """
    logger.info(f"API request for blind-spots: datastore_id={datastore_id}")
    manager = await get_cached_parser_manager(datastore_id)
    parser = await manager.get_file_directory_parser()
    if not parser:
        return {"error": "Parser not initialized"}
        
    return await parser.get_blind_spot_list()

