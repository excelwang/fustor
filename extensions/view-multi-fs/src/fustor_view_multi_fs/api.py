from fastapi import APIRouter, Query, Depends, HTTPException, status
from fastapi.responses import ORJSONResponse
from typing import Dict, Any, Optional, List
import logging

from .driver import MultiFSViewDriver

logger = logging.getLogger(__name__)

async def get_multi_driver(
    view_id: str,
    get_driver_func
) -> MultiFSViewDriver:
    driver = await get_driver_func(view_id)
    if not driver:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail=f"Driver for view '{view_id}' not initialized"
        )
    if not isinstance(driver, MultiFSViewDriver):
        # Should not happen if configured correctly
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Driver for view '{view_id}' is not a MultiFSViewDriver"
        )
    return driver

def _find_best_view(stats_agg: Dict[str, Any], strategy: str) -> Optional[Dict[str, Any]]:
    """
    Find the best view from aggregated stats based on strategy.
    Strategies: file_count, total_size, latest_mtime
    """
    members = stats_agg.get("members", [])
    if not members:
        return None
        
    best_value = -1
    best_member = None
    
    for member in members:
        if member.get("status") != "ok":
            continue
            
        val = 0
        if strategy == "file_count":
            val = member.get("file_count", 0)
        elif strategy == "total_size":
            val = member.get("total_size", 0)
        elif strategy == "latest_mtime":
            val = member.get("latest_mtime", 0.0)
            
        if val > best_value:
            best_value = val
            best_member = member
            
    if best_member:
         return {
             "view_id": best_member["view_id"],
             "reason": strategy,
             "value": best_value
         }
    return None

async def _logic_get_stats(
    path: str,
    best: Optional[str],
    view_id: str,
    driver: MultiFSViewDriver
):
    stats_agg = await driver.get_subtree_stats_agg(path)
    
    result = stats_agg.copy()
    if best:
        best_view = _find_best_view(stats_agg, best)
        if best_view:
            result["best"] = best_view
            
    return ORJSONResponse(content=result)

async def _logic_get_tree(
    path: str,
    recursive: bool,
    max_depth: Optional[int],
    only_path: bool,
    best: Optional[str],
    view_id: str,
    driver: MultiFSViewDriver
):
    target_view_id = None
    best_info = None

    if best:
        # First get stats to decide best view
        stats_agg = await driver.get_subtree_stats_agg(path)
        best_info = _find_best_view(stats_agg, best)
        if best_info:
            target_view_id = best_info["view_id"]
        else:
            return ORJSONResponse(content={
                "path": path,
                "members": {},
                "detail": "No valid members found for best view selection"
            })

    # Get tree
    tree_result = await driver.get_directory_tree(
        path, 
        recursive=recursive, 
        max_depth=max_depth, 
        only_path=only_path,
        best_view=target_view_id
    )
    
    result = tree_result.copy()
    if best_info:
        result["best_view_selected"] = best_info

    return ORJSONResponse(content=result)

def create_multi_fs_router(
    get_driver_func,
    check_snapshot_func,
    get_view_id_dep,
    check_metadata_limit_func=None
):
    """Factory to create the router with dependencies injested."""
    
    async def get_driver_dependency(view_id: str = Depends(get_view_id_dep)):
        return await get_multi_driver(view_id, get_driver_func)
    
    router = APIRouter()
    
    @router.get("/stats", summary="获取聚合统计信息")
    async def get_stats_wrapper(
        path: str = Query("/", description="查询路径"),
        best: Optional[str] = Query(None, description="自动推荐策略: file_count / total_size / latest_mtime"),
        view_id: str = Depends(get_view_id_dep),
        driver = Depends(get_driver_dependency)
    ):
        return await _logic_get_stats(path, best, view_id, driver)

    @router.get("/tree", summary="获取聚合目录树")
    async def get_tree_wrapper(
        path: str = Query("/", description="查询路径"),
        recursive: bool = Query(True, description="是否递归"),
        max_depth: Optional[int] = Query(None, description="最大深度"),
        only_path: bool = Query(False, description="仅返回路径结构"),
        best: Optional[str] = Query(None, description="指定策略仅返回最优成员树"),
        view_id: str = Depends(get_view_id_dep),
        driver = Depends(get_driver_dependency)
    ):
        return await _logic_get_tree(path, recursive, max_depth, only_path, best, view_id, driver)
        
    return router
