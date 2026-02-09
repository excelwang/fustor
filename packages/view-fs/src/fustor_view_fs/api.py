"""
FastAPI Router for FS View API endpoints.
This module provides all /fs/* endpoints for the filesystem view driver.
"""
from fastapi import APIRouter, Query, Depends, HTTPException, status
from fastapi.responses import ORJSONResponse
from typing import Dict, Any, Optional, List
from pydantic import BaseModel
import logging

logger = logging.getLogger(__name__)

# Create the router with /fs prefix
fs_router = APIRouter(tags=["Filesystem Views"])


class SuspectUpdateRequest(BaseModel):
    """Request body for updating suspect file mtimes."""
    updates: List[Dict[str, Any]]  # List of {"path": str, "mtime": float}


def create_fs_router(get_driver_func, check_snapshot_func, get_view_id_dep):
    """
    Factory function to create the FS router with proper dependencies.
    
    Args:
        get_driver_func: Async function to get the FSViewDriver for a view
        check_snapshot_func: Async function to check snapshot status (includes Core + Live logic)
        get_view_id_dep: FastAPI dependency to get view_id from API key

    Returns:
        Configured FastAPI APIRouter
    """
    router = APIRouter(tags=["Filesystem Views"])
    
    @router.get("/tree", 
        summary="获取文件系统树结构", 
        response_class=ORJSONResponse
    )
    async def get_directory_tree_api(
        path: str = Query("/", description="要检索的目录路径 (默认: '/')"),
        recursive: bool = Query(True, description="是否递归检索子目录"),
        max_depth: Optional[int] = Query(None, description="最大递归深度 (1 表示仅当前目录及其直接子级)"),
        only_path: bool = Query(False, description="是否仅返回路径结构，排除元数据"),
        dry_run: bool = Query(False, description="压测模式：跳过逻辑处理以测量框架延迟"),
        view_id: str = Depends(get_view_id_dep)
    ) -> Optional[Dict[str, Any]]:
        """获取指定路径起始的目录结构树。"""
        await check_snapshot_func(view_id)
        
        if dry_run:
            return ORJSONResponse(content={"message": "dry-run", "view_id": view_id})

        driver = await get_driver_func(view_id)
        if not driver:
            return ORJSONResponse(content={"detail": "Driver not initialized"}, status_code=503)
        
        effective_recursive = recursive if max_depth is None else True
        result = await driver.get_directory_tree(path, recursive=effective_recursive, max_depth=max_depth, only_path=only_path)
        
        if result is None:
            return ORJSONResponse(content={"detail": "路径未找到或尚未同步"}, status_code=404)
        return ORJSONResponse(content=result)

    @router.get("/search", summary="基于模式搜索文件")
    async def search_files_api(
        pattern: str = Query(..., description="要搜索的路径匹配模式 (例如: '*.log' 或 '/data/res*')"),
        view_id: str = Depends(get_view_id_dep)
    ) -> list:
        """搜索匹配模式的文件。"""
        await check_snapshot_func(view_id)
        driver = await get_driver_func(view_id)
        if not driver:
            return []
        return await driver.search_files(pattern)

    @router.get("/stats", summary="获取文件系统统计指标")
    async def get_directory_stats_api(
        view_id: str = Depends(get_view_id_dep)
    ) -> Dict[str, Any]:
        """获取当前目录结构的统计信息。"""
        await check_snapshot_func(view_id)
        driver = await get_driver_func(view_id)
        if not driver:
            return {"error": "Driver not initialized"}
        return await driver.get_directory_stats()

    @router.delete("/reset", 
        summary="Reset directory tree structure",
        status_code=status.HTTP_204_NO_CONTENT
    )
    async def reset_directory_tree_api(
        view_id: str = Depends(get_view_id_dep)
    ) -> None:
        """Reset the directory tree structure by clearing all entries for a specific view."""
        # Try global reset first (clears sessions, leadership, and cache)
        try:
            from fustor_fusion.view_manager.manager import reset_views
            await reset_views(view_id)
            return
        except (ImportError, Exception):
            # Fallback to driver-specific reset if fustor_fusion not available
            driver = await get_driver_func(view_id)
            if driver:
                await driver.reset()

    @router.get("/suspect-list", summary="Get Suspect List for Sentinel Sweep")
    async def get_suspect_list_api(
        view_id: str = Depends(get_view_id_dep)
    ) -> List[Dict[str, Any]]:
        """Get the current Suspect List for this view."""
        driver = await get_driver_func(view_id)
        if not driver:
            return []
        suspect_list = await driver.get_suspect_list()
        return [{"path": path, "suspect_until": ts} for path, ts in suspect_list.items()]

    @router.put("/suspect-list", summary="Update Suspect List from Sentinel Sweep")
    async def update_suspect_list_api(
        request: SuspectUpdateRequest,
        view_id: str = Depends(get_view_id_dep)
    ) -> Dict[str, Any]:
        """Update suspect files with their current mtimes from Agent's Sentinel Sweep."""
        driver = await get_driver_func(view_id)
        if not driver:
            raise HTTPException(status_code=404, detail="View Driver not initialized")
        
        updated_count = 0
        for item in request.updates:
            path = item.get("path")
            mtime = item.get("mtime") or item.get("current_mtime")
            if path and mtime is not None:
                await driver.update_suspect(path, float(mtime))
                updated_count += 1
        
        return {"view_id": view_id, "updated_count": updated_count, "status": "ok"}

    @router.get("/blind-spots", summary="Get Blind-spot Information")
    async def get_blind_spots_api(
        view_id: str = Depends(get_view_id_dep)
    ) -> Dict[str, Any]:
        """Get the current Blind-spot List for this view."""
        driver = await get_driver_func(view_id)
        if not driver:
            return {"error": "Driver not initialized"}
        return await driver.get_blind_spot_list()

    return router


def get_router():
    """
    Returns a placeholder router. The actual router should be created via create_fs_router().
    This is for compatibility with entry point discovery.
    """
    return fs_router
