from fastapi import APIRouter, Depends, HTTPException, Query
from typing import Dict, Any, Optional
import logging

logger = logging.getLogger(__name__)

from .driver import ForestFSViewDriver

def create_view_router(get_driver_func, check_snapshot_func, get_view_id_dep, check_metadata_limit_func=None) -> APIRouter:
    """
    Factory for Forest View API Router.
    Matches the signature expected by fustor-fusion.
    """
    router = APIRouter()

    @router.get("/stats")
    async def get_stats(
        path: str = "/", 
        recursive: bool = True,
        best: Optional[str] = None,
        view_id: str = Depends(get_view_id_dep)
    ):
        """
        Get aggregated stats comparison across all pipes.
        """
        driver: ForestFSViewDriver = await get_driver_func(view_id)
        if not driver:
            raise HTTPException(status_code=503, detail="Driver not initialized")
            
        try:
            return await driver.get_subtree_stats_agg(path)
        except Exception as e:
            logger.error(f"Error in Forest stats for {view_id}: {e}", exc_info=True)
            raise HTTPException(status_code=500, detail=str(e))

    @router.get("/tree")
    async def get_tree(
        path: str = "/",
        best: Optional[str] = None,
        recursive: bool = True,
        view_id: str = Depends(get_view_id_dep)
    ):
        """
        Get aggregated directory trees.
        If ?best=strategy is provided, returns only the best tree.
        """
        driver: ForestFSViewDriver = await get_driver_func(view_id)
        if not driver:
            raise HTTPException(status_code=503, detail="Driver not initialized")

        try:
            # We pass 'best' strategy to driver
            return await driver.get_directory_tree(
                path=path, 
                best=best,
                recursive=recursive
            )
        except Exception as e:
            logger.error(f"Error in Forest tree for {view_id}: {e}", exc_info=True)
            raise HTTPException(status_code=500, detail=str(e))

    return router
