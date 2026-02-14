from fastapi import APIRouter, Depends, HTTPException
from typing import Dict, Any, Optional
from fustor_fusion.api.deps import get_view_driver
from .driver import ForestFSViewDriver

def create_view_router(view_name: str) -> APIRouter:
    """
    Factory for Forest View API Router.
    """
    router = APIRouter()

    @router.get("/stats")
    async def get_stats(
        path: str = "/", 
        recursive: bool = True,
        best: Optional[str] = None,
        driver: ForestFSViewDriver = Depends(get_view_driver(view_name))
    ):
        """
        Get aggregated stats comparison across all pipes.
        """
        # Note: best parameter logic handles within get_subtree_stats_agg if needed
        # but currently implementation splits them.
        # Let's align with driver implementation.
        # Driver implementation: get_subtree_stats_agg(path) -> {path, members, best}
        
        try:
            return await driver.get_subtree_stats_agg(path)
        except Exception as e:
            raise HTTPException(status_code=500, detail=str(e))

    @router.get("/tree")
    async def get_tree(
        path: str = "/",
        best: Optional[str] = None,
        recursive: bool = True,
        driver: ForestFSViewDriver = Depends(get_view_driver(view_name))
    ):
        """
        Get aggregated directory trees.
        If ?best=strategy is provided, returns only the best tree.
        """
        try:
            # We pass 'best' strategy to driver
            return await driver.get_directory_tree(
                path=path, 
                best=best,
                recursive=recursive
            )
        except Exception as e:
            raise HTTPException(status_code=500, detail=str(e))

    return router
