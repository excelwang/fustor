from typing import Dict, Any, List, Optional
import asyncio
import logging
from fustor_core.drivers import ViewDriver
from fustor_core.event.base import EventBase

logger = logging.getLogger(__name__)

class MultiFSViewDriver(ViewDriver):
    schema_name = "multi-fs"

    def __init__(self, handler_id: str, view_id: str, config: Dict[str, Any]):
        super().__init__(handler_id, view_id, config)
        # Extract members from driver_params
        # config passed to handler is typically the view config dict or object
        # In Fustor, Handler keys off config dict.
        # ViewConfig.driver_params is where parameters live.
        self.members: List[str] = config.get("driver_params", {}).get("members", [])
        if not self.members:
            logger.warning(f"MultiFSViewDriver '{handler_id}' initialized with no members!")

    async def process_event(self, event: EventBase) -> bool:
        # Multi-FS view does not consume events directly.
        # It relies on member views to process events.
        return True

    def _get_view_manager_func(self):
        """Helper to get the view manager function. Useful for mocking."""
        from fustor_fusion.view_manager.manager import get_cached_view_manager
        return get_cached_view_manager

    async def _get_member_driver(self, view_id: str):
        """Helper to get a member view's driver instance."""
        try:
            # Import here to avoid circular dependency and ensure runtime availability
            get_cached_view_manager = self._get_view_manager_func()
            
            manager = await get_cached_view_manager(view_id)
            if not manager.driver_instances:
                return None
            # Return the first driver instance (assuming 1:1 for now)
            return list(manager.driver_instances.values())[0]
        except Exception as e:
            logger.debug(f"Failed to get driver for view '{view_id}': {e}")
            return None

    async def get_subtree_stats_agg(self, path: str) -> Dict[str, Any]:
        """
        Aggregate subtree stats from all members.
        Returns:
            {
                "path": path,
                "members": [
                    { "view_id": "v1", "status": "ok", ...stats... },
                    { "view_id": "v2", "status": "error", "error": "..." }
                ]
            }
        """
        results = []
        
        async def fetch_stats(vid):
            try:
                driver = await self._get_member_driver(vid)
                if not driver:
                     return {"view_id": vid, "status": "error", "error": "View not found or driver not ready"}
                
                # Call get_subtree_stats on member driver
                # NOTE: FSViewDriver must implement get_subtree_stats
                if not hasattr(driver, "get_subtree_stats"):
                     return {"view_id": vid, "status": "error", "error": "Driver does not support subtree stats"}

                stats = await driver.get_subtree_stats(path)
                return {"view_id": vid, "status": "ok", **stats}
            except Exception as e:
                return {"view_id": vid, "status": "error", "error": str(e)}

        # Perform concurrent queries
        # If no members, return empty list
        if not self.members:
            # Fallback: if no members explicitly configured, maybe try to query all views?
            # Spec says "members" param is used. If empty, maybe specific logic?
            # Spec 4.1 says: `views` param in API defaults to all. 
            # But here we are in the driver which is configured with specific members (e.g. "shared-storage").
            # If members is empty in config, it returns empty results.
            # The API level might handle "all views" by not strictly relying on this driver 
            # if we wanted a generic "query all" endpoint. 
            # But the spec says `view-multi-fs` *is* the driver for aggregation.
            # So `members` should be configured.
            pass

        # We query all configured members
        tasks = [fetch_stats(vid) for vid in self.members]
        if tasks:
            results = await asyncio.gather(*tasks)
        
        return {
            "path": path,
            "members": list(results)
        }

    async def get_directory_tree(self, path: str, recursive: bool = True, max_depth: Optional[int] = None, only_path: bool = False, best_view: Optional[str] = None) -> Dict[str, Any]:
        """
        Get directory tree from members.
        If best_view is specified, only query that view.
        Otherwise query all members.
        """
        target_views = [best_view] if best_view else self.members
        
        results = {}

        async def fetch_tree(vid):
            try:
                driver = await self._get_member_driver(vid)
                if not driver:
                     return vid, {"status": "error", "error": "View not found or driver not ready"}
                
                # Call get_directory_tree on member driver
                data = await driver.get_directory_tree(path, recursive=recursive, max_depth=max_depth, only_path=only_path)
                if data is None:
                    return vid, {"status": "error", "error": "Path not found"}
                
                return vid, {"status": "ok", "data": data}
            except Exception as e:
                return vid, {"status": "error", "error": str(e)}

        tasks = [fetch_tree(vid) for vid in target_views]
        if tasks:
            results_list = await asyncio.gather(*tasks)
            results = dict(results_list)
            
        return {
            "path": path,
            "members": results
        }

    async def get_data_view(self, **kwargs) -> Any:
        """Required by the ViewDriver ABC."""
        # For FS views, get_data_view usually maps to get_directory_tree
        # kwargs might contain path, recursive, etc.
        path = kwargs.get("path", "/")
        return await self.get_directory_tree(path, **{k:v for k,v in kwargs.items() if k != "path"})
