from typing import Dict, Any, List, Optional, Tuple
import asyncio
import logging
from fustor_core.drivers import ViewDriver
from fustor_core.event.base import EventBase

logger = logging.getLogger(__name__)

class MultiFSViewDriver(ViewDriver):
    schema_name = "multi-fs"
    target_schema = "multi-fs"  # Avoid processing 'fs' events

    QUERY_TIMEOUT = 5.0  # Seconds to wait for member responses

    def __init__(self, id: str, view_id: str, config: Dict[str, Any]):
        super().__init__(id, view_id, config)
        # Extract members from driver_params
        # config passed to handler is typically the view config dict or object
        # In Fustor, Handler keys off config dict.
        # ViewConfig.driver_params is where parameters live.
        self.members: List[str] = config.get("members", [])
        self.query_timeout = float(config.get("query_timeout", self.QUERY_TIMEOUT))
        if not self.members:
            logger.warning(f"MultiFSViewDriver '{id}' initialized with no members!")

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

    async def get_subtree_stats_agg(self, path: str, timeout: Optional[float] = None) -> Dict[str, Any]:
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
                # Use wait_for for the entire operation including driver lookup
                # (driver lookup is usually fast but might be slow if locks are held)
                effective_timeout = timeout if timeout is not None else self.query_timeout
                return await asyncio.wait_for(self._fetch_stats_internal(vid, path), timeout=effective_timeout)
            except asyncio.TimeoutError:
                logger.warning(f"Timeout fetching stats for member '{vid}' (path='{path}')")
                return {"view_id": vid, "status": "error", "error": "Timeout"}
            except Exception as e:
                return {"view_id": vid, "status": "error", "error": str(e)}

        # Perform concurrent queries
        if self.members:
            tasks = [fetch_stats(vid) for vid in self.members]
            results = await asyncio.gather(*tasks)
        
        return {
            "path": path,
            "members": list(results)
        }

    async def _fetch_stats_internal(self, vid: str, path: str) -> Dict[str, Any]:
        driver = await self._get_member_driver(vid)
        if not driver:
             return {"view_id": vid, "status": "error", "error": "View not found or driver not ready"}
        
        # Call get_subtree_stats on member driver
        # NOTE: FSViewDriver must implement get_subtree_stats
        if not hasattr(driver, "get_subtree_stats"):
             return {"view_id": vid, "status": "error", "error": "Driver does not support subtree stats"}

        stats = await driver.get_subtree_stats(path)
        return {"view_id": vid, "status": "ok", **stats}

    async def get_directory_tree(self, path: str, recursive: bool = True, max_depth: Optional[int] = None, only_path: bool = False, best_view: Optional[str] = None, timeout: Optional[float] = None) -> Dict[str, Any]:
        """
        Get directory tree from members.
        If best_view is specified, only query that view.
        Otherwise query all members.
        """
        target_views = [best_view] if best_view else self.members
        
        results = {}

        async def fetch_tree(vid):
            try:
                effective_timeout = timeout if timeout is not None else self.query_timeout
                return await asyncio.wait_for(self._fetch_tree_internal(vid, path, recursive, max_depth, only_path), timeout=effective_timeout)
            except asyncio.TimeoutError:
                logger.warning(f"Timeout fetching tree for member '{vid}' (path='{path}')")
                return vid, {"status": "error", "error": "Timeout"}
            except Exception as e:
                return vid, {"status": "error", "error": str(e)}

        if target_views:
            tasks = [fetch_tree(vid) for vid in target_views]
            results_list = await asyncio.gather(*tasks)
            results = dict(results_list)
            
        return {
            "path": path,
            "members": results
        }

    async def _fetch_tree_internal(self, vid: str, path: str, recursive: bool, max_depth: Optional[int], only_path: bool) -> Tuple[str, Dict[str, Any]]:
        driver = await self._get_member_driver(vid)
        if not driver:
             return vid, {"status": "error", "error": "View not found or driver not ready"}
        
        # Call get_directory_tree on member driver
        data = await driver.get_directory_tree(path, recursive=recursive, max_depth=max_depth, only_path=only_path)
        if data is None:
            return vid, {"status": "error", "error": "Path not found"}
        
        return vid, {"status": "ok", "data": data}

    async def get_data_view(self, **kwargs) -> Any:
        """Required by the ViewDriver ABC."""
        # For FS views, get_data_view usually maps to get_directory_tree
        # kwargs might contain path, recursive, etc.
        path = kwargs.get("path", "/")
        return await self.get_directory_tree(path, **{k:v for k,v in kwargs.items() if k != "path"})

    async def trigger_on_demand_scan(self, path: str, recursive: bool = True) -> Tuple[bool, Optional[str]]:
        """
        Broadcast on-demand scan command to ALL member views.
        Each member FSViewDriver will further broadcast to its active sessions/agents.
        
        Returns:
            (overall_success, composite_result_summary)
        """
        if not self.members:
            logger.warning(f"MultiFSViewDriver '{self.id}' has no members. Cannot broadcast on-demand scan.")
            return False, None

        async def broadcast_to_member(vid: str):
            try:
                # Command dispatch is usually very fast unless SessionManager is locked
                return await asyncio.wait_for(self._broadcast_internal(vid, path, recursive), timeout=self.query_timeout)
            except asyncio.TimeoutError:
                logger.warning(f"Timeout broadcasting scan command to member '{vid}'")
                return vid, False, None, "Timeout"
            except Exception as e:
                logger.error(f"Error broadcasting on-demand scan to member '{vid}': {e}")
                return vid, False, None, str(e)

        tasks = [broadcast_to_member(vid) for vid in self.members]
        task_results = await asyncio.gather(*tasks)

        any_success = False
        member_jobs = {}
        for vid, success, job_id, error in task_results:
            member_jobs[vid] = {
                "success": success,
                "job_id": job_id,
                "error": error
            }
            if success:
                any_success = True

        logger.info(
            f"On-demand scan broadcast complete for path '{path}': "
            f"{sum(1 for r in task_results if r[1])}/{len(self.members)} members accepted"
        )

        # Return a summary job_id that encodes all member jobs
        import json
        composite_id = json.dumps(member_jobs, default=str) if any_success else None
        return any_success, composite_id

    async def _broadcast_internal(self, vid: str, path: str, recursive: bool) -> Tuple[str, bool, Optional[str], Optional[str]]:
        driver = await self._get_member_driver(vid)
        if not driver:
            return vid, False, None, "View not found or driver not ready"
        
        if not hasattr(driver, 'trigger_on_demand_scan'):
            return vid, False, None, "Driver does not support on-demand scan"
        
        success, job_id = await driver.trigger_on_demand_scan(path, recursive=recursive)
        return vid, success, job_id, None
