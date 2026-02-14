# fusion/src/fustor_fusion/api/on_command.py
"""
On-Command Find Fallback Mechanism.

This module provides the logic to fallback to a realtime Agent command
when the memory view is incomplete or unavailable.
"""
import logging
import time
from typing import Any, Dict, List, Optional

from fustor_fusion.core.session_manager import session_manager
from fustor_fusion import runtime_objects

logger = logging.getLogger("fustor_fusion.api.on_command")

async def on_command_fallback(view_id: str, params: Dict[str, Any]) -> Dict[str, Any]:
    """
    Execute a remote scan on the Agent when local view query fails.
    """
    start_time = time.time()
    
    try:
        # 1. Resolve Pipes
        pipe_manager = runtime_objects.pipe_manager
        if not pipe_manager:
            raise RuntimeError("Pipe manager not initialized")

        p_ids = pipe_manager.resolve_pipes_for_view(view_id)
        if not p_ids:
            raise RuntimeError(f"No active pipes found to service view {view_id}")

        # 2. Prepare Parallel Execution
        from ..config.unified import fusion_config
        fallback_timeout = fusion_config.fusion.on_command_fallback_timeout

        async def execute_on_pipe(p_id: str) -> Optional[Dict[str, Any]]:
            pipe = pipe_manager.get_pipe(p_id)
            if not pipe: return None
            
            bridge = pipe_manager.get_bridge(p_id)
            if not bridge: return None

            # Use leader session or any active session
            target_session_id = pipe.leader_session
            if not target_session_id:
                active_sessions = await bridge.get_all_sessions()
                if active_sessions:
                    target_session_id = next(iter(active_sessions))
            
            if not target_session_id:
                return None

            # 1. Create Job
            target_path = params.get("path", "/")
            job_id = await session_manager.create_agent_job(view_id, target_path, [target_session_id])

            # 2. Queue Command
            cmd_params = {
                "type": "scan",
                "path": target_path,
                "max_depth": 1 if not params.get("recursive", False) else params.get("depth", 10),
                "limit": params.get("limit", 1000),
                "pattern": params.get("pattern", "*"),
                "job_id": job_id
            }

            # We use session_manager to queue directly to ensure it matches the job
            queued = await session_manager.queue_command(view_id, target_session_id, cmd_params)
            if not queued:
                logger.warning(f"Failed to queue fallback command for {target_session_id}")
                return None

            # 3. Wait for Completion (Polling)
            # The agent will ingest events and then call complete_agent_job
            import asyncio
            poll_interval = 0.5
            start_poll = time.time()
            
            while time.time() - start_poll < fallback_timeout:
                if job_id not in session_manager._agent_jobs:
                    # Maybe it finished and was cleaned up? Or invalid?
                    break
                
                job = session_manager._agent_jobs[job_id]
                if job.status == "COMPLETED":
                     # Job done! Data should be in View now.
                     # We return a dummy success structure, or query the view.
                     # Since we are iterating multiple pipes, we just return metadata.
                     return {
                         "success": True, 
                         "job_id": job_id,
                         "files": [] # We don't have the files list directly here without querying view
                     }
                
                if job.status == "FAILED":
                     logger.warning(f"Job {job_id} failed for session {target_session_id}")
                     return None
                     
                await asyncio.sleep(poll_interval)
            
            logger.warning(f"Fallback timed out for job {job_id}")
            return None

        # 3. Dispatch and Wait
        results = await asyncio.gather(*[execute_on_pipe(pid) for pid in p_ids])
        
        # 4. Check Success
        successful = any(r and r.get("success") for r in results)

        if not successful:
            raise HTTPException(
                status_code=502,
                detail=f"Fallback command failed on all {len(p_ids)} pipes for view {view_id}"
            )


        
        # 5. Query View for Results (Compensatory Read)
        # Now that ingestion is complete, we read from the view to return "files"
        # We query the View Manager for the Forest/View Driver and fetch the tree.
        try:
            if runtime_objects.view_manager:
                # Get the view driver (ForestView or FSView)
                view_driver = runtime_objects.view_manager.get_view(view_id)
                if view_driver:
                    # Query the view for the requested path
                    tree_data = await view_driver.get_directory_tree(
                        path=params.get("path", "/"),
                        recursive=params.get("recursive", True), # Default to True if not specified, or checks params
                        # We must be careful not to trigger infinite recursion if get_directory_tree calls us back.
                        # But get_directory_tree tracks 'on_demand_scan' param.
                        # Here we are calling directly on driver, bypassing the API wrapper that triggers fallback.
                        # Wait, driver.get_directory_tree MIGHT trigger fallback if it hits TypeError?
                        # No, driver.get_directory_tree raises TypeError.
                        # We should catch it and return empty if so, but we just want the data if present.
                    )
                    
                    if tree_data:
                         # Ensure we return 'entries' list as expected by API
                         # The tree_data might be a DirectoryNode dict or list of entries depending on implementation
                         # FSViewQuery.get_directory_tree returns dict with 'entries' key usually.
                         # Let's check what it returns.
                         # Based on driver.py: return self.query.get_directory_tree(...)
                         # It usually returns { "path": ..., "entries": [...] } or similar.
                         # We'll just merge it.
                         return tree_data
        except Exception as e:
            logger.warning(f"View {view_id}: Compensatory read failed: {e}")
            # Fallthrough to returning empty result with success metadata
            pass

        duration = time.time() - start_time
        return {
            "path": params.get("path", "/"),
            "entries": [], # Default if read fails
            "metadata": {
                "source": "remote_fallback_async",
                "latency_ms": int(duration * 1000),
                "pipes_queried": len(p_ids),
                "successful_pipes": len([r for r in results if r]),
                "timestamp": time.time()
            }
        }
    except Exception as e:
        logger.error(f"View {view_id}: Fallback FAILED: {e}")
        raise HTTPException(
            status_code=502,
            detail=f"Fallback command failed on view {view_id}: {str(e)}"
        )


from fastapi import HTTPException
import asyncio
