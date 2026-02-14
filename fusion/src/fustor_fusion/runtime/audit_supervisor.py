import time
import logging
from .. import runtime_objects

logger = logging.getLogger(__name__)

async def check_audit_timeout():
    """Check for stuck audit cycles and force close them."""
    pm = runtime_objects.pipe_manager
    if not pm: return
    
    now = time.time()
    for pipe in pm.get_pipes().values():
        # Default to 600s if not configured, timeout is 2x interval
        interval = getattr(pipe, 'audit_interval_sec', None) or \
                   pipe.config.get("audit_interval_sec", 600)
        timeout_threshold = interval * 2
        
        # Check all handlers in this pipe
        # We access protected _view_handlers as we are in the runtime/main loop
        if not hasattr(pipe, '_view_handlers'):
            continue
            
        for handler in pipe._view_handlers.values():
            start_time = getattr(handler, 'audit_start_time', None)
            
            if start_time and (now - start_time > timeout_threshold):
                logger.warning(
                    f"Audit timeout detected for handler {handler.id} in pipe {pipe.id} "
                    f"(dur={now-start_time:.1f}s > limit={timeout_threshold}s). Forcing completion."
                )
                try:
                    if hasattr(handler, 'handle_audit_end'):
                        await handler.handle_audit_end()
                    
                    # Manually clear timestamp since the driver doesn't know about it
                    # Handle Adapter cases
                    if hasattr(handler, 'driver'): # ViewDriverAdapter
                        handler.driver._audit_start_time = None
                    elif hasattr(handler, 'manager'): # ViewManagerAdapter
                         for d in handler.manager.driver_instances.values():
                             d._audit_start_time = None
                             
                except Exception as e:
                    logger.error(f"Error forcing audit end for {handler.id}: {e}")
