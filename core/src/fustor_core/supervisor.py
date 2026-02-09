"""
Component Supervisor for Fault Isolation.

This module provides a lightweight supervisor pattern for managing component
lifecycles with fault isolation. Individual component failures are contained
and do not affect other components.
"""
import asyncio
import logging
from enum import Enum, auto
from typing import Any, Callable, Coroutine, Dict, List, Optional, Protocol
from dataclasses import dataclass, field

logger = logging.getLogger(__name__)


class ComponentState(Enum):
    """State of a managed component."""
    IDLE = auto()
    STARTING = auto()
    RUNNING = auto()
    STOPPING = auto()
    STOPPED = auto()
    FAILED = auto()
    DEGRADED = auto()


class RestartPolicy(Enum):
    """Restart policy for failed components."""
    NEVER = auto()      # Never restart
    ON_FAILURE = auto() # Restart on failure (default)
    ALWAYS = auto()     # Always restart when stopped


class ManagedComponent(Protocol):
    """Protocol for components that can be managed by the Supervisor."""
    
    async def start(self) -> None:
        """Start the component."""
        ...
    
    async def stop(self) -> None:
        """Stop the component gracefully."""
        ...
    
    def is_healthy(self) -> bool:
        """Check if the component is healthy."""
        ...


@dataclass
class ComponentWrapper:
    """Wraps a component with lifecycle management metadata."""
    component: Any
    state: ComponentState = ComponentState.IDLE
    info: str = ""
    error_count: int = 0
    last_error: Optional[Exception] = None
    restart_policy: RestartPolicy = RestartPolicy.ON_FAILURE
    max_restarts: int = 3
    restart_count: int = 0
    
    async def start(self) -> bool:
        """Start the wrapped component with error handling."""
        if self.state == ComponentState.RUNNING:
            return True
        
        self.state = ComponentState.STARTING
        try:
            if hasattr(self.component, 'start'):
                await self.component.start()
            self.state = ComponentState.RUNNING
            self.info = "Component started successfully"
            self.restart_count = 0  # Reset on successful start
            return True
        except Exception as e:
            self.state = ComponentState.FAILED
            self.last_error = e
            self.error_count += 1
            self.info = f"Start failed: {e}"
            logger.error(f"Component start failed: {e}", exc_info=True)
            return False
    
    async def stop(self) -> bool:
        """Stop the wrapped component with error handling."""
        if self.state in (ComponentState.STOPPED, ComponentState.IDLE):
            return True
        
        self.state = ComponentState.STOPPING
        try:
            if hasattr(self.component, 'stop'):
                await self.component.stop()
            self.state = ComponentState.STOPPED
            self.info = "Component stopped successfully"
            return True
        except Exception as e:
            self.state = ComponentState.FAILED
            self.last_error = e
            self.info = f"Stop failed: {e}"
            logger.error(f"Component stop failed: {e}", exc_info=True)
            return False
    
    def is_healthy(self) -> bool:
        """Check component health."""
        if self.state != ComponentState.RUNNING:
            return False
        if hasattr(self.component, 'is_healthy'):
            return self.component.is_healthy()
        return True
    
    def can_restart(self) -> bool:
        """Check if the component can be restarted based on policy."""
        if self.restart_policy == RestartPolicy.NEVER:
            return False
        if self.restart_count >= self.max_restarts:
            return False
        return True


@dataclass
class StartResult:
    """Result of a component start operation."""
    component_id: str
    success: bool
    error: Optional[str] = None


class ComponentSupervisor:
    """
    Manages independent component lifecycles with fault isolation.
    
    Features:
    - Isolated startup: One component's failure doesn't block others
    - Health monitoring: Periodic health checks with optional auto-restart
    - Graceful degradation: Failed components are marked but don't kill the process
    
    Usage:
        supervisor = ComponentSupervisor()
        await supervisor.register("pipe-1", pipe1)
        await supervisor.register("pipe-2", pipe2)
        
        results = await supervisor.start_all()
        # results shows which components succeeded/failed
        
        # Later...
        await supervisor.stop_all()
    """
    
    def __init__(self, health_check_interval: float = 30.0):
        self._components: Dict[str, ComponentWrapper] = {}
        self._health_check_task: Optional[asyncio.Task] = None
        self._health_check_interval = health_check_interval
        self._lock = asyncio.Lock()
    
    async def register(
        self, 
        component_id: str, 
        component: Any,
        restart_policy: RestartPolicy = RestartPolicy.ON_FAILURE,
        max_restarts: int = 3
    ) -> None:
        """
        Register a component for management.
        
        Args:
            component_id: Unique identifier for the component
            component: The component instance (should have start/stop methods)
            restart_policy: When to automatically restart the component
            max_restarts: Maximum number of restart attempts
        """
        async with self._lock:
            if component_id in self._components:
                logger.warning(f"Component {component_id} already registered, replacing")
            
            wrapper = ComponentWrapper(
                component=component,
                restart_policy=restart_policy,
                max_restarts=max_restarts
            )
            self._components[component_id] = wrapper
            logger.debug(f"Registered component: {component_id}")
    
    async def unregister(self, component_id: str) -> bool:
        """Unregister a component (stops it first if running)."""
        async with self._lock:
            wrapper = self._components.pop(component_id, None)
            if wrapper:
                await wrapper.stop()
                return True
            return False
    
    async def start_one(self, component_id: str) -> StartResult:
        """Start a single component."""
        wrapper = self._components.get(component_id)
        if not wrapper:
            return StartResult(component_id, False, "Component not found")
        
        success = await wrapper.start()
        return StartResult(
            component_id=component_id,
            success=success,
            error=str(wrapper.last_error) if wrapper.last_error else None
        )
    
    async def start_all(self, ignore_failures: bool = True) -> List[StartResult]:
        """
        Start all registered components with fault isolation.
        
        Args:
            ignore_failures: If True (default), continue starting other components
                           even if some fail. If False, stop on first failure.
        
        Returns:
            List of StartResult for each component
        """
        results: List[StartResult] = []
        
        # Use gather with return_exceptions for parallel isolated startup
        async def _start_with_result(cid: str) -> StartResult:
            try:
                return await self.start_one(cid)
            except Exception as e:
                logger.error(f"Unexpected error starting {cid}: {e}")
                return StartResult(cid, False, str(e))
        
        tasks = [_start_with_result(cid) for cid in self._components.keys()]
        results = await asyncio.gather(*tasks, return_exceptions=False)
        
        # Log summary
        succeeded = sum(1 for r in results if r.success)
        failed = len(results) - succeeded
        logger.info(f"Started {succeeded}/{len(results)} components, {failed} failed")
        
        if failed > 0:
            for r in results:
                if not r.success:
                    logger.error(f"  - {r.component_id}: {r.error}")
        
        return results
    
    async def stop_one(self, component_id: str) -> bool:
        """Stop a single component."""
        wrapper = self._components.get(component_id)
        if wrapper:
            return await wrapper.stop()
        return False
    
    async def stop_all(self) -> None:
        """Stop all components gracefully."""
        # Stop health check first
        if self._health_check_task:
            self._health_check_task.cancel()
            try:
                await self._health_check_task
            except asyncio.CancelledError:
                pass
            self._health_check_task = None
        
        # Stop all components in parallel
        async def _stop_one(cid: str) -> None:
            try:
                await self.stop_one(cid)
            except Exception as e:
                logger.error(f"Error stopping {cid}: {e}")
        
        await asyncio.gather(
            *[_stop_one(cid) for cid in self._components.keys()],
            return_exceptions=True
        )
        
        logger.info("All components stopped")
    
    async def start_health_monitoring(self) -> None:
        """Start periodic health check task."""
        if self._health_check_task and not self._health_check_task.done():
            return
        
        self._health_check_task = asyncio.create_task(
            self._health_check_loop(),
            name="supervisor-health-check"
        )
        logger.info(f"Health monitoring started (interval: {self._health_check_interval}s)")
    
    async def _health_check_loop(self) -> None:
        """Periodic health check loop."""
        while True:
            try:
                await asyncio.sleep(self._health_check_interval)
                await self._check_all_health()
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Error in health check loop: {e}")
    
    async def _check_all_health(self) -> None:
        """Check health of all components and restart if needed."""
        for cid, wrapper in self._components.items():
            try:
                if wrapper.state == ComponentState.RUNNING and not wrapper.is_healthy():
                    logger.warning(f"Component {cid} is unhealthy")
                    wrapper.state = ComponentState.DEGRADED
                    
                    if wrapper.can_restart():
                        logger.info(f"Attempting to restart {cid}")
                        await wrapper.stop()
                        wrapper.restart_count += 1
                        await wrapper.start()
                
                elif wrapper.state == ComponentState.FAILED and wrapper.can_restart():
                    logger.info(f"Attempting to restart failed component {cid}")
                    wrapper.restart_count += 1
                    await wrapper.start()
                    
            except Exception as e:
                logger.error(f"Error checking health of {cid}: {e}")
    
    def get_status(self) -> Dict[str, Dict[str, Any]]:
        """Get status of all components."""
        return {
            cid: {
                "state": wrapper.state.name,
                "info": wrapper.info,
                "healthy": wrapper.is_healthy(),
                "error_count": wrapper.error_count,
                "restart_count": wrapper.restart_count,
            }
            for cid, wrapper in self._components.items()
        }
    
    def get_healthy_count(self) -> int:
        """Get count of healthy components."""
        return sum(1 for w in self._components.values() if w.is_healthy())
    
    def get_failed_count(self) -> int:
        """Get count of failed components."""
        return sum(1 for w in self._components.values() 
                   if w.state in (ComponentState.FAILED, ComponentState.DEGRADED))
