# agent/src/fustor_agent/runtime/sender_handler_adapter.py
"""
Adapter to wrap a Sender transport as a SenderHandler for use in AgentPipe.

This allows the existing sender-http and other transport implementations
to be used with the new Pipe-based architecture.
"""
import logging
from typing import Any, Dict, List, Optional, Tuple, TYPE_CHECKING

from fustor_core.pipe.sender import SenderHandler
from fustor_core.transport import Sender
from fustor_core.models.config import SenderConfig

if TYPE_CHECKING:
    from fustor_agent.services.drivers.sender_driver import SenderDriverService

logger = logging.getLogger("fustor_agent")


class SenderHandlerAdapter(SenderHandler):
    """
    Adapts a Sender transport to the SenderHandler interface.
    
    This adapter bridges the gap between:
    - fustor_core.transport.Sender (transport/protocol layer)
    - fustor_core.pipe.sender.SenderHandler (pipe/handler layer)
    
    Example usage:
        from fustor_sender_http import HTTPSender
        
        sender = HTTPSender(
            sender_id="my-sender",
            endpoint="http://fusion:8080",
            credential={"key": "my-api-key"}
        )
        handler = SenderHandlerAdapter(sender)
        
        # Now usable with AgentPipe
        pipe = AgentPipe(
            source_handler=source,
            sender_handler=handler,
            ...
        )
    """
    
    def __init__(
        self,
        sender: Sender,
        config: Optional[Dict[str, Any]] = None
    ):
        """
        Initialize the adapter.
        
        Args:
            sender: The underlying Sender transport instance
            config: Optional configuration overrides
        """
        super().__init__(
            handler_id=sender.id,
            config=config or sender.config
        )
        self._sender = sender
        self.schema_name = "sender-adapter"
        self._connected = False
    
    @property
    def sender(self) -> Sender:
        """Access the underlying sender."""
        return self._sender
    
    async def initialize(self) -> None:
        """Initialize the handler (connect the underlying sender)."""
        if not self._connected:
            await self._sender.connect()
            self._connected = True
            logger.debug(f"SenderHandlerAdapter {self.id} initialized")
    
    async def close(self) -> None:
        """Close the handler and release resources."""
        if self._connected:
            await self._sender.close()
            self._connected = False
            logger.debug(f"SenderHandlerAdapter {self.id} closed")
    
    async def create_session(
        self,
        task_id: str,
        source_type: str,
        session_timeout_seconds: int = 30,
        **kwargs
    ) -> Tuple[str, Dict[str, Any]]:
        """
        Create a new session with Fusion.
        
        Delegates to the underlying Sender's create_session method.
        """
        await self.initialize()
        
        result = await self._sender.create_session(
            task_id,
            source_type=source_type,
            session_timeout_seconds=session_timeout_seconds,
            **kwargs
        )
        
        # Handle both dict return and tuple return (for compatibility)
        if isinstance(result, tuple):
            session_id, session_data = result
        else:
            # Sender returns dict directly
            session_data = result
            session_id = session_data.get("session_id", f"sess-{task_id}")

        metadata = {
            "role": session_data.get("role", "follower"),
            "session_timeout_seconds": session_data.get("session_timeout_seconds", session_timeout_seconds),
            "source_type": source_type,
            **{k: v for k, v in session_data.items() if k not in ("session_id",)}
        }
        
        return session_id, metadata
    
    async def send_heartbeat(self, session_id: str, **kwargs) -> Dict[str, Any]:
        """
        Send a heartbeat to keep the session alive.
        
        Delegates to the underlying Sender's heartbeat method.
        """
        response = await self._sender.heartbeat(**kwargs)
        
        return {
            "role": response.get("role", response.get("current_role")),
            "session_id": session_id,
            "status": response.get("status", "ok"),
            **{k: v for k, v in response.items() if k not in ("role", "current_role")}
        }
    
    async def send_batch(
        self,
        session_id: str,
        events: List[Any],
        batch_context: Optional[Dict[str, Any]] = None
    ) -> Tuple[bool, Dict[str, Any]]:
        """
        Send a batch of events to Fusion.
        
        Delegates to the underlying Sender's send_events method.
        """
        context = batch_context or {}
        phase = context.get("phase", "message")
        is_final = context.get("is_final", False)
        
        # Map phase to source_type
        source_type_map = {
            "snapshot": "snapshot",
            "realtime": "message",
            "audit": "audit",
            "sentinel": "message",
            "message": "message",
        }
        source_type = source_type_map.get(phase, "message")
        
        # Handle audit signals
        if phase == "audit":
            if context.get("is_start"):
                await self._sender.signal_audit_start()
            
        # All V2 senders must implement send_events
        if hasattr(self._sender, "send_events"):
            response = await self._sender.send_events(
                events=events,
                source_type=source_type,
                is_end=is_final
            ) or {}
        else:
            raise AttributeError(f"Sender {self.id} has no 'send_events' method (legacy 'push' is no longer supported)")
        
        success = response.get("success", False)
        
        # Handle audit end signal
        if phase == "audit" and is_final and success:
            await self._sender.signal_audit_end()
            
        return success, response
    
    async def close_session(self, session_id: str) -> bool:
        """
        Close the session.
        
        Delegates to the underlying Sender's close_session method.
        """
        try:
            await self._sender.close_session()
            return True
        except Exception as e:
            logger.warning(f"Error closing session {session_id}: {e}")
            return False
    
    async def get_latest_committed_index(self, session_id: str) -> int:
        """
        Get the latest committed event index from Fusion.
        
        Delegates to the underlying Sender's get_latest_committed_index method.
        """
        if hasattr(self._sender, "get_latest_committed_index"):
            return await self._sender.get_latest_committed_index(session_id)
        
        # Fallback for senders that don't support resume (safest default is 0 or -1?)
        # 0 means "start from beginning" which might cause duplication.
        # -1 means "start from now" which might cause loss.
        # Given D-04 concerns data integrity, we should default to 0 (replay is safer than loss) 
        # or error out if critical. But for now, 0 seems appropriate default if not supported.
        # This is expected for some sender types (like Echo or legacy wrappers)
        logger.debug(f"Sender {self.id} does not support get_latest_committed_index. Defaulting to 0.")
        return 0

    async def test_connection(self, **kwargs) -> Tuple[bool, str]:
        """Test connectivity by attempting to connect."""
        try:
            await self.initialize()
            return (True, "Connection successful")
        except Exception as e:
            return (False, f"Connection failed: {e}")
    
    # --- Sentinel support ---
    
    async def get_sentinel_tasks(self) -> Optional[Dict[str, Any]]:
        """Get sentinel verification tasks from Fusion."""
        return await self._sender.get_sentinel_tasks()
    
    async def submit_sentinel_results(self, results: Dict[str, Any]) -> bool:
        """Submit sentinel verification results to Fusion."""
        return await self._sender.submit_sentinel_results(results)


class SenderHandlerFactory:
    """
    Factory for creating SenderHandler instances from configuration.
    
    This factory uses the existing SenderDriverService to create
    Sender instances and wraps them with SenderHandlerAdapter.
    """
    
    def __init__(self, sender_driver_service: "SenderDriverService"):
        """
        Initialize the factory.
        
        Args:
            sender_driver_service: Service for creating sender instances
        """
        self._driver_service = sender_driver_service
    
    def create_handler(
        self,
        sender_config: SenderConfig,
        handler_id: Optional[str] = None
    ) -> SenderHandlerAdapter:
        """
        Create a SenderHandler from configuration.
        
        Args:
            sender_config: Sender configuration
            handler_id: Optional override for handler ID
        
        Returns:
            A SenderHandlerAdapter wrapping the appropriate Sender
        """
        # Get credential dict from Pydantic model
        credential_dict = {}
        if hasattr(sender_config.credential, "model_dump"):
            credential_dict = sender_config.credential.model_dump()
        elif hasattr(sender_config.credential, "dict"):
            credential_dict = sender_config.credential.dict()
        else:
            credential_dict = dict(sender_config.credential)
        
        # Get the driver class
        driver_type = sender_config.driver
        driver_class = self._driver_service._get_driver_by_type(driver_type)
        
        # Create sender instance
        sender = driver_class(
            sender_id=handler_id or f"sender-{driver_type}",
            endpoint=sender_config.uri,
            credential=credential_dict,
            config={
                "batch_size": sender_config.batch_size,
                **sender_config.driver_params
            }
        )
        
        # Wrap in adapter
        return SenderHandlerAdapter(sender)


# Convenience function for simple use cases
def create_sender_handler_from_config(
    sender_config: SenderConfig,
    sender_driver_service: "SenderDriverService",
    handler_id: Optional[str] = None
) -> SenderHandlerAdapter:
    """
    Create a SenderHandler from configuration.
    
    This is a convenience function that creates a factory and uses it.
    
    Args:
        sender_config: Sender configuration
        sender_driver_service: Driver service for loading sender classes
        handler_id: Optional handler ID
        
    Returns:
        A SenderHandlerAdapter instance
    """
    factory = SenderHandlerFactory(sender_driver_service)
    return factory.create_handler(sender_config, handler_id)
