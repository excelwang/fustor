"""
Fustor Pusher Fusion - DEPRECATED

This package is deprecated. Please use fustor-sender-http instead.

All exports are re-exported from fustor_sender_http for backward compatibility.
"""
import warnings

warnings.warn(
    "fustor_pusher_fusion is deprecated. Use fustor_sender_http instead.",
    DeprecationWarning,
    stacklevel=2
)

# Re-export from new package for backward compatibility
from fustor_sender_http import HTTPSender

# Legacy alias
class FusionDriver(HTTPSender):
    """
    DEPRECATED: Use fustor_sender_http.HTTPSender instead.
    
    This class is provided for backward compatibility with existing code
    that uses the PusherDriver interface.
    """
    
    def __init__(self, id: str, config):
        """
        Initialize from legacy SenderConfig.
        
        Args:
            id: Driver ID
            config: SenderConfig object (legacy)
        """
        # Extract fields from legacy SenderConfig
        endpoint = config.endpoint
        credential = {"key": config.credential.key} if hasattr(config.credential, 'key') else {}
        super().__init__(
            sender_id=id,
            endpoint=endpoint,
            credential=credential,
            config={}
        )
    
    # Map legacy method names
    async def push(self, events, **kwargs):
        """Legacy push method - maps to send_events."""
        source_type = kwargs.get("source_type", "message")
        is_end = kwargs.get("is_snapshot_end", False)
        return await self.send_events(events, source_type=source_type, is_end=is_end)
    
    @classmethod
    async def get_needed_fields(cls, **kwargs):
        """Legacy method - returns empty dict."""
        return {}


__all__ = ["FusionDriver", "HTTPSender"]
