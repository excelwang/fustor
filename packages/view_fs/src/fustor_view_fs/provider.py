from .base import FSViewBase
from .memory_mixin import MemoryMixin
from .event_mixin import EventMixin
from .audit_mixin import AuditMixin
from .integrity_mixin import IntegrityMixin
from .view_mixin import ViewMixin

class FSViewProvider(
    MemoryMixin, 
    EventMixin, 
    AuditMixin, 
    IntegrityMixin, 
    ViewMixin, 
    FSViewBase
):
    """
    Consistent File System View Provider.
    Coordinates various mixins to maintain a fused, consistent view of the FS
    using Smart Merge arbitration logic.
    """
    def __init__(self, datastore_id: int, config: dict = None, hot_file_threshold: float = 30.0):
        """
        Initialize FSViewProvider.
        
        Args:
            datastore_id: The datastore this provider manages.
            config: Optional config dict (for ViewDriver compatibility).
            hot_file_threshold: TTL for suspect files (seconds).
        """
        super().__init__(datastore_id, config=config, hot_file_threshold=hot_file_threshold)

    async def on_session_start(self, session_id: str):
        """Called when a new session joins. Resets blind-spot lists for fresh calibration."""
        async with self._global_exclusive_lock():
            self._blind_spot_deletions.clear()
            self._blind_spot_additions.clear()
            self.logger.info(f"Blind spot lists reset for datastore {self.datastore_id} due to session {session_id}")

    async def on_session_close(self, session_id: str):
        """Called when a session terminates. Currently a no-op, for future use."""
        self.logger.debug(f"Session {session_id} closed for datastore {self.datastore_id}")

    async def reset(self):
        """Clears all in-memory data for this datastore."""
        async with self._global_exclusive_lock():
            from .nodes import DirectoryNode
            self._root = DirectoryNode("", "/")
            self._directory_path_map = {"/": self._root}
            self._file_path_map = {}
            self._tombstone_list = {}
            self._suspect_list = {}
            self._suspect_heap = []
            self._last_audit_start = None
            self._audit_seen_paths.clear()
            self._blind_spot_deletions.clear()
            self._blind_spot_additions.clear()
            self.logger.info(f"View state reset for datastore {self.datastore_id}")

    async def get_data_view(self, **kwargs) -> dict:
        """Required by the ViewDriver ABC."""
        return await self.get_directory_tree(**kwargs)  # type: ignore
