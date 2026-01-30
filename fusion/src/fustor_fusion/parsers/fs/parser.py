from .base import ParserBase
from .memory_mixin import MemoryMixin
from .event_mixin import EventMixin
from .audit_mixin import AuditMixin
from .integrity_mixin import IntegrityMixin
from .view_mixin import ViewMixin

class DirectoryStructureParser(
    MemoryMixin, 
    EventMixin, 
    AuditMixin, 
    IntegrityMixin, 
    ViewMixin, 
    ParserBase
):
    """
    Main FS Parser class that combines various functional mixins.
    Provides Smart Merge logic for directory consistency arbitration.
    """
    def __init__(self, datastore_id: int):
        # Multiple inheritance handles this correctly since ParserBase is last
        # and all Mixins also inherit from it (though ParserBase.__init__ is where state is).
        super().__init__(datastore_id)

    async def on_session_start(self, session_id: str):
        """Called when a new session joins. Resets blind-spot lists for fresh calibration."""
        async with self._global_exclusive_lock():
            self._blind_spot_deletions.clear()
            self._blind_spot_additions.clear()
            self.logger.info(f"Blind spot lists reset for datastore {self.datastore_id} due to session {session_id}")

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
            self.logger.info(f"Parser state reset for datastore {self.datastore_id}")

    async def get_data_view(self, **kwargs) -> dict:
        """Required by the Parser Protocol."""
        return await self.get_directory_tree(**kwargs) # type: ignore
