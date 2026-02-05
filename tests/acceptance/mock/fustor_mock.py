"""
Mock implementation of UnifiedDataSystem.

Implements L1 Contract behaviors WITHOUT depending on real Fustor code.
This validates the CONTRACT LOGIC, not the implementation.
"""
from typing import Any, Optional

from ..protocol import (
    ConsistencyState,
    DataItem,
    Event,
    EventAction,
    Observation,
    SubmitResult,
    UnifiedDataSystem,
)


class MockFustorSystem(UnifiedDataSystem):
    """
    In-memory implementation based on L1 contract behavior.
    
    Implements:
    - CONTRACTS.TOMBSTONE_PROTECTION
    - CONTRACTS.MTIME_ARBITRATION
    - CONTRACTS.EVENTUAL_CONSISTENCY
    """
    
    def __init__(self):
        self.tree: dict[str, DataItem] = {}
        self.tombstones: dict[str, Any] = {}  # item_id -> version (logical_ts)
        self.hot_threshold: float = 30.0
        self.current_time: float = 1000.0  # Simulated logical time
    
    def setup(self, initial_items: list[dict]) -> None:
        self.tree.clear()
        self.tombstones.clear()
        for item in initial_items:
            self.tree[item["id"]] = DataItem(
                id=item["id"],
                version=item.get("version", self.current_time),
                metadata=item.get("metadata", {})
            )
    
    def submit_event(self, event: Event) -> SubmitResult:
        item_id = event.item_id
        
        # === CONTRACTS.TOMBSTONE_PROTECTION ===
        if item_id in self.tombstones:
            tomb_version = self.tombstones[item_id]
            if self._version_lte(event.version, tomb_version):
                return SubmitResult(accepted=False, reason="tombstone_protection")
            else:
                # Reincarnation
                del self.tombstones[item_id]
        
        # === Handle DELETE ===
        if event.action == EventAction.DELETE:
            if item_id in self.tree:
                del self.tree[item_id]
                self.tombstones[item_id] = event.version
                return SubmitResult(accepted=True)
            return SubmitResult(accepted=False, reason="not_found")
        
        # === CONTRACTS.MTIME_ARBITRATION ===
        if item_id in self.tree:
            existing = self.tree[item_id]
            if self._version_lte(event.version, existing.version):
                return SubmitResult(accepted=False, reason="stale")
        
        # === Accept event ===
        is_suspect = self._is_suspect(event.version)
        self.tree[item_id] = DataItem(
            id=item_id,
            version=event.version,
            metadata={
                "blind_spot": event.is_audit,
                "integrity_suspect": is_suspect
            }
        )
        return SubmitResult(accepted=True)
    
    def observe(self, item_id: str) -> Observation:
        if item_id not in self.tree:
            return Observation(exists=False)
        
        item = self.tree[item_id]
        state = ConsistencyState.SUSPECT if item.metadata.get("integrity_suspect") else ConsistencyState.STABLE
        return Observation(exists=True, item=item, consistency_state=state)
    
    def list_items(self, prefix: Optional[str] = None) -> list[str]:
        if prefix:
            return [k for k in self.tree.keys() if k.startswith(prefix)]
        return list(self.tree.keys())
    
    def wait_for_consistency(self, timeout_sec: float = 10.0) -> bool:
        # Mock is always consistent
        return True
    
    def teardown(self) -> None:
        self.tree.clear()
        self.tombstones.clear()
    
    # === Helpers ===
    
    def _version_lte(self, v1: Any, v2: Any) -> bool:
        """Compare versions. For mtime-based, this is numeric comparison."""
        return float(v1) <= float(v2)
    
    def _is_suspect(self, version: Any) -> bool:
        """Check if version is within hot threshold (suspect)."""
        age = self.current_time - float(version)
        return age < self.hot_threshold
    
    def advance_time(self, delta: float) -> None:
        """Test helper: advance simulated time."""
        self.current_time += delta
