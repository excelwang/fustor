"""
Acceptance tests for L1 Consistency Contracts.

These tests validate the black-box behaviors defined in L1-CONTRACTS.md.
They work with both Mock and Real system implementations.
"""
import pytest

from .protocol import Event, EventAction


class TestTombstoneProtection:
    """
    Tests for CONTRACTS.TOMBSTONE_PROTECTION.
    
    Invariant: A deleted file with mtime <= tombstone_ts must not reappear.
    """
    
    def test_stale_event_rejected(self, unified_system):
        """
        Given a file was deleted at logical_time=100
        When an audit reports the file with mtime=90
        Then the event is DISCARDED
        """
        # Given: File exists and gets deleted
        unified_system.setup([{"id": "/data/x.txt", "version": 100}])
        unified_system.submit_event(Event(
            source_id="agent_a",
            item_id="/data/x.txt",
            action=EventAction.DELETE,
            version=100
        ))
        
        # When: Stale audit reports old version
        result = unified_system.submit_event(Event(
            source_id="agent_b",
            item_id="/data/x.txt",
            action=EventAction.CREATE,
            version=90,
            is_audit=True
        ))
        
        # Then: Rejected, file stays deleted
        assert not result.accepted
        assert result.reason == "tombstone_protection"
        obs = unified_system.observe("/data/x.txt")
        assert not obs.exists
    
    def test_reincarnation_accepted(self, unified_system):
        """
        Given a file was deleted at logical_time=100
        When a new event arrives with mtime=150
        Then the event is ACCEPTED (reincarnation)
        """
        # Given: File deleted
        unified_system.setup([{"id": "/data/x.txt", "version": 100}])
        unified_system.submit_event(Event(
            source_id="agent_a",
            item_id="/data/x.txt",
            action=EventAction.DELETE,
            version=100
        ))
        
        # When: New event with newer version
        result = unified_system.submit_event(Event(
            source_id="agent_b",
            item_id="/data/x.txt",
            action=EventAction.CREATE,
            version=150
        ))
        
        # Then: Accepted (reincarnation)
        assert result.accepted
        obs = unified_system.observe("/data/x.txt")
        assert obs.exists
        assert obs.item.version == 150


class TestMtimeArbitration:
    """
    Tests for CONTRACTS.MTIME_ARBITRATION.
    
    Invariant: event.mtime <= node.mtime → DISCARD
    """
    
    def test_stale_update_rejected(self, unified_system):
        """
        Given file exists with mtime=200
        When an event arrives with mtime=150
        Then the event is DISCARDED
        """
        # Given
        unified_system.setup([{"id": "/data/y.txt", "version": 200}])
        
        # When
        result = unified_system.submit_event(Event(
            source_id="agent_a",
            item_id="/data/y.txt",
            action=EventAction.MODIFY,
            version=150
        ))
        
        # Then
        assert not result.accepted
        assert result.reason == "stale"
        obs = unified_system.observe("/data/y.txt")
        assert obs.item.version == 200  # Unchanged
    
    def test_newer_update_accepted(self, unified_system):
        """
        Given file exists with mtime=200
        When an event arrives with mtime=250
        Then the event is ACCEPTED
        """
        # Given
        unified_system.setup([{"id": "/data/y.txt", "version": 200}])
        
        # When
        result = unified_system.submit_event(Event(
            source_id="agent_a",
            item_id="/data/y.txt",
            action=EventAction.MODIFY,
            version=250
        ))
        
        # Then
        assert result.accepted
        obs = unified_system.observe("/data/y.txt")
        assert obs.item.version == 250


class TestEventualConsistency:
    """
    Tests for CONTRACTS.EVENTUAL_CONSISTENCY.
    """
    
    def test_multiple_events_converge(self, unified_system):
        """
        Given multiple sources report events with varying delays
        When the system is quiescent
        Then the view reflects the latest state
        """
        unified_system.setup([])
        
        # Simulate concurrent updates
        unified_system.submit_event(Event(
            source_id="agent_a",
            item_id="/data/z.txt",
            action=EventAction.CREATE,
            version=100
        ))
        unified_system.submit_event(Event(
            source_id="agent_b",
            item_id="/data/z.txt",
            action=EventAction.MODIFY,
            version=150
        ))
        unified_system.submit_event(Event(
            source_id="agent_a",
            item_id="/data/z.txt",
            action=EventAction.MODIFY,
            version=120  # Out of order, should be rejected
        ))
        
        # Wait for consistency
        unified_system.wait_for_consistency()
        
        # Verify final state
        obs = unified_system.observe("/data/z.txt")
        assert obs.exists
        assert obs.item.version == 150  # Latest wins
