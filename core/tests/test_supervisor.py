"""
Tests for ComponentSupervisor.
"""
import pytest
import asyncio
from unittest.mock import AsyncMock, MagicMock

from fustor_core.supervisor import (
    ComponentSupervisor,
    ComponentWrapper,
    ComponentState,
    RestartPolicy,
    StartResult,
)


class MockComponent:
    """Mock component for testing."""
    
    def __init__(self, should_fail_start: bool = False, should_fail_health: bool = False):
        self.started = False
        self.stopped = False
        self.should_fail_start = should_fail_start
        self.should_fail_health = should_fail_health
    
    async def start(self):
        if self.should_fail_start:
            raise RuntimeError("Mock start failure")
        self.started = True
    
    async def stop(self):
        self.stopped = True
        self.started = False
    
    def is_healthy(self) -> bool:
        return not self.should_fail_health and self.started


class TestComponentWrapper:
    """Tests for ComponentWrapper."""
    
    @pytest.mark.asyncio
    async def test_start_success(self):
        component = MockComponent()
        wrapper = ComponentWrapper(component=component)
        
        success = await wrapper.start()
        
        assert success is True
        assert wrapper.state == ComponentState.RUNNING
        assert component.started is True
    
    @pytest.mark.asyncio
    async def test_start_failure(self):
        component = MockComponent(should_fail_start=True)
        wrapper = ComponentWrapper(component=component)
        
        success = await wrapper.start()
        
        assert success is False
        assert wrapper.state == ComponentState.FAILED
        assert wrapper.error_count == 1
        assert wrapper.last_error is not None
    
    @pytest.mark.asyncio
    async def test_stop_success(self):
        component = MockComponent()
        wrapper = ComponentWrapper(component=component)
        await wrapper.start()
        
        success = await wrapper.stop()
        
        assert success is True
        assert wrapper.state == ComponentState.STOPPED
        assert component.stopped is True
    
    def test_is_healthy_running(self):
        component = MockComponent()
        component.started = True
        wrapper = ComponentWrapper(component=component)
        wrapper.state = ComponentState.RUNNING
        
        assert wrapper.is_healthy() is True
    
    def test_is_healthy_not_running(self):
        component = MockComponent()
        wrapper = ComponentWrapper(component=component)
        
        assert wrapper.is_healthy() is False
    
    def test_can_restart_on_failure_policy(self):
        component = MockComponent()
        wrapper = ComponentWrapper(
            component=component,
            restart_policy=RestartPolicy.ON_FAILURE,
            max_restarts=3
        )
        
        assert wrapper.can_restart() is True
        
        wrapper.restart_count = 3
        assert wrapper.can_restart() is False
    
    def test_can_restart_never_policy(self):
        component = MockComponent()
        wrapper = ComponentWrapper(
            component=component,
            restart_policy=RestartPolicy.NEVER
        )
        
        assert wrapper.can_restart() is False


class TestComponentSupervisor:
    """Tests for ComponentSupervisor."""
    
    @pytest.mark.asyncio
    async def test_register_component(self):
        supervisor = ComponentSupervisor()
        component = MockComponent()
        
        await supervisor.register("test-1", component)
        
        assert "test-1" in supervisor._components
    
    @pytest.mark.asyncio
    async def test_start_all_success(self):
        supervisor = ComponentSupervisor()
        c1 = MockComponent()
        c2 = MockComponent()
        
        await supervisor.register("c1", c1)
        await supervisor.register("c2", c2)
        
        results = await supervisor.start_all()
        
        assert len(results) == 2
        assert all(r.success for r in results)
        assert c1.started is True
        assert c2.started is True
    
    @pytest.mark.asyncio
    async def test_start_all_partial_failure(self):
        """Test that one component failing doesn't block others."""
        supervisor = ComponentSupervisor()
        c1 = MockComponent()
        c2 = MockComponent(should_fail_start=True)
        c3 = MockComponent()
        
        await supervisor.register("c1", c1)
        await supervisor.register("c2-will-fail", c2)
        await supervisor.register("c3", c3)
        
        results = await supervisor.start_all()
        
        # c1 and c3 should succeed, c2 should fail
        succeeded = [r for r in results if r.success]
        failed = [r for r in results if not r.success]
        
        assert len(succeeded) == 2
        assert len(failed) == 1
        assert failed[0].component_id == "c2-will-fail"
        
        # Verify c1 and c3 are running despite c2's failure
        assert c1.started is True
        assert c3.started is True
    
    @pytest.mark.asyncio
    async def test_stop_all(self):
        supervisor = ComponentSupervisor()
        c1 = MockComponent()
        c2 = MockComponent()
        
        await supervisor.register("c1", c1)
        await supervisor.register("c2", c2)
        await supervisor.start_all()
        
        await supervisor.stop_all()
        
        assert c1.stopped is True
        assert c2.stopped is True
    
    @pytest.mark.asyncio
    async def test_get_status(self):
        supervisor = ComponentSupervisor()
        c1 = MockComponent()
        c2 = MockComponent(should_fail_start=True)
        
        await supervisor.register("running", c1)
        await supervisor.register("failed", c2)
        await supervisor.start_all()
        
        status = supervisor.get_status()
        
        assert status["running"]["state"] == "RUNNING"
        assert status["failed"]["state"] == "FAILED"
    
    @pytest.mark.asyncio
    async def test_healthy_and_failed_counts(self):
        supervisor = ComponentSupervisor()
        c1 = MockComponent()
        c2 = MockComponent(should_fail_start=True)
        c3 = MockComponent()
        
        await supervisor.register("c1", c1)
        await supervisor.register("c2", c2)
        await supervisor.register("c3", c3)
        await supervisor.start_all()
        
        assert supervisor.get_healthy_count() == 2
        assert supervisor.get_failed_count() == 1
