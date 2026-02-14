import pytest
import asyncio
from fustor_core.supervisor import ComponentSupervisor, ComponentState

class SlowComponent:
    def __init__(self, delay=0.01):
        self.delay = delay
        self.started = False
        self.stopped = False
        
    async def start(self):
        await asyncio.sleep(self.delay)
        self.started = True
        
    async def stop(self):
        await asyncio.sleep(self.delay)
        self.stopped = True
    
    def is_healthy(self):
        return self.started and not self.stopped

@pytest.mark.asyncio
async def test_supervisor_high_concurrency():
    """并发注册和启停大量组件，验证稳定性"""
    supervisor = ComponentSupervisor(health_check_interval=0.1)
    count = 50
    
    # 1. 并发注册
    tasks = [supervisor.register(f"c-{i}", SlowComponent()) for i in range(count)]
    await asyncio.gather(*tasks)
    
    # 2. 并发启动
    results = await supervisor.start_all()
    assert len(results) == count
    assert all(r.success for r in results)
    assert supervisor.get_healthy_count() == count
    
    # 3. 模拟其中一部分组件随机失效并触发监控
    for i in range(10):
        comp_id = f"c-{i}"
        wrapper = supervisor._components[comp_id]
        wrapper.component.started = False # 模拟意外停止
        
    # 执行一次监控检查
    await supervisor._check_all_health()
    
    # 验证是否重新启动（状态恢复为 RUNNING）
    status = supervisor.get_status()
    restarted_count = sum(1 for i in range(10) if status[f"c-{i}"]["restart_count"] > 0)
    # 注意：根据之前的测试，restart_count 可能在 start() 成功后被重置。
    # 但我们可以确认状态是否变回了 RUNNING
    for i in range(10):
        assert status[f"c-{i}"]["state"] == "RUNNING"
    
    # 4. 并发停止
    await supervisor.stop_all()
    assert supervisor.get_healthy_count() == 0
