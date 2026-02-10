# fusion/tests/runtime/test_fusion_pipe_handler_recovery.py
"""
U3: ViewHandler 永久降级不可恢复。
验证当前实现中 Handler 被加入 _disabled_handlers 后永远不会被移除。

被测代码: fusion/src/fustor_fusion/runtime/fusion_pipe.py
  → _record_handler_error() + _dispatch_to_handlers()
"""
import pytest
import asyncio
from fustor_core.event import EventBase
from fustor_core.event.types import EventType
from fustor_fusion.runtime import FusionPipe


class FlakeyViewHandler:
    """模拟先失败后恢复的 ViewHandler。"""
    schema_name = "flakey"

    def __init__(self, fail_count: int = 5):
        self.id = "flakey-handler"
        self.call_count = 0
        self.fail_count = fail_count
        self.processed = []

    async def initialize(self): pass
    async def close(self): pass

    async def process_event(self, event):
        self.call_count += 1
        if self.call_count <= self.fail_count:
            raise RuntimeError(f"Simulated failure #{self.call_count}")
        self.processed.append(event)
        return True

    async def on_session_start(self): pass
    async def on_session_close(self): pass
    def get_data_view(self, **kwargs): return {}
    def get_stats(self): return {}


class TestHandlerDegradation:
    """验证 ViewHandler 降级和恢复行为。"""

    @pytest.mark.asyncio
    async def test_handler_is_disabled_after_max_errors(self):
        """验证当前行为: Handler 达到 MAX_HANDLER_ERRORS 后被永久禁用。"""
        handler = FlakeyViewHandler(fail_count=100)  # 永远失败
        pipe = FusionPipe(
            pipe_id="test",
            config={"view_id": "1", "allow_concurrent_push": True},
            view_handlers=[handler]
        )
        await pipe.start()

        # 发送足够多的事件触发 MAX_HANDLER_ERRORS
        for i in range(pipe.MAX_HANDLER_ERRORS + 5):
            event = EventBase(
                event_type=EventType.INSERT,
                event_schema="s", table="t",
                rows=[{"id": i}], fields=["id"], index=i
            )
            await pipe._dispatch_to_handlers(event)

        # 断言: Handler 已被禁用
        assert "flakey-handler" in pipe._disabled_handlers, \
            "Handler 应在达到 MAX_HANDLER_ERRORS 后被禁用"

        await pipe.stop()

    @pytest.mark.asyncio
    async def test_disabled_handler_recovers(self):
        """
        验证修复后: 被禁用的 Handler 在冷却期后恢复。
        """
        handler = FlakeyViewHandler(fail_count=5)  # 5 次后恢复
        pipe = FusionPipe(
            pipe_id="test",
            config={
                "view_id": "1", 
                "allow_concurrent_push": True,
                "handler_recovery_interval": 0.5  # 短冷却时间方便测试
            },
            view_handlers=[handler]
        )
        pipe.MAX_HANDLER_ERRORS = 3  # 降低阈值方便测试
        await pipe.start()

        # 触发足够错误使 Handler 被禁用
        for i in range(5):
            event = EventBase(
                event_type=EventType.INSERT,
                event_schema="s", table="t",
                rows=[{"id": i}], fields=["id"], index=i
            )
            await pipe._dispatch_to_handlers(event)

        assert "flakey-handler" in pipe._disabled_handlers

        # 等待冷却期过去
        await asyncio.sleep(0.6)

        # 现在 Handler 已"恢复"(fail_count=5, 已超过)，发送更多事件
        # 第一次调用 _dispatch_to_handlers 会检测并恢复 Handler
        for i in range(10, 20):
            event = EventBase(
                event_type=EventType.INSERT,
                event_schema="s", table="t",
                rows=[{"id": i}], fields=["id"], index=i
            )
            await pipe._dispatch_to_handlers(event)

        # 修复验证: Handler 应已从 disabled 移除，且处理了新事件
        assert "flakey-handler" not in pipe._disabled_handlers, \
            "Handler 应在冷却期后被重新启用"
        assert len(handler.processed) > 0, \
            "恢复后的 Handler 应能处理事件"

        await pipe.stop()
