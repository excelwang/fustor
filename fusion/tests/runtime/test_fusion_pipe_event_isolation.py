# fusion/tests/runtime/test_fusion_pipe_event_isolation.py
"""
U2: FusionPipe.process_events 对畸形事件的隔离能力。
Spec: 05-Stability.md §1.2 — 单个事件失败不应中断整个 Batch。

被测代码: fusion/src/fustor_fusion/runtime/fusion_pipe.py → process_events()

已知 BUG: process_events 中 model_validate 失败会直接抛异常，
不会跳过畸形事件继续处理其余合法事件。
"""
import pytest
import asyncio
from fustor_fusion.runtime import FusionPipe


class MockViewHandler:
    """简化的 ViewHandler Mock。"""
    schema_name = "test"

    def __init__(self):
        self.id = "mock-view"
        self.events = []

    async def initialize(self): pass
    async def close(self): pass

    async def process_event(self, event):
        self.events.append(event)
        return True

    async def on_session_start(self, **kwargs): pass
    async def on_session_close(self, **kwargs): pass
    def get_data_view(self, **kwargs): return {}
    def get_stats(self): return {}


@pytest.fixture
def fusion_pipe_with_handler():
    handler = MockViewHandler()
    pipe = FusionPipe(
        pipe_id="test",
        config={"view_id": "1", "allow_concurrent_push": True},
        view_handlers=[handler]
    )
    pipe.pipe_id = pipe.id # Inject required attribute
    return pipe, handler


class TestProcessEventIsolation:
    """验证 process_events 中单个畸形事件不毒化整个 Batch。"""

    @pytest.mark.asyncio
    async def test_malformed_event_is_skipped(
        self, fusion_pipe_with_handler
    ):
        """
        验证修复后: process_events 会跳过畸形事件，继续处理合法事件。
        """
        pipe, handler = fusion_pipe_with_handler
        await pipe.start()

        good_events = [
            {
                "event_type": "insert", "event_schema": "test", "table": "files",
                "rows": [{"path": "/a.txt"}], "fields": ["path"], "index": 1,
                "metadata": {}
            },
            {
                "event_type": "insert", "event_schema": "test", "table": "files",
                "rows": [{"path": "/b.txt"}], "fields": ["path"], "index": 2,
                "metadata": {}
            }
        ]
        bad_event = {"garbage_field": True, "not_a_valid_event": 123}
        batch = [good_events[0], bad_event, good_events[1]]

        # 修复验证: 应该成功处理 2 个合法事件，忽略 1 个畸形事件
        result = await pipe.process_events(batch, session_id="sess-1")
        
        assert result["success"] is True
        assert result["count"] == 2  # Only successfully processed events
        assert result["skipped"] == 1  # 1 malformed event skipped
        
        # 验证 Handler 确实收到了 2 个事件
        # wait for async processing
        for _ in range(10):
            if len(handler.events) >= 2: break
            await asyncio.sleep(0.01)
            
        assert len(handler.events) == 2
        assert handler.events[0].rows[0]["path"] == "/a.txt"
        assert handler.events[1].rows[0]["path"] == "/b.txt"

        await pipe.stop()

    @pytest.mark.asyncio
    async def test_all_valid_events_processed_normally(
        self, fusion_pipe_with_handler
    ):
        """基线: 全部合法事件应正常处理。"""
        pipe, handler = fusion_pipe_with_handler
        await pipe.start()

        events = [
            {
                "event_type": "insert", "event_schema": "test", "table": "files",
                "rows": [{"path": f"/{i}.txt"}], "fields": ["path"], "index": i,
                "metadata": {}
            }
            for i in range(5)
        ]

        result = await pipe.process_events(events, session_id="sess-1")
        assert result["success"] is True
        assert result["count"] == 5

        await pipe.stop()
