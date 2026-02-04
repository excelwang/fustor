"""
Unit tests for Tombstone TTL and Blind-spot Session Reset.

验证文档中描述的两个核心行为：
1. Tombstone 使用 1 小时逻辑时间 TTL 清理
2. Session 重置时清空盲区列表
"""
import pytest
import time
from unittest.mock import MagicMock, AsyncMock

from fustor_view_fs.state import FSState
from fustor_view_fs.tree import TreeManager
from fustor_view_fs.audit import AuditManager
from fustor_view_fs.provider import FSViewProvider


class TestTombstoneTTLCleanup:
    """Test Tombstone 1-hour TTL cleanup logic."""

    @pytest.fixture
    def state(self):
        return FSState(view_id=1)

    @pytest.fixture
    def tree_manager(self, state):
        return TreeManager(state)

    @pytest.fixture
    def audit_manager(self, state, tree_manager):
        return AuditManager(state, tree_manager)

    @pytest.mark.asyncio
    async def test_tombstone_older_than_1hr_is_cleaned(self, state, audit_manager):
        """
        验证: 超过 1 小时的 Tombstone 在 Audit-End 时被清理
        """
        # Setup: 创建一个 2 小时前的 Tombstone
        old_ts = time.time() - 7200  # 2 hours ago
        state.tombstone_list["/old_file.txt"] = (old_ts, old_ts)
        
        # 设置逻辑时钟水位线为当前时间
        state.logical_clock.update(time.time())
        
        # 设置 audit_start 以允许 handle_end 执行
        state.last_audit_start = time.time() - 100
        
        # Action: 执行 Audit End
        await audit_manager.handle_end()
        
        # Assert: 旧的 Tombstone 被清理
        assert "/old_file.txt" not in state.tombstone_list, \
            "Tombstone older than 1 hour should be cleaned"

    @pytest.mark.asyncio
    async def test_tombstone_younger_than_1hr_is_preserved(self, state, audit_manager):
        """
        验证: 不到 1 小时的 Tombstone 在 Audit-End 时保留
        """
        # Setup: 创建一个 30 分钟前的 Tombstone
        recent_ts = time.time() - 1800  # 30 minutes ago
        state.tombstone_list["/recent_file.txt"] = (recent_ts, recent_ts)
        
        # 设置逻辑时钟水位线为当前时间
        state.logical_clock.update(time.time())
        
        # 设置 audit_start
        state.last_audit_start = time.time() - 100
        
        # Action: 执行 Audit End
        await audit_manager.handle_end()
        
        # Assert: 较新的 Tombstone 被保留
        assert "/recent_file.txt" in state.tombstone_list, \
            "Tombstone younger than 1 hour should be preserved"


class TestBlindSpotSessionReset:
    """Test Blind-spot list clearing on session start."""

    @pytest.fixture
    def provider(self):
        return FSViewProvider(id="test_view", view_id="1")

    @pytest.mark.asyncio
    async def test_blind_spot_additions_cleared_on_session_start(self, provider):
        """
        验证: 新 Session 开始时清空 blind_spot_additions
        参考: CONSISTENCY_DESIGN.md §4.4
        """
        # Setup: 添加一些盲区条目
        provider.state.blind_spot_additions.add("/blind_file_1.txt")
        provider.state.blind_spot_additions.add("/blind_file_2.txt")
        
        assert len(provider.state.blind_spot_additions) == 2
        
        # Action: 启动新 Session
        await provider.on_session_start()
        
        # Assert: 盲区列表被清空
        assert len(provider.state.blind_spot_additions) == 0, \
            "Blind-spot additions should be cleared on session start"

    @pytest.mark.asyncio
    async def test_blind_spot_deletions_cleared_on_session_start(self, provider):
        """
        验证: 新 Session 开始时清空 blind_spot_deletions
        """
        # Setup: 添加一些盲区删除条目
        provider.state.blind_spot_deletions.add("/deleted_blind_1.txt")
        provider.state.blind_spot_deletions.add("/deleted_blind_2.txt")
        
        assert len(provider.state.blind_spot_deletions) == 2
        
        # Action: 启动新 Session
        await provider.on_session_start()
        
        # Assert: 盲区删除列表被清空
        assert len(provider.state.blind_spot_deletions) == 0, \
            "Blind-spot deletions should be cleared on session start"

    @pytest.mark.asyncio
    async def test_audit_state_also_cleared_on_session_start(self, provider):
        """
        验证: 新 Session 开始时同时清空 audit 状态
        """
        # Setup: 设置 audit 状态
        provider.state.last_audit_start = time.time()
        provider.state.audit_seen_paths.add("/some/path")
        
        # Action: 启动新 Session
        await provider.on_session_start()
        
        # Assert: audit 状态被清空
        assert provider.state.last_audit_start is None
        assert len(provider.state.audit_seen_paths) == 0
