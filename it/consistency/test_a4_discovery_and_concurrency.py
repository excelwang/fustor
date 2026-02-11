# it/consistency/test_a4_discovery_and_concurrency.py
"""
Test A4: Discovery API and Concurrency Control.

验证:
1. 视图发现接口 (Discovery API): 仅凭 API Key 识别 view_id。
2. 并发冲突控制 (Concurrency Control): 当 allow_concurrent_push 为 false 时，拒绝第二个 Session 会话。
3. 心跳异常响应 (Obsoleted Session): 验证 419 响应的 Payload。
"""
import pytest
import time
import requests
from ..utils import docker_manager
from ..conftest import CONTAINER_FUSION
from ..fixtures.constants import (
    VIEW_READY_TIMEOUT,
    SHORT_TIMEOUT,
    FAST_POLL_INTERVAL
)

class TestDiscoveryAndConcurrency:
    """Test Discovery API and session concurrency/obsolescence."""

    def test_api_key_discovery(self, fusion_client, test_api_key, test_view):
        """
        验证 GET /api/v1/pipe/session/ 能够根据 Key 自动发现 view_id。
        """
        # 使用 raw request
        resp = fusion_client.api_request(
            "GET",
            "pipe/session/",
            headers={"X-API-Key": test_api_key["key"]}
        )
        assert resp.status_code == 200, f"Discovery API failed: {resp.text}"
        data = resp.json()
        assert data["view_id"] == test_view["id"]
        assert data["status"] == "authorized"

    def test_concurrent_push_conflict(self, fusion_client, test_api_key, test_view):
        """
        验证当 allow_concurrent_push=false 时，并发推送冲突。
        """
        view_id = test_view["id"]
        api_key = "test-strict-key-789"
        
        payload1 = {
            "task_id": "task-1",
            "client_info": {"agent_id": "agent-1"}
        }
        
        # 创建第一个会话
        resp1 = fusion_client.api_request(
            "POST",
            "pipe/session/",
            json=payload1,
            headers={"X-API-Key": api_key}
        )
        assert resp1.status_code == 200, f"Failed to create first session: {resp1.text}"
        session1_id = resp1.json()["session_id"]
        
        # 2. 尝试创建第二个会话（不同 task_id，但 view 被第一个锁定了）
        payload2 = {
            "task_id": "task-2",
            "client_info": {"agent_id": "agent-2"}
        }
        
        resp2 = fusion_client.api_request(
            "POST",
            "pipe/session/",
            json=payload2,
            headers={"X-API-Key": api_key}
        )
        
        # 预期冲突 (409)
        assert resp2.status_code == 409, f"Should refuse concurrent session. Got: {resp2.status_code} {resp2.text}"
        assert "active sessions" in resp2.json()["detail"].lower()
        
        # 3. 清理第一个会话
        fusion_client.terminate_session(session1_id)

    def test_heartbeat_obsoleted_payload(self, fusion_client, test_api_key, test_view):
        """
        验证当会话过期后，心跳返回 419 的 Payload 结构。
        """
        view_id = test_view["id"]
        api_key = test_api_key["key"]
        
        # 1. 创建会话
        resp = fusion_client.api_request(
            "POST",
            "pipe/session/",
            json={"task_id": "hb-test"},
            headers={"X-API-Key": api_key}
        )
        session_id = resp.json()["session_id"]
        
        # 2. 立即终止会话（在 Fusion 端使其注销）
        fusion_client.terminate_session(session_id)
        
        # 3. 尝试发送心跳
        hb_resp = fusion_client.api_request(
            "POST",
            f"pipe/session/{session_id}/heartbeat",
            json={"can_realtime": True},
            headers={"X-API-Key": api_key}
        )
        
        # 预期 419
        assert hb_resp.status_code == 419, f"Should return 419 Obsoleted. Got: {hb_resp.status_code} {hb_resp.text}"
        data = hb_resp.json()
        assert "not found" in data["detail"].lower() or "obsoleted" in data["detail"].lower()

    def test_view_stats_access(self, fusion_client, test_view):
        """
        验证 Stats 接口返回正确的统计结构。
        """
        resp = fusion_client.api_request("GET", f"views/{test_view['id']}/stats")
        assert resp.status_code == 200
        data = resp.json()
        assert "item_count" in data
        assert "total_size" in data
        assert "audit_cycle_count" in data

    def test_view_search_access(self, fusion_client, test_view):
        """
        验证 Search 接口能够进行各种通配符模式搜索。
        """
        patterns = [
            "*.txt",         # 基础后缀
            "data/*/*.log",  # 深度通配符
            "**/config.yaml",# 递归通配符 (如果驱动支持)
            "file-?.tmp"     # 单字符通配符
        ]
        
        for p in patterns:
            resp = fusion_client.api_request(
                "GET", 
                f"views/{test_view['id']}/search",
                params={"pattern": p}
            )
            assert resp.status_code == 200, f"Search failed for pattern: {p}"
            assert isinstance(resp.json(), list)
