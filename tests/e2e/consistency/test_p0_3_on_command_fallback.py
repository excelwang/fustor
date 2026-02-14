"""
Test P0-3: On-Command Find Fallback (E2E Verification).

This test specifically verifies the "Fallback" mechanism rooted in `views.py`.
It triggers the fallback by passing an unknown argument `on_demand_scan` to the driver,
which raises a TypeError, causing `FallbackDriverWrapper` to catch it and invoke `on_command_fallback`.
This confirms that the `on_command_fallback` logic (which we fixed) works end-to-end.
"""
import pytest
import time
import os
from ..conftest import CONTAINER_CLIENT_A, MOUNT_POINT, CONTAINER_NFS_SERVER, CONTAINER_FUSION
from ..fixtures.constants import (
    VIEW_READY_TIMEOUT,
    SHORT_TIMEOUT,
    MEDIUM_TIMEOUT,
    POLL_INTERVAL
)

class TestOnCommandFallback:
    """Test the P0-3 On-Command Fallback mechanism."""

    def test_fallback_mechanism_works(
        self,
        docker_env,
        fusion_client,
        setup_agents,
        clean_shared_dir
    ):
        """
        场景: 
        1. 正常启动环境。
        2. 在 NFS 上创建一个新文件 (Agent 可能尚未同步)。
        3. 发送带 `on_demand_scan=true` 的 API 请求。
           - 这会导致 ViewDriver 抛出 TypeError (未知参数)。
           - FallbackDriverWrapper 捕获异常，打印 Warning。
           - 调用 `on_command_fallback`。
           - `on_command_fallback` 使用修复后的 `scan` 命令调用 Agent。
        4. 验证 Agent 收到命令并执行扫描，文件最终出现在视图中。
        5. 验证 Fusion 日志确认触发了 Fallback。
        """
        from ..utils import docker_manager
        
        # 1. 确保环境就绪
        assert fusion_client.wait_for_view_ready(timeout=VIEW_READY_TIMEOUT), "View did not become ready"
        assert fusion_client.wait_for_agent_ready("client-a", timeout=SHORT_TIMEOUT), "Agent A not ready"

        # 2. 创建测试文件
        test_file_name = f"fallback_test_{int(time.time())}.txt"
        test_file_path = f"/exports/{test_file_name}"
        test_file_rel = f"/{test_file_name}"
        
        print(f"\n[Test] Creating file {test_file_name} on NFS server...")
        docker_manager.create_file_in_container(CONTAINER_NFS_SERVER, test_file_path, "Fallback test content")
        
        # Actimeo wait
        time.sleep(2.0)

        # 3. 触发 Fallback (通过传递未知参数导致 Driver 报错)
        print(f"[Test] Triggering fallback via on_demand_scan param for {test_file_rel}...")
        response = fusion_client.api_request(
            "GET", 
            f"views/{fusion_client.view_id}/tree", 
            params={"path": test_file_rel, "on_demand_scan": "true"}
        )
        assert response.status_code == 200, f"API call failed: {response.text}"
        data = response.json()
        
        # 验证返回数据包含文件 (Metadata source 应该是 remote_fallback)
        # 注意: 实际 API 返回结构可能有所不同，取决于 on_command_fallback 的返回值
        print(f"[Test] Fallback API Response: {data}")

        # 4. 验证 Fusion 日志中存在 Fallback 触发记录
        print("[Test] Verifying Fusion logs for Fallback trigger...")
        fusion_log = docker_manager.exec_in_container(CONTAINER_FUSION, ["cat", "/root/.fustor/logs/fusion.log"]).stdout
        
        # FallbackDriverWrapper logs warning: "primary query failed (Type Error...), triggering On-Command Fallback..."
        assert "triggering On-Command Fallback" in fusion_log, "Fusion log should indicate Fallback trigger"
        
        # 5. 验证文件出现在树中
        print("[Test] Verifying file visibility...")
        found = fusion_client.wait_for_file_in_tree(test_file_rel, timeout=MEDIUM_TIMEOUT)
        assert found, "File should appear after fallback scan"
        
        # 6. 验证 Agent 日志确认收到正确的 'scan' 命令
        print("[Test] Verifying Agent logs for 'scan' command...")
        agent_log = docker_manager.exec_in_container(CONTAINER_CLIENT_A, ["cat", "/root/.fustor/logs/agent.log"]).stdout
        # PipeCommandMixin logs: "Received command 'scan'"
        assert "Received command 'scan'" in agent_log, "Agent should have received 'scan' command (Verify P0-3 fix)"
        
        print("[Test] P0-3 On-Command Fallback verified successfully.")
