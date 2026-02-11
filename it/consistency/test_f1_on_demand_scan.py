"""
Test F1: On-Demand Find (Forced Realtime Query).

验证 "强制实时查询" 功能。当怀疑某路径不一致时，
可以通过 Fusion API 手动触发特定路径的查找来恢复一致性。
"""
import pytest
import time
import os
from ..conftest import CONTAINER_CLIENT_A, MOUNT_POINT, CONTAINER_NFS_SERVER
from ..fixtures.constants import (
    VIEW_READY_TIMEOUT,
    SHORT_TIMEOUT,
    MEDIUM_TIMEOUT,
    POLL_INTERVAL
)

class TestOnDemandScan:
    """Test the on-demand find functionality."""

    def test_forced_realtime_find_recovers_missed_event(
        self,
        docker_env,
        fusion_client,
        setup_agents,
        clean_shared_dir
    ):
        """
        场景: 在 NFS 共享目录中创建一个文件。
        预期: 即使不依赖自动同步（inotify/audit），手动触发 Find 也能确保文件被发现且处于正确状态。
        由于 inotify 非常灵敏，本测试重点在于验证 API 调用能成功触发 Agent 侧的 Find 逻辑，
        并且该逻辑能正确上报文件并清除缺失标志。
        """
        from ..utils import docker_manager
        
        # 1. 确保环境就绪
        assert fusion_client.wait_for_view_ready(timeout=VIEW_READY_TIMEOUT), "View did not become ready"
        assert fusion_client.wait_for_agent_ready("client-a", timeout=SHORT_TIMEOUT), "Agent A not ready"

        # 2. 创建测试文件
        # 我们在 NFS Server 侧直接创建，绕过 Agent 的 local IO (虽然 Leader 还是会看到)
        test_file_name = f"on_demand_{int(time.time())}.txt"
        test_file_path = f"/exports/{test_file_name}"
        test_file_rel = f"/{test_file_name}"
        
        print(f"\n[Test] Creating file {test_file_name} on NFS server...")
        docker_manager.create_file_in_container(CONTAINER_NFS_SERVER, test_file_path, "On-demand find test content")
        
        # 3. 立即触发 On-Demand Find (Force Realtime)
        # 即使 inotify 可能已经触发了，我们也要验证手动触发是有效的
        print(f"[Test] Triggering on-demand find for {test_file_rel}...")
        response = fusion_client.api_request(
            "GET", 
            f"views/{fusion_client.view_id}/tree", 
            params={"path": test_file_rel, "force_real_time": "true"}
        )
        assert response.status_code == 200, f"API call failed: {response.text}"
        data = response.json()
        # 根据我们对 api.py 的修改，如果文件还没 in tree，对应应该返回 find_pending=True
        # 如果已经在了，也应该触发了后台扫描
        print(f"[Test] API Response: {data}")

        # 4. 验证文件最终出现在视图中
        print("[Test] Waiting for file to appear/be confirmed...")
        found = fusion_client.wait_for_file_in_tree(test_file_rel, timeout=MEDIUM_TIMEOUT)
        assert found, "File should appear after forced on-demand find"
        
        # 5. 验证 agent_missing 标志被清除
        print("[Test] Verifying agent_missing flag is cleared...")
        # 强制扫描产生的事件应该被视为由于同步产生的，会清除 agent_missing
        assert fusion_client.wait_for_flag(test_file_rel, "agent_missing", False, timeout=SHORT_TIMEOUT), \
            f"Flag agent_missing should be False"
        
        # 6. 验证 integrity_suspect 标志被清除 (因为是 REALTIME 且 atomic)
        print("[Test] Verifying integrity_suspect flag is cleared...")
        assert fusion_client.wait_for_flag(test_file_rel, "integrity_suspect", False, timeout=SHORT_TIMEOUT), \
            f"Flag integrity_suspect should be False"
        
        # 7. 白盒验证：检查 Agent 日志中是否有 On-Demand Find 的记录
        print("[Test] Verifying agent logs for on-demand find record...")
        agent_log = docker_manager.exec_in_container(CONTAINER_CLIENT_A, ["cat", "/root/.fustor/agent.log"]).stdout
        assert "Executing realtime find" in agent_log, "Log should contain 'Executing realtime find'"
        assert "Realtime find complete" in agent_log, "Log should contain 'Realtime find complete'"
        print("[Test] Success: On-demand find identified and completed in logs.")
