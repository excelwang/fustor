"""
Test C2: Audit triggers Suspect marking for blind-spot recent files.

验证 Audit 发现的盲区新增文件如果 mtime 距今 < 10 分钟，会被同时标记为 agent_missing 和 integrity_suspect。
参考文档: CONSISTENCY_DESIGN.md - Section 5.3 场景 1 (加入 Suspect List)
"""
import pytest
import time

from ..utils import docker_manager
from ..conftest import CONTAINER_CLIENT_C, MOUNT_POINT


class TestAuditTriggersSuspect:
    """Test that audit marks recent blind-spot files as suspect."""

    def test_blind_spot_recent_file_double_flagged(
        self,
        docker_env,
        fusion_client,
        setup_agents,
        clean_shared_dir,
        wait_for_audit
    ):
        """
        场景:
          1. 无 Agent 客户端创建文件（mtime 是当前时间）
          2. Audit 发现该文件，因为它是盲区新增
          3. 同时，因为 mtime < 10 分钟，加入 Suspect List
        预期:
          - 文件同时具有 agent_missing: true 和 integrity_suspect: true
        """
        test_file = f"{MOUNT_POINT}/audit_suspect_test_{int(time.time()*1000)}.txt"
        
        # Create file from blind-spot client
        docker_manager.create_file_in_container(
            CONTAINER_CLIENT_C,
            test_file,
            content="blind spot recent file"
        )
        
        # Wait for Audit to discover
        wait_for_audit()
        
        # File should appear
        found = fusion_client.wait_for_file_in_tree(test_file, timeout=10)
        assert found is not None, "Blind-spot file should be discovered by Audit"
        
        # Check both flags
        flags = fusion_client.check_file_flags(test_file)
        
        assert flags["agent_missing"] is True, \
            "Blind-spot file should be marked agent_missing"
        assert flags["integrity_suspect"] is True, \
            "Recent blind-spot file should also be marked integrity_suspect"

    def test_audit_suspects_in_suspect_list(
        self,
        docker_env,
        fusion_client,
        setup_agents,
        clean_shared_dir,
        wait_for_audit
    ):
        """
        场景: Audit 发现的可疑文件应同时出现在 Suspect List 中
        """
        test_file = f"{MOUNT_POINT}/audit_suspect_list_{int(time.time()*1000)}.txt"
        
        docker_manager.create_file_in_container(
            CONTAINER_CLIENT_C,
            test_file,
            content="for audit suspect list"
        )
        
        wait_for_audit()
        fusion_client.wait_for_file_in_tree(test_file, timeout=10)
        
        suspect_list = fusion_client.get_suspect_list()
        paths = [item.get("path") for item in suspect_list]
        
        assert test_file in paths, \
            "Audit-discovered recent file should be in suspect list"
