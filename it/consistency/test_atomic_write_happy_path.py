"""
Test Atomic Write "Happy Path".

验证原子写入操作（如 mv 重命名）在系统中的表现。
预期:
1. 写入操作是原子的，不会产生中间状态。
2. 文件出现时，integrity_suspect 标志应为 False（因为是 Atomic Write）。
"""
import pytest
import time
from ..utils import docker_manager
from ..conftest import CONTAINER_CLIENT_A, MOUNT_POINT, AUDIT_INTERVAL
from ..fixtures.constants import SHORT_TIMEOUT

class TestAtomicWriteHappyPath:
    """Test standard atomic operations (mv)."""

    def test_atomic_rename_is_trusted(
        self,
        docker_env,
        fusion_client,
        setup_agents,
        clean_shared_dir,
        wait_for_audit
    ):
        """
        场景:
          1. 客户端在 .tmp 目录写入文件 (模拟原子写过程)
          2. 客户端执行 mv .tmp/file target (原子重命名)
        预期:
          - Fusion 立即收到 Create/Update 事件
          - target 文件在树中出现
          - integrity_suspect 标志为 False (关键验证点)
        """
        timestamp = int(time.time())
        tmp_file = f"{MOUNT_POINT}/.tmp_atomic_{timestamp}"
        target_file = f"{MOUNT_POINT}/atomic_result_{timestamp}.txt"
        
        # 1. Write to temp file first
        docker_manager.exec_in_container(
            CONTAINER_CLIENT_A,
            ["sh", "-c", f"echo 'atomic content' > {tmp_file}"]
        )
        
        # 2. Atomic Rename (mv) to target
        docker_manager.exec_in_container(
            CONTAINER_CLIENT_A,
            ["mv", tmp_file, target_file]
        )
        
        # 3. Verify visibility
        found = fusion_client.wait_for_file_in_tree(target_file, timeout=SHORT_TIMEOUT)
        assert found is not None, "Atomic move result should be visible in Fusion tree"
        
        # 4. Verify Integrity (The Core Check)
        flags = fusion_client.check_file_flags(target_file)
        
        # Debug info
        if flags["integrity_suspect"]:
            print(f"\n[DEBUG] File {target_file} is SUSPECT. Node Data: {found}")
            
        assert flags["integrity_suspect"] is False, \
            "Atomic write (mv) should NOT satisfy suspect criteria (should be trusted)"
            
        assert flags["agent_missing"] is False, \
            "File should be known by agent"
