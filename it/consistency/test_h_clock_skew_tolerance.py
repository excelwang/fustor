"""
Test H: Clock Skew Tolerance.

验证 Fustor 系统在分布式时钟偏移环境下的正确性：
- 节点通过 libfaketime 模拟不同的物理时间
- Agent A (Leader): +2 小时 (领先)
- Agent B (Follower): -1 小时 (落后)
- Fusion/NFS/Client C: 默认时间 (基准)
- Logical Clock 机制应确保发现和同步正常，不受 mtime 偏移影响
"""
import pytest
import time
import logging
from ..utils import docker_manager
from ..conftest import (
    CONTAINER_CLIENT_A, 
    CONTAINER_CLIENT_B, 
    MOUNT_POINT
)
from ..fixtures.constants import (
    EXTREME_TIMEOUT, 
    LONG_TIMEOUT, 
    INGESTION_DELAY,
    CONTAINER_FUSION,
    CONTAINER_CLIENT_A,
    CONTAINER_CLIENT_B,
    CONTAINER_CLIENT_C
)

logger = logging.getLogger("fustor_test")

class TestClockSkewTolerance:
    """Test that the system handles clock skew between components correctly."""

    def test_clock_skew_environment_setup(
        self,
        docker_env,
        fusion_client,
        setup_agents
    ):
        """
        验证时钟偏移环境已正确设置。
        
        预期：
        - Agent A 时间领先约 2 小时
        - Agent B 时间落后约 1 小时
        - Fusion 时间为物理主机时间
        """
        # Get timestamps from each container
        def get_container_timestamp(container):
            result = docker_manager.exec_in_container(container, ["date", "-u", "+%s"])
            return int(result.stdout.strip())
        
        fusion_time = get_container_timestamp(CONTAINER_FUSION)
        client_a_time = get_container_timestamp(CONTAINER_CLIENT_A)
        client_b_time = get_container_timestamp(CONTAINER_CLIENT_B)
        
        # Log times for debugging
        logger.info(f"Fusion UTC:   {fusion_time}")
        logger.info(f"Client A UTC: {client_a_time}")
        logger.info(f"Client B UTC: {client_b_time}")
        
        a_skew = client_a_time - fusion_time
        b_skew = client_b_time - fusion_time
        
        logger.info(f"Agent A skew: {a_skew}s ({a_skew/3600:.2f}h)")
        logger.info(f"Agent B skew: {b_skew}s ({b_skew/3600:.2f}h)")
        
        # Assertions
        assert 7100 < a_skew < 7300, f"Agent A should be ~2h ahead, got {a_skew}s"
        assert -3700 < b_skew < -3500, f"Agent B should be ~1h behind, got {b_skew}s"

    def test_audit_discovery_of_new_file_is_flagged_suspect(
        self,
        docker_env,
        fusion_client,
        setup_agents,
        clean_shared_dir,
        wait_for_audit
    ):
        """
        验证跨节点新创建的文件（未被本节点实时捕获，而是通过审计发现）应被标记为 suspect。
        
        预期：
          - 由 Client C (无监控 Agent) 创建一个文件。mtime 为 T。
          - Fusion 的水位线此时可能由 Agent A (+2h) 的心跳或之前活动保持。
          - Agent A 通过审计发现该文件。
          - 由于该文件是新出现的且 mtime 与当前水位线接近（Age < threshold），应标记为 suspect。
        """
        filename = f"audit_discovery_{int(time.time())}.txt"
        file_path = f"{MOUNT_POINT}/{filename}"
        
        # 1. Create file from Client C (No Agent)
        docker_manager.create_file_in_container(CONTAINER_CLIENT_C, file_path, "from_c")
        
        # NOTE: Smart Audit relies on parent directory mtime change.
        # Ensure directory mtime is definitely updated to T (host time)
        trigger_path = f"{MOUNT_POINT}/trigger_h_{int(time.time()*1000)}.txt"
        docker_manager.create_file_in_container(CONTAINER_CLIENT_C, trigger_path, "trigger")
        
        logger.info(f"Client C created file: {filename} and trigger.")
        
        # 2. Wait for Audit cycle to complete
        wait_for_audit()
        
        # 3. Wait for discovery via Audit/Scan from Agent A (Leader)
        assert fusion_client.wait_for_file_in_tree(file_path, timeout=EXTREME_TIMEOUT) is not None
        
        # 3. Verify suspect flag
        flags = fusion_client.check_file_flags(file_path)
        logger.info(f"Audit discovered file flags: {flags}")
        assert flags["integrity_suspect"] is True, "New file discovered via Audit should be suspect"

    def test_realtime_sync_phase_from_past_agent(
        self,
        docker_env,
        fusion_client,
        setup_agents,
        clean_shared_dir
    ):
        """
        验证落后 Agent 的实时修改能正常同步并由于时间较旧而清除 suspect。
        """
        filename = f"past_rt_{int(time.time())}.txt"
        file_path = f"{MOUNT_POINT}/{filename}"
        
        # 1. Create file via Client C (Host time)
        docker_manager.create_file_in_container(CONTAINER_CLIENT_C, file_path, "orig")
        
        # 2. Wait for it to be suspect (if discovered via audit by A) or just wait for it to appear
        assert fusion_client.wait_for_file_in_tree(file_path) is not None
        
        # 3. Agent B (落后 1h) 修改文件。实时事件会清除 suspect。
        docker_manager.exec_in_container(CONTAINER_CLIENT_B, ["sh", "-c", f"echo 'mod' >> {file_path}"])
        
        # 4. Verify suspect is cleared
        time.sleep(INGESTION_DELAY)
        flags = fusion_client.check_file_flags(file_path)
        assert flags["integrity_suspect"] is False, "Realtime update should clear suspect status"

    def test_logical_clock_jumps_forward_and_remains_stable(
        self,
        docker_env,
        fusion_client,
        clean_shared_dir,
        wait_for_audit
    ):
        """
        验证逻辑时钟处理跳跃式的未来更新。
        
        场景：
          1. Agent A (+2h) 创建一个文件，推动逻辑时钟跳变。
          2. 然后在 Client C (正常时间) 创建一个文件。
        预期：
          - 逻辑时钟水位线由于 Agent A 的文件而推进。
          - 后续正常文件被视为"旧"的（Age ~ 2h），不会标记为 suspect。
        """
        # 1. Agent A (+2h) creates a file
        filename_a = f"jump_trigger_{int(time.time())}.txt"
        file_path_a = f"{MOUNT_POINT}/{filename_a}"
        docker_manager.exec_in_container(CONTAINER_CLIENT_A, ["touch", file_path_a])
        
        # Wait for discovery to ensure clock jumped
        assert fusion_client.wait_for_file_in_tree(file_path_a, timeout=LONG_TIMEOUT) is not None
        
        stats = fusion_client.get_stats()
        logical_now = stats.get("logical_now", 0)
        host_now = time.time()
        logger.info(f"Watermark after A: {logical_now}, Host Physical: {host_now}")
        assert logical_now > host_now + 7100, "Logical Clock should have jumped forward via Agent A's mtime"
        
        # 2. Now create a NORMAL file (Host time) from Client C
        normal_file = f"cold_file_{int(time.time())}.txt"
        file_path_normal = f"{MOUNT_POINT}/{normal_file}"
        docker_manager.create_file_in_container(CONTAINER_CLIENT_C, file_path_normal, "normal")
        
        # 3. Wait for Audit to discover it
        # (We use a marker to be sure audit finished after the file was created)
        audit_marker = f"marker_jump_{int(time.time())}.txt"
        docker_manager.create_file_in_container(CONTAINER_CLIENT_C, f"{MOUNT_POINT}/{audit_marker}", "marker")
        assert fusion_client.wait_for_file_in_tree(f"{MOUNT_POINT}/{audit_marker}", timeout=EXTREME_TIMEOUT) is not None
        
        # 4. Verify the normal file is NOT suspect
        assert fusion_client.wait_for_file_in_tree(file_path_normal) is not None
        flags = fusion_client.check_file_flags(file_path_normal)
        logger.info(f"Post-jump normal file flags: {flags}")
        
        assert flags["integrity_suspect"] is False, \
            f"File from past relative to Watermark ({logical_now}) should not be suspect"
