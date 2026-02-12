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
    MOUNT_POINT
)
from ..utils.path_utils import to_view_path
from ..fixtures.constants import (
    EXTREME_TIMEOUT, 
    LONG_TIMEOUT, 
    INGESTION_DELAY,
    CONTAINER_FUSION,
    CONTAINER_CLIENT_A,
    CONTAINER_CLIENT_B,
    CONTAINER_CLIENT_B,
    CONTAINER_CLIENT_C,
    MEDIUM_TIMEOUT
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
        import os.path
        filename = f"audit_discovery_{int(time.time())}.txt"
        file_path = f"{MOUNT_POINT}/{filename}"
        file_rel = to_view_path(file_path)
        
        # 1. Create file from Client C (No Agent)
        docker_manager.create_file_in_container(CONTAINER_CLIENT_C, file_path, "from_c")
        
        # Ensure watermark is not too far ahead by getting samples from Agent B (-1h)
        # This ensures host-time files (from C) are within the hot threshold.
        logger.info("Step 1.1: Establishing Mode with Agent B (-1h) to stabilize watermark...")
        for i in range(3):
            docker_manager.exec_in_container(CONTAINER_CLIENT_B, ["touch", f"{MOUNT_POINT}/warmup_h_{i}.txt"])
        
        # NOTE: Smart Audit relies on parent directory mtime change.
        trigger_path = f"{MOUNT_POINT}/trigger_h_{int(time.time()*1000)}.txt"
        docker_manager.create_file_in_container(CONTAINER_CLIENT_C, trigger_path, "trigger")
        
        logger.info(f"Client C created file: {filename} and trigger.")
        
        # 2. Wait for Audit cycle to complete
        wait_for_audit()
        
        # 3. Wait for discovery via Audit/Scan from Agent A (Leader)
        assert fusion_client.wait_for_file_in_tree(file_rel, timeout=EXTREME_TIMEOUT) is not None
        
        # 3. Verify suspect flag
        flags = fusion_client.check_file_flags(file_rel)
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
        import os.path
        filename = f"past_rt_{int(time.time())}.txt"
        file_path = f"{MOUNT_POINT}/{filename}"
        file_rel = to_view_path(file_path)
        
        # 1. Create file via Client C (Host time)
        docker_manager.create_file_in_container(CONTAINER_CLIENT_C, file_path, "orig")
        
        # 2. Wait for it to be suspect (if discovered via audit by A) or just wait for it to appear
        assert fusion_client.wait_for_file_in_tree(file_rel) is not None
        
        # 3. Agent B (落后 1h) 修改文件。实时事件会清除 suspect。
        docker_manager.exec_in_container(CONTAINER_CLIENT_B, ["sh", "-c", f"echo 'mod' >> {file_path}"])
        
        # 4. Verify suspect is cleared
        time.sleep(INGESTION_DELAY)
        flags = fusion_client.check_file_flags(file_rel)
        assert flags["integrity_suspect"] is False, "Realtime update should clear suspect status"

        # 5. Explicit Regression Test for Split-Brain Timer (Proposal A.1)
        # Verify that a brand new file from the skewed agent correctly reaches Fusion.
        probe_file = f"lag_probe_{int(time.time())}.txt"
        probe_path = f"{MOUNT_POINT}/{probe_file}"
        probe_rel = "/" + probe_file
        
        logger.info(f"Creating Realtime Probe from lagging Agent B: {probe_file}")
        docker_manager.create_file_in_container(CONTAINER_CLIENT_B, probe_path, "probe")
        
        # This will fail if Agent B drops the event due to Index Regression (Part A.1)
        assert fusion_client.wait_for_file_in_tree(probe_rel, timeout=MEDIUM_TIMEOUT) is not None, \
            "Realtime event from lagging Agent B was dropped (possible Split-Brain Timer regression)"
        logger.info("Verified: Realtime events from lagging Agent survive skew.")

    def test_logical_clock_remains_stable_despite_skew(
        self,
        docker_env,
        fusion_client,
        setup_agents,
        clean_shared_dir,
        wait_for_audit
    ):
        """
        验证逻辑时钟在时钟偏移环境下的 Mode 机制正确性。
        
        Spec §4.1.A: 仅 Realtime 事件参与 skew 采样。
        Spec §2: 免疫单点故障: 通过全局 Mode 选举将异常样本剔除。
        
        场景：
          1. Agent B (-1h) 创建多个文件 (Realtime)，产生 diff ≈ +3600 的采样。
          2. Agent A (+2h) 创建 1 个文件 (Realtime)，产生 diff ≈ -7200 的采样。
          3. 验证 Mode 选择了 Agent B 的偏差 (3600)，而非 Agent A (−7200)。
             因此 watermark ≈ T - 3600（即 1h 前的时间）。
          4. Client C (正常时间) 创建文件，验证正确 suspect 判定。
        
        注: Audit 事件不参与 skew 采样 (can_sample_skew=is_realtime)，
            因此 Client C 的文件只能通过 Audit 发现，不影响 Mode。
        """
        
        # 0. Generate realtime events from Agent B (-1h) to establish Mode
        # Agent B's files have mtime = T-3600, so diff = T_fusion - (T-3600) = 3600
        logger.info("Step 0: Establishing Mode with Agent B (-1h) realtime events...")
        for i in range(5):
            warmup_file_b = f"warmup_b_{int(time.time())}_{i}.txt"
            docker_manager.exec_in_container(CONTAINER_CLIENT_B, ["touch", f"{MOUNT_POINT}/{warmup_file_b}"])
            time.sleep(0.3)  # Brief pause to ensure unique filenames
        
        # Wait for Agent B's realtime events to be ingested
        warmup_rel = to_view_path(f"{MOUNT_POINT}/{warmup_file_b}")
        assert fusion_client.wait_for_file_in_tree(warmup_rel, timeout=LONG_TIMEOUT) is not None, \
            "Agent B warmup files should be ingested via realtime"
        
        # 1. Agent A (+2h) creates a file (1 event with diff ≈ -7200)
        logger.info("Step 1: Agent A (+2h) creating file...")
        filename_a = f"stable_trigger_{int(time.time())}.txt"
        file_path_a = f"{MOUNT_POINT}/{filename_a}"
        file_path_a_rel = to_view_path(file_path_a)
        docker_manager.exec_in_container(CONTAINER_CLIENT_A, ["touch", file_path_a])
        
        # Wait for discovery
        assert fusion_client.wait_for_file_in_tree(file_path_a_rel, timeout=LONG_TIMEOUT) is not None
        
        stats = fusion_client.get_stats()
        logical_now = stats.get("logical_now", 0)
        host_now = time.time()
        logger.info(f"Watermark: {logical_now}, Host Physical: {host_now}, Diff: {logical_now - host_now:.1f}s")
        
        # Mode should be ~3600 (from Agent B's 5 events) not -7200 (from Agent A's 1-2 events).
        # Watermark = T_fusion - mode_skew ≈ T - 3600 (i.e., 1 hour behind host time).
        # So logical_now - host_now ≈ -3600. Allow generous tolerance.
        diff = logical_now - host_now
        logger.info(f"Logical Clock drift from host: {diff:.1f}s (expected ≈ -3600)")
        
        # The clock should NOT have jumped +7200 (Agent A's skew)
        assert diff < 600, f"Logical Clock jumped too far forwards ({diff:.0f}s). Mode voted for Agent A's +2h skew?"
        
        # The clock should be within Agent B's skew range (≈ -3600 ± tolerance)
        # Allow wide tolerance since NFS latency and audit events could affect timing
        assert diff > -5000, f"Logical Clock lagged too far behind ({diff:.0f}s)."
        
        # 2. Now create a NORMAL file (Host time) from Client C
        logger.info("Step 2: Client C creating normal file...")
        normal_file = f"fresh_file_{int(time.time())}.txt"
        file_path_normal = f"{MOUNT_POINT}/{normal_file}"
        file_path_normal_rel = to_view_path(file_path_normal)
        docker_manager.create_file_in_container(CONTAINER_CLIENT_C, file_path_normal, "normal")
        
        # 3. Wait for Audit to discover it
        wait_for_audit()
        
        assert fusion_client.wait_for_file_in_tree(file_path_normal_rel, timeout=EXTREME_TIMEOUT) is not None
        
        # 4. Verify the normal file's suspect status
        # Watermark ≈ T-3600. File mtime ≈ T.
        # Age = watermark - mtime ≈ (T-3600) - T = -3600 (negative age!)
        # When age < 0 or age < hot_file_threshold, the file should be suspect.
        flags = fusion_client.check_file_flags(file_path_normal_rel)
        logger.info(f"Normal file flags: {flags}")
        
        assert flags["integrity_suspect"] is True, \
            "Fresh file should be suspect (file mtime is ahead of watermark due to Agent B's -1h skew)"
