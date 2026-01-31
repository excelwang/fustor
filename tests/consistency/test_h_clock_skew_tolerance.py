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
    CONTAINER_CLIENT_C,
    CONTAINER_FUSION,
    CONTAINER_NFS_SERVER,
    MOUNT_POINT
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

    def test_file_from_future_nfs_handled_correctly(
        self,
        docker_env,
        fusion_client,
        setup_agents,
        clean_shared_dir,
        wait_for_audit
    ):
        """
        从 Agent B (落后) 的视角看，NFS 上的文件可能来自"未来"。
        
        场景：
          1. 通过 Client C (正常时间) 创建文件。 (mtime = T)
          2. Agent B (时间 = T - 1h) 尝试发现该文件。
        预期：
          - Agent B 虽然处于"过去"，但仍应能正确扫描到 mtime 为 T 的文件。
          - Fusion 接收到 mtime T，逻辑时钟正确演进。
        """
        # 1. Create file from Client C (Host time)
        filename = f"future_for_b_{int(time.time())}.txt"
        file_path = f"{MOUNT_POINT}/{filename}"
        docker_manager.create_file_in_container(CONTAINER_CLIENT_C, file_path, content="test")
        
        # Ensure Fusion's logical clock is initialized by waiting for a report
        # Agent A should be sending index = Host + 2h
        logger.info("Waiting for Fusion Logical Clock to initialize...")
        start_init = time.time()
        logical_now = 0
        while time.time() - start_init < 30:
            stats = fusion_client.get_stats()
            logical_now = stats.get("logical_now", 0)
            if logical_now > 1700000000: # Recent human time
                break
            time.sleep(1)
        
        # Set mtime to significantly LATER than logical_now (e.g. +24 hours)
        hot_mtime = logical_now + 86400  # +24h
        touch_date = time.strftime('%Y%m%d%H%M.%S', time.gmtime(hot_mtime))
        docker_manager.exec_in_container(CONTAINER_NFS_SERVER, ["touch", "-t", touch_date, f"/exports/{filename}"])
        logger.info(f"Targeting HOT mtime: {hot_mtime} (date: {touch_date}), Current Logical: {logical_now}")
        
        # 2. Sync for audit
        audit_marker = f"audit_marker_future_{int(time.time()*1000)}.txt"
        docker_manager.create_file_in_container(CONTAINER_CLIENT_C, f"{MOUNT_POINT}/{audit_marker}", "marker")
        assert fusion_client.wait_for_file_in_tree(f"{MOUNT_POINT}/{audit_marker}", timeout=60) is not None
        
        # 3. Verify file is discovered and IS suspect (age < threshold)
        found = fusion_client.wait_for_file_in_tree(file_path, timeout=10)
        assert found is not None
        
        flags = fusion_client.check_file_flags(file_path)
        logger.info(f"Future file flags: {flags}")
        assert flags["integrity_suspect"] is True, "File with future mtime relative to system should be suspect"

    def test_realtime_sync_from_past_agent(
        self,
        docker_env,
        fusion_client,
        setup_agents,
        clean_shared_dir
    ):
        """
        验证时间落后的 Agent (Follower) 发送的实时事件能被正确处理。
        
        场景：
          1. Leaderc A (正常时间) 创建文件。
          2. Follower B (落后 1h) 修改文件。
        预期：
          - 即使 Agent B 自己的系统时间落后，它观察到的文件 mtime (来自 NFS) 仍是正常的。
          - Fusion 应该能正确处理这个更新。
        """
        filename = f"stale_agent_realtime_{int(time.time())}.txt"
        file_path = f"{MOUNT_POINT}/{filename}"
        
        # 1. Agent A creates file
        docker_manager.create_file_in_container(CONTAINER_CLIENT_A, file_path, "original")
        assert fusion_client.wait_for_file_in_tree(file_path) is not None
        
        # 2. Agent B modifies file
        # Note: B's system time is T-1h, but NFS is T.
        # When B appends, NFS sets mtime to ~T.
        docker_manager.exec_in_container(CONTAINER_CLIENT_B, ["sh", "-c", f"echo 'modified' >> {file_path}"])
        
        # 3. Verify update
        time.sleep(2)
        # Wait for tree to show change (we can't easily check content via tree API yet, but we check suspect cleared)
        flags = fusion_client.check_file_flags(file_path)
        assert flags["integrity_suspect"] is False, "Realtime from follower should clear suspect"

    def test_logical_clock_jumps_forward_and_remains_stable(
        self,
        docker_env,
        fusion_client,
        setup_agents,
        clean_shared_dir,
        wait_for_audit
    ):
        """
        验证逻辑时钟处理跳跃式的未来更新。
        
        场景：
          1. 制造一个"领先" Agent A (+2h) 的发现。
          2. 然后制造一个常规的发现。
        预期：
          - 逻辑时钟水位线推进到物理时间 + 2h。
          - 后续物理时间创建的文件由于 mtime 小于水位线，被视为"旧"的，不会再标记为 suspect。
        """
        # 1. Agent A (+2h) creates a file
        # mtime will be ~Host Time (unless faketime affects NFS writes, which it usually doesn't on shared volumes)
        # Wait, if Agent A uses faketime and does 'touch' without arguments, it might send a fake timestamp to NFS.
        filename_a = f"future_jump_{int(time.time())}.txt"
        file_path_a = f"{MOUNT_POINT}/{filename_a}"
        
        # Use A to create file. 
        docker_manager.exec_in_container(CONTAINER_CLIENT_A, ["sh", "-c", f"echo 'jump' > {file_path_a}"])
        
        # Wait for discovery
        assert fusion_client.wait_for_file_in_tree(file_path_a) is not None
        
        # 2. Check logical clock state indirectly by verifying a jump
        jump_file = f"manual_jump_{int(time.time())}.txt"
        docker_manager.create_file_in_container(CONTAINER_CLIENT_C, f"{MOUNT_POINT}/{jump_file}", "jump")
        # Set mtime to +24 hours manually on NFS server (very far future)
        future_ts = time.time() + 86400  # +24h
        touch_date = time.strftime('%Y%m%d%H%M.%S', time.gmtime(future_ts))
        docker_manager.exec_in_container(CONTAINER_NFS_SERVER, ["touch", "-t", touch_date, f"/exports/{jump_file}"])
        
        # Use marker to wait for audit
        audit_marker = f"audit_marker_h_1_{int(time.time()*1000)}.txt"
        docker_manager.create_file_in_container(CONTAINER_CLIENT_C, f"{MOUNT_POINT}/{audit_marker}", "marker")
        
        # Wait for both to appear
        assert fusion_client.wait_for_file_in_tree(f"{MOUNT_POINT}/{audit_marker}", timeout=60) is not None
        assert fusion_client.wait_for_file_in_tree(f"{MOUNT_POINT}/{jump_file}", timeout=10) is not None
        
        # Verify Logical Clock jumped
        stats = fusion_client.get_stats()
        logical_now = stats.get("logical_now", 0)
        host_now = time.time()
        logger.info(f"Fusion Logical Clock: {logical_now}, Host Physical: {host_now}")
        
        # Logical clock should be roughly Host + 24h
        assert logical_now > host_now + 36000, "Logical Clock should have jumped forward significantly"
        
        # 3. Now create a NORMAL file (Host time)
        # Logical Clock is at least +24h. New file is at 0h.
        # Age = Watermark - mtime = (Host+24h) - (Host) = 24 hours.
        # 24 hours > 5s.
        
        normal_file = f"post_jump_normal_{int(time.time())}.txt"
        docker_manager.create_file_in_container(CONTAINER_CLIENT_C, f"{MOUNT_POINT}/{normal_file}", "normal")
        
        # Marker again
        audit_marker_2 = f"audit_marker_h_2_{int(time.time()*1000)}.txt"
        docker_manager.create_file_in_container(CONTAINER_CLIENT_C, f"{MOUNT_POINT}/{audit_marker_2}", "marker")
        
        assert fusion_client.wait_for_file_in_tree(f"{MOUNT_POINT}/{audit_marker_2}", timeout=60) is not None
        assert fusion_client.wait_for_file_in_tree(f"{MOUNT_POINT}/{normal_file}", timeout=10) is not None
        
        flags = fusion_client.check_file_flags(f"{MOUNT_POINT}/{normal_file}")
        logger.info(f"Post-jump normal file flags: {flags}")
        
        assert flags["integrity_suspect"] is False, \
            f"Files significantly older than Logical Clock ({logical_now}) should not be suspect upon audit discovery"
