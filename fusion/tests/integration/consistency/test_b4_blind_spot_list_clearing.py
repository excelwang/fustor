"""
Test B4: Blind-spot list cleared at each Audit start.

验证每轮 Audit 开始时，旧的盲区名单被清空。
参考文档: CONSISTENCY_DESIGN.md - Section 4.4 (清空时机: 每轮 Audit 开始时清空)
"""
import pytest
import time

from ..utils import docker_manager
from ..conftest import CONTAINER_CLIENT_C, MOUNT_POINT


class TestBlindSpotListPersistence:
    """Test that blind-spot list persists across audits, but updates on changes."""

    def test_blind_spot_list_persists_and_updates(
        self,
        docker_env,
        fusion_client,
        setup_agents,
        clean_shared_dir,
        wait_for_audit
    ):
        """
        场景:
          1. 创建盲区文件 A，等待 Audit 发现 -> A 在列表中
          2. 从 Blind Spot 删除 A -> Audit 发现缺失 -> A 从 Additions 列表移除 (并进入 Deletions)
          3. 创建 Blind Spot 文件 B -> Audit 发现 -> B 加入 Additions
          4. 验证: A 不在 Additions, B 在 Additions.
             关键点: 如果有其他未变动的文件 C，它应该*保留*在 Additions 中(Persistence).
        """
        test_file_a = f"{MOUNT_POINT}/blind_persist_test_a_{int(time.time()*1000)}.txt"
        test_file_b = f"{MOUNT_POINT}/blind_persist_test_b_{int(time.time()*1000)}.txt"
        test_file_c = f"{MOUNT_POINT}/blind_persist_test_c_{int(time.time()*1000)}.txt"
        
        # Step 1: Create files A and C from blind-spot client
        docker_manager.create_file_in_container(CONTAINER_CLIENT_C, test_file_a, content="file A")
        docker_manager.create_file_in_container(CONTAINER_CLIENT_C, test_file_c, content="file C - persistent")
        
        # Trigger Audit
        marker_1 = f"{MOUNT_POINT}/audit_marker_b4_p1_{int(time.time())}.txt"
        docker_manager.create_file_in_container(CONTAINER_CLIENT_C, marker_1, content="marker")
        time.sleep(3)
        assert fusion_client.wait_for_file_in_tree(marker_1, timeout=30)
        
        # Verify A and C are in blind-spot list
        blind_list = fusion_client.get_blind_spot_list()
        paths = [item.get("path") for item in blind_list if item.get("type") == "file"]
        assert test_file_a in paths, "File A should be in blind-spot list"
        assert test_file_c in paths, "File C should be in blind-spot list"
        
        # Step 3: Delete A, Create B. C remains untouched.
        docker_manager.delete_file_in_container(CONTAINER_CLIENT_C, test_file_a)
        docker_manager.create_file_in_container(CONTAINER_CLIENT_C, test_file_b, content="file B")
        
        # Trigger next Audit
        marker_2 = f"{MOUNT_POINT}/audit_marker_b4_p2_{int(time.time())}.txt"
        docker_manager.create_file_in_container(CONTAINER_CLIENT_C, marker_2, content="marker")
        time.sleep(6) # Increased NFS cache wait
        assert fusion_client.wait_for_file_in_tree(marker_2, timeout=45)
        
        # Step 4: Verify List State
        # - A should be GONE (Deleted) - Wait for async blind-spot deletion
        # - B should be PRESENT (New)
        # - C should be PRESENT (Persisted)
        
        start_wait = time.time()
        blind_list_2 = []
        paths_2 = []
        a_removed = False
        
        while time.time() - start_wait < 30:
            blind_list_2 = fusion_client.get_blind_spot_list()
            paths_2 = [item.get("path") for item in blind_list_2 if item.get("type") == "file"]
            
            if test_file_a not in paths_2:
                a_removed = True
                break
            time.sleep(1)
            
        assert a_removed, f"Deleted file A should be removed from additions list. Final paths: {paths_2}"
        assert test_file_b in paths_2, "New file B should be in blind-spot list"
        assert test_file_c in paths_2, "Untouched file C should PERSIST in blind-spot list (Persistence Check)"

    def test_agent_missing_flag_cleared_after_realtime_update(
        self,
        docker_env,
        fusion_client,
        setup_agents,
        clean_shared_dir,
        wait_for_audit
    ):
        """
        场景: 盲区文件被 Agent 客户端重新 touch，agent_missing 标记应被清除
        参考: CONSISTENCY_DESIGN.md - Section 5.1 (Realtime 从 Blind-spot List 移除)
        """
        from ..conftest import CONTAINER_CLIENT_A
        
        test_file = f"{MOUNT_POINT}/blind_flag_clear_test_{int(time.time()*1000)}.txt"
        
        # Create file from blind-spot
        docker_manager.create_file_in_container(
            CONTAINER_CLIENT_C,
            test_file,
            content="blind spot file"
        )
        # Use marker file to detect Audit completion
        marker_file = f"{MOUNT_POINT}/audit_marker_b4_flag_{int(time.time())}.txt"
        docker_manager.create_file_in_container(CONTAINER_CLIENT_C, marker_file, content="marker")
        time.sleep(3) # NFS cache delay
        assert fusion_client.wait_for_file_in_tree(marker_file, timeout=30) is not None
        
        # Verify agent_missing is set
        assert fusion_client.wait_for_flag(test_file, "agent_missing", True, timeout=10), \
            "File should eventually be marked agent_missing"
        
        # Touch file from Agent client (triggers realtime update)
        docker_manager.touch_file(CONTAINER_CLIENT_A, test_file)
        
        # Wait for realtime update (poll for flag cleared)
        assert fusion_client.wait_for_flag(test_file, "agent_missing", False, timeout=10), \
            "agent_missing flag should be cleared after realtime update"
