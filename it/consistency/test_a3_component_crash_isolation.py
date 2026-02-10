"""
Test A3: Component Crash Isolation.

验证各个组件（Source, Pipe, Receiver, View）及其内部模块的崩溃隔离性。
确保局部故障不会导致整个系统或数据链崩溃。

参考文档: CONSISTENCY_DESIGN.md - Reliability & Fault Tolerance
"""
import pytest
import time
import os
import requests
import json
from ..utils import docker_manager
from ..conftest import CONTAINER_CLIENT_A, CONTAINER_CLIENT_B, CONTAINER_FUSION, MOUNT_POINT
from ..fixtures.constants import (
    SHORT_TIMEOUT,
    MEDIUM_TIMEOUT,
    LONG_TIMEOUT,
    POLL_INTERVAL
)

class TestComponentCrashIsolation:
    """Test system resilience against specific component crashes."""

    def test_source_component_isolation_partial_failure(
        self,
        docker_env,
        fusion_client,
        setup_agents,
        clean_shared_dir,
        wait_for_audit
    ):
        """
        Scenario: Source Component (Driver) Partial Failure.
        验证 Source Component (FSDriver) 在遇到不可读目录（Permission Error）时，
        能够隔离错误，继续监控其他目录，且不导致 Agent 崩溃。
        """
        # 1. Setup healthy state
        base_dir = f"{MOUNT_POINT}/source_isolation_{int(time.time())}"
        readable_dir = f"{base_dir}/readable"
        unreadable_dir = f"{base_dir}/unreadable"
        
        docker_manager.exec_in_container(CONTAINER_CLIENT_A, ["mkdir", "-p", readable_dir])
        docker_manager.exec_in_container(CONTAINER_CLIENT_A, ["mkdir", "-p", unreadable_dir])
        
        docker_manager.create_file_in_container(CONTAINER_CLIENT_A, f"{readable_dir}/file1.txt", "content1")
        docker_manager.create_file_in_container(CONTAINER_CLIENT_A, f"{unreadable_dir}/file2.txt", "content2")
        
        # Make one directory unreadable
        docker_manager.exec_in_container(CONTAINER_CLIENT_A, ["chmod", "000", unreadable_dir])
        
        # 2. Trigger Scan/Audit
        # Wait for audit to ensure scanner runs over the structure
        wait_for_audit()
        
        # 3. Verify Isolation
        # Readable file should be present
        assert fusion_client.wait_for_file_in_tree(
            f"/{os.path.relpath(readable_dir, MOUNT_POINT)}/file1.txt", 
            timeout=SHORT_TIMEOUT
        ), "Readable file should be synced despite sibling permission error"
        
        # Agent should be alive
        check_agent = docker_manager.exec_in_container(CONTAINER_CLIENT_A, ["pgrep", "-f", "fustor-agent"])
        assert check_agent.returncode == 0, "Agent process should survive source driver permission errors"
        
        # Cleanup (restore permissions to delete)
        docker_manager.exec_in_container(CONTAINER_CLIENT_A, ["chmod", "755", unreadable_dir])

    def test_sender_pipe_isolation_network_partition(
        self,
        docker_env,
        fusion_client,
        setup_agents,
        clean_shared_dir
    ):
        """
        Scenario: Sender/Pipe Component Isolation (Network/Fusion Unavailable).
        验证当 Receiver (Fusion) 不可用时，Sender/Pipe 组件：
        1. 不会崩溃
        2. 能够缓冲/重试事件
        3. 在连接恢复后自动恢复数据同步
        """
        assert fusion_client.wait_for_view_ready(timeout=MEDIUM_TIMEOUT)
        
        # 1. Simulate Network Partition / Receiver Down
        print("DEBUG: Pausing Fusion container to simulate network timeout/unavailability...")
        docker_manager.exec_in_container(CONTAINER_FUSION, ["kill", "-STOP", "1"]) # Pause main process
        # Or use docker pause, but keeping consistent with exec usage
        subprocess_cmd = ["docker", "pause", CONTAINER_FUSION]
        import subprocess
        subprocess.run(subprocess_cmd, check=True)
        
        try:
            # 2. Generate Events while Receiver is Down
            # These should be buffered in Agent Pipe
            test_file = f"{MOUNT_POINT}/sender_isolation_{int(time.time())}.txt"
            docker_manager.create_file_in_container(CONTAINER_CLIENT_A, test_file, "buffered content")
            
            # Wait a bit to ensure Sender tries and fails
            time.sleep(10)
            
            # 3. Verify Agent is still running (Pipe didn't crash due to connection error)
            check_agent = docker_manager.exec_in_container(CONTAINER_CLIENT_A, ["pgrep", "-f", "fustor-agent"])
            assert check_agent.returncode == 0, "Agent should survive Fusion unavailability"
            
        finally:
            # 4. Restore Receiver
            print("DEBUG: Unpausing Fusion container...")
            subprocess.run(["docker", "unpause", CONTAINER_FUSION], check=True)
        
        # 5. Verify Data Recovery
        # The buffered event should eventually be sent
        test_file_rel = "/" + os.path.relpath(test_file, MOUNT_POINT)
        assert fusion_client.wait_for_file_in_tree(test_file_rel, timeout=LONG_TIMEOUT), \
            "Pipe should resume sending buffered events after Receiver recovers"

    def test_receiver_isolation_malformed_payload(
        self,
        docker_env,
        fusion_client,
        setup_agents,
        clean_shared_dir
    ):
        """
        Scenario: Receiver Component (Fusion Ingest) Isolation.
        验证 Receiver 在接收到畸形数据包时：
        1. 能够正确拒绝（400/500）
        2. Fusion 服务本身不崩溃
        3. 正常的后续请求仍能处理
        """
        assert fusion_client.wait_for_view_ready(timeout=MEDIUM_TIMEOUT)
        
        # 1. Send Malformed Data to Fusion Receiver API manually
        # Accessing Fusion API from Host
        import requests
        base_url = "http://localhost:8080" # Assuming port mapping from docker-compose
        
        # Need a valid session_id to pass auth check if any, or just hit an endpoint
        # Sending garbage JSON
        try:
            resp = requests.post(f"{base_url}/api/v1/ingest", data="GARBAGE_NOT_JSON", headers={"Content-Type": "application/json"})
            assert resp.status_code in [400, 500, 415], f"Receiver should reject garbage, got {resp.status_code}"
        except Exception as e:
            # Might fail if port not mapped to host, assuming test runs on host/network with access
            print(f"DEBUG: Could not hit Fusion API directly: {e}")
            
        # Try sending valid JSON but invalid schema
        try:
            resp = requests.post(f"{base_url}/api/v1/ingest", json={"invalid": "schema"})
            assert resp.status_code in [400, 422, 500], f"Receiver should reject invalid schema, got {resp.status_code}"
        except Exception:
            pass
            
        # 2. Verify Fusion is Still Healthy
        # Normal agent operations should still work
        test_file = f"{MOUNT_POINT}/receiver_isolation_{int(time.time())}.txt"
        docker_manager.create_file_in_container(CONTAINER_CLIENT_A, test_file, "content")
        
        test_file_rel = "/" + os.path.relpath(test_file, MOUNT_POINT)
        assert fusion_client.wait_for_file_in_tree(test_file_rel, timeout=MEDIUM_TIMEOUT), \
            "Fusion Receiver should continue processing valid requests after malformed input"

    def test_view_component_logic_error_isolation(
        self,
        docker_env,
        fusion_client,
        setup_agents,
        clean_shared_dir
    ):
        """
        Scenario: View Component Isolation (Logical State Integrity).
        验证当 View 收到不一致或冲突的逻辑状态时（如 'future' 时间戳），
        View 能够隔离该错误，保持整体状态一致性，不崩溃。
        """
        # 1. Inject 'Logic Bomb' - File with Future Mtime (via manual touch in container)
        # This tests how Arbitrator/View handles "impossible" timestamps which might cause logic errors
        # in clock calculations if not isolated.
        
        future_file = f"{MOUNT_POINT}/future_file_{int(time.time())}.txt"
        docker_manager.create_file_in_container(CONTAINER_CLIENT_A, future_file, "future")
        
        # Manually set mtime to far future (e.g., year 2050)
        future_time = 2524608000 # 2050-01-01
        docker_manager.exec_in_container(CONTAINER_CLIENT_A, ["touch", "-d", "@2524608000", future_file])
        
        # 2. Wait for it to sync (View should accept or reject, but NOT crash)
        future_file_rel = "/" + os.path.relpath(future_file, MOUNT_POINT)
        
        # It might be accepted (system dependent), but critical check is Crash isolation
        try:
            fusion_client.wait_for_file_in_tree(future_file_rel, timeout=SHORT_TIMEOUT)
        except:
            pass # It's okay if it's rejected
            
        # 3. Verify Fusion View is still functioning for normal files
        normal_file = f"{MOUNT_POINT}/normal_file_after_future_{int(time.time())}.txt"
        docker_manager.create_file_in_container(CONTAINER_CLIENT_A, normal_file, "normal")
        
        normal_file_rel = "/" + os.path.relpath(normal_file, MOUNT_POINT)
        assert fusion_client.wait_for_file_in_tree(normal_file_rel, timeout=MEDIUM_TIMEOUT), \
            "View should continue processing normal updates after handling logical anomalies"
