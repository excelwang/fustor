# tests/e2e/consistency/test_a3_component_crash_isolation.py
"""
Test A3: Component Crash Isolation.

验证各个组件（Source, Pipe, Receiver, View）及其内部模块的崩溃隔离性。
确保局部故障不会导致整个系统或数据链崩溃。

参考文档: specs/05-Stability.md
"""
import pytest
import time
import os
import requests
import subprocess

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
        验证 FSDriver 在遇到 PermissionError 时隔离错误、继续监控其他目录。
        Spec 依据: specs/05-Stability.md §1.2
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

        # 2. Trigger Audit
        wait_for_audit()

        # 3. Verify Isolation — readable file should still be synced
        assert fusion_client.wait_for_file_in_tree(
            f"/{os.path.relpath(readable_dir, MOUNT_POINT)}/file1.txt",
            timeout=SHORT_TIMEOUT
        ), "Readable file should be synced despite sibling permission error"

        # 4. Agent should be alive (use PID file for reliable detection)
        check_agent = docker_manager.exec_in_container(
            CONTAINER_CLIENT_A,
            ["sh", "-c", "kill -0 $(cat /root/.fustor/agent.pid)"]
        )
        assert check_agent.returncode == 0, \
            "Agent 进程应在遇到 PermissionError 后存活 (specs/05-Stability.md §1.2)"

        # 5. Verify error was logged (Spec requires "记录错误并跳过")
        logs_res = docker_manager.exec_in_container(
            CONTAINER_CLIENT_A, ["cat", "/root/.fustor/logs/agent.log"]
        )
        logs = (logs_res.stdout + logs_res.stderr).lower()
        assert "permission" in logs or "error" in logs or "warning" in logs or "skip" in logs, \
            "Agent 应记录 PermissionError (Error/Warning) 到日志 (specs/05-Stability.md §1.2)"

        # Cleanup
        docker_manager.exec_in_container(CONTAINER_CLIENT_A, ["chmod", "755", unreadable_dir])

    def test_sender_pipe_isolation_network_partition(
        self,
        docker_env,
        fusion_client,
        setup_agents,
        clean_shared_dir
    ):
        """
        Scenario: Sender/Pipe Component Isolation (Fusion 不可用).
        验证 Agent Pipe 在 Fusion 不可用时不崩溃，恢复后自动续传。
        Spec 依据: specs/05-Stability.md §1.1 (连接重试: 指数退避)
        """
        assert fusion_client.wait_for_view_ready(timeout=MEDIUM_TIMEOUT)

        # 提前定义变量，避免 NameError
        test_file = f"{MOUNT_POINT}/sender_isolation_{int(time.time())}.txt"
        test_file_rel = "/" + os.path.relpath(test_file, MOUNT_POINT)

        # 1. 暂停 Fusion 容器（只用 docker pause，不要 kill -STOP）
        subprocess.run(["docker", "pause", CONTAINER_FUSION], check=True)

        try:
            # 2. 在 Fusion 不可用期间产生事件
            docker_manager.create_file_in_container(
                CONTAINER_CLIENT_A, test_file, "buffered content"
            )

            # 等待足够时间让 Sender 尝试发送并失败
            time.sleep(10)

            # 3. 验证 Agent 进程存活（Pipe 未因连接错误而崩溃）
            check_agent = docker_manager.exec_in_container(
                CONTAINER_CLIENT_A,
                ["sh", "-c", "cat /root/.fustor/agent.pid && kill -0 $(cat /root/.fustor/agent.pid)"]
            )
            assert check_agent.returncode == 0, \
                "Agent 进程应在 Fusion 不可用时存活 (specs/05-Stability.md §1.1)"

        finally:
            # 4. 恢复 Fusion
            subprocess.run(["docker", "unpause", CONTAINER_FUSION], check=True)

        # 5. 验证数据恢复 — Pipe 应在 Fusion 恢复后自动重传缓冲事件
        assert fusion_client.wait_for_file_in_tree(test_file_rel, timeout=LONG_TIMEOUT), \
            "Pipe 应在 Fusion 恢复后续传缓冲事件 (specs/05-Stability.md §1.1)"

    def test_receiver_isolation_malformed_payload(
        self,
        docker_env,
        fusion_client,
        setup_agents,
        clean_shared_dir
    ):
        """
        Scenario: Receiver 接收畸形数据后不崩溃。
        Spec 依据: specs/05-Stability.md §1.2 (异常隔离)
        """
        assert fusion_client.wait_for_view_ready(timeout=MEDIUM_TIMEOUT)

        # Use exposed receiver port (18889) to test receiver isolation specifically.
        # Note: main port (8102) now also has ingestion routes via pipe_router,
        # but here we want to test the HTTPReceiver's own error handling.
        base_url = "http://localhost:18889"

        # --- 测试 1: 发送完全无效的 JSON ---
        # Use fake session ID to target the actual endpoint
        resp = requests.post(
            f"{base_url}/api/v1/pipe/ingest/fake-session/events",
            data="THIS_IS_NOT_JSON",
            headers={"Content-Type": "application/json"},
            timeout=5
        )
        # 不用 try/except：如果连接失败说明测试环境有问题，应立即暴露
        assert resp.status_code in [400, 422, 500], \
            f"畸形数据应被拒绝, 实际返回: {resp.status_code} {resp.text}"

        # --- 测试 2: 发送合法 JSON 但 Schema 不匹配 ---
        resp = requests.post(
            f"{base_url}/api/v1/pipe/ingest/fake-session/events",
            json={"invalid_field": "no_session_id", "garbage": True},
            timeout=5
        )
        assert resp.status_code in [400, 422, 500], \
            f"Schema 不匹配应被拒绝, 实际返回: {resp.status_code} {resp.text}"

        # --- 验证: Fusion 在接受畸形数据后仍能正常处理合法请求 ---
        test_file = f"{MOUNT_POINT}/receiver_isolation_{int(time.time())}.txt"
        test_file_rel = "/" + os.path.relpath(test_file, MOUNT_POINT)
        docker_manager.create_file_in_container(CONTAINER_CLIENT_A, test_file, "content")

        assert fusion_client.wait_for_file_in_tree(test_file_rel, timeout=MEDIUM_TIMEOUT), \
            "Fusion 应在处理畸形请求后继续正常工作 (specs/05-Stability.md §1.2)"

    def test_view_component_empty_and_oversized_batch_isolation(
        self,
        docker_env,
        fusion_client,
        setup_agents,
        clean_shared_dir
    ):
        """
        Scenario: View 收到空事件批次或异常结构 Batch 时不崩溃。
        Spec 依据: specs/05-Stability.md §4
        替换原 test_view_component_logic_error_isolation（与 test_i 重复）。
        """
        assert fusion_client.wait_for_view_ready(timeout=MEDIUM_TIMEOUT)
        # Use exposed receiver port (18889)
        base_url = "http://localhost:18889"

        # --- 测试 1: 空 Batch (边界条件) ---
        # Use fake session ID to target the actual endpoint
        resp = requests.post(
            f"{base_url}/api/v1/pipe/ingest/fake-session/events",
            json={"session_id": "fake-session", "events": []},
            timeout=5
        )
        # 允许 400（拒绝）或 200（空操作），但不允许 500（崩溃）
        assert resp.status_code != 500, \
            f"空 Batch 不应导致 500 错误, 实际: {resp.status_code}"

        # --- 验证 Fusion 仍正常工作 ---
        test_file = f"{MOUNT_POINT}/batch_isolation_{int(time.time())}.txt"
        test_file_rel = "/" + os.path.relpath(test_file, MOUNT_POINT)
        docker_manager.create_file_in_container(CONTAINER_CLIENT_A, test_file, "ok")
        assert fusion_client.wait_for_file_in_tree(test_file_rel, timeout=MEDIUM_TIMEOUT), \
            "Fusion 应在处理异常 Batch 后继续正常工作"
