"""
Test C6: Large File Writing Detection.

验证正在写入的大文件（写入持续时间 > Audit 周期）能被正确检测为 Suspect。

注意: Suspect 清除依赖 Realtime 事件确认，而非 TTL 自动过期。
本测试仅验证写入期间文件保持 Suspect 状态。
"""
import pytest
import time
from ..utils import docker_manager
from ..conftest import CONTAINER_CLIENT_C, MOUNT_POINT, AUDIT_INTERVAL
from ..fixtures.constants import SHORT_TIMEOUT, STRESS_DELAY

class TestLargeFileWriting:
    """Test detection of files currently being written."""

    def test_writing_file_remains_suspect_during_write(
        self,
        docker_env,
        fusion_client,
        setup_agents,
        clean_shared_dir,
        wait_for_audit
    ):
        """
        场景:
          1. 盲区客户端开始写入一个大文件 (持续追加内容)
          2. 写入过程持续时间超过 Audit 周期
          3. Audit 扫描到文件
        预期:
          - 文件被标记为 Suspect (因为来自盲区且 mtime 较新)
          - 写入期间，文件持续保持 Suspect 状态 (mtime 不断更新)
          
        注意: 本测试不验证 Suspect 自动过期，因为清除需要 Realtime 事件。
        """
        import os
        filename = f"large_writing_file_{int(time.time())}.log"
        file_path = f"{MOUNT_POINT}/{filename}"
        file_rel = "/" + os.path.relpath(file_path, MOUNT_POINT)
        
        # 1. Start a background process using subprocess.Popen to keep it running
        import subprocess
        
        # Write for 15 seconds (longer than audit interval)
        write_cmd = f"for i in $(seq 1 15); do echo 'chunk $i' >> {file_path}; sleep 1; done"
        
        # Note: using docker directly instead of docker_manager to allow asynchronous/background process
        docker_cmd = ["docker", "exec", CONTAINER_CLIENT_C, "sh", "-c", write_cmd]
        
        # Start the writer process
        writer_proc = subprocess.Popen(
            docker_cmd,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL
        )
        
        try:
            # 2. Wait for first Audit to pick it up
            wait_for_audit()
        
            # 3. Check status - should be Suspect
            found = fusion_client.wait_for_file_in_tree(file_rel, timeout=SHORT_TIMEOUT)
            assert found is not None, "File should be discovered by Audit"
            
            flags = fusion_client.check_file_flags(file_rel)
            assert flags["integrity_suspect"] is True, \
                "Writing file from blind-spot should be marked as suspect"
            
            # 4. Wait for NEXT audit to pick up changes
            wait_for_audit()
            
            # 5. Check again - should STILL be suspect (mtime keeps updating during write)
            flags = fusion_client.check_file_flags(file_rel)
            assert flags["integrity_suspect"] is True, \
                "File being actively written should remain suspect"
            
            # Test passes: file correctly stays suspect during continuous writes
            
        finally:
            # Cleanup process if still running
            if writer_proc.poll() is None:
                writer_proc.kill()
                writer_proc.wait()
