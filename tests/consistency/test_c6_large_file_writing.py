"""
Test C6: Large File Writing Detection.

验证正在写入的大文件（写入持续时间 > Audit 周期）能被正确检测为 Suspect。
"""
import pytest
import time
from ..utils import docker_manager
from ..conftest import CONTAINER_CLIENT_C, MOUNT_POINT, AUDIT_INTERVAL

class TestLargeFileWriting:
    """Test detection of files currently being written."""

    def test_writing_file_remains_suspect(
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
          - 文件被标记为 Suspect
          - 随着 mtime 更新，文件保持 Suspect 状态 (直到停止写入且 TTL 过期)
        """
        filename = f"large_writing_file_{int(time.time())}.log"
        file_path = f"{MOUNT_POINT}/{filename}"
        
        # 1. Start a background process using subprocess.Popen to keep it running
        # We invoke docker exec from host side, keeping it running as a child process of the test
        import subprocess
        
        write_cmd = f"for i in $(seq 1 20); do echo 'chunk $i' >> {file_path}; sleep 1; done"
        docker_cmd = ["docker", "exec", CONTAINER_CLIENT_C, "sh", "-c", write_cmd]
        
        # Start the writer process
        writer_proc = subprocess.Popen(
            docker_cmd,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL
        )
        
        try:
            # 2. Wait for first Audit to pick it up (after ~5s)
            wait_for_audit()
        
            # 3. Check status - should be Suspect
            # We might need to retry a few times as creation + audit timing is racey
            found = fusion_client.wait_for_file_in_tree(file_path, timeout=10)
            assert found is not None, "File should be discovered"
            
            flags = fusion_client.check_file_flags(file_path)
            assert flags["integrity_suspect"] is True, "Writing file should be suspect"
            
            # 4. Wait more (file still being written)
            time.sleep(5)
            
            # check again - should still be suspect (mtime updated, keeping it hot)
            flags = fusion_client.check_file_flags(file_path)
            assert flags["integrity_suspect"] is True, "Still writing file should remain suspect"
            
            # 5. Wait for writing to finish (20s total) + TTL (30s)
            # Total wait > 20s (write) - 10s (elapsed) + 30s (TTL) = ~40s
            # Let's verify it clears eventually
            
            # Wait for write to finish
            time.sleep(15) 
            
            # Now wait for TTL
            import logging
            logger = logging.getLogger("fustor_test")
            logger.info("Writing finished. Waiting for Suspect TTL expiry...")
            
            # Poll for cleared flag
            start = time.time()
            cleared = False
            while time.time() - start < 40:
                flags = fusion_client.check_file_flags(file_path)
                if flags["integrity_suspect"] is False:
                    cleared = True
                    break
                time.sleep(2)
                
            assert cleared, "File should eventually become trusted (not suspect) after writing stops"
            
        finally:
            # Cleanup process if still running
            if writer_proc.poll() is None:
                writer_proc.kill()
                writer_proc.wait()
