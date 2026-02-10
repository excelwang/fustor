"""
Docker Compose environment manager for integration tests.
"""
import os
import subprocess
import time
import logging
import functools
from pathlib import Path
from typing import Optional, Callable, TypeVar

logger = logging.getLogger("fustor_test")

# Type variable for retry decorator
T = TypeVar('T')

COMPOSE_FILE = Path(__file__).parent.parent / "docker-compose.yml"


def retry(
    max_attempts: int = 3,
    delay: float = 1.0,
    backoff: float = 2.0,
    exceptions: tuple = (Exception,)
) -> Callable:
    """
    Retry decorator with exponential backoff.
    
    Args:
        max_attempts: Maximum number of retry attempts
        delay: Initial delay between retries (seconds)
        backoff: Multiplier for delay after each retry
        exceptions: Tuple of exceptions to catch and retry
    """
    def decorator(func: Callable[..., T]) -> Callable[..., T]:
        @functools.wraps(func)
        def wrapper(*args, **kwargs) -> T:
            last_exception = None
            current_delay = delay
            
            for attempt in range(1, max_attempts + 1):
                try:
                    return func(*args, **kwargs)
                except exceptions as e:
                    last_exception = e
                    if attempt < max_attempts:
                        logger.warning(
                            f"{func.__name__} failed (attempt {attempt}/{max_attempts}): {e}. "
                            f"Retrying in {current_delay:.1f}s..."
                        )
                        time.sleep(current_delay)
                        current_delay *= backoff
                    else:
                        logger.error(
                            f"{func.__name__} failed after {max_attempts} attempts: {e}"
                        )
            
            raise last_exception
        return wrapper
    return decorator


class DockerManager:
    """Manages Docker Compose lifecycle for integration tests."""

    def __init__(self, compose_file: Path = COMPOSE_FILE, project_name: str = "fustor-integration"):
        self.compose_file = compose_file
        self.project_name = project_name
        self._base_cmd = [
            "docker", "compose",
            "-f", str(self.compose_file),
            "-p", self.project_name
        ]

    def up(self, services: Optional[list[str]] = None, build: bool = True, wait: bool = True) -> None:
        """Start services."""
        cmd = self._base_cmd + ["up", "-d"]
        if build:
            cmd.append("--build")
        if wait:
            cmd.append("--wait")
        if services:
            cmd.extend(services)
        subprocess.run(cmd, check=True)

    def down(self, volumes: bool = True) -> None:
        """Stop and remove services."""
        cmd = self._base_cmd + ["down"]
        if volumes:
            cmd.append("-v")
        subprocess.run(cmd, check=True)

    def exec_in_container(
        self,
        container: str,
        command: list[str],
        workdir: Optional[str] = None,
        env: Optional[dict[str, str]] = None,
        capture_output: bool = True,
        timeout: Optional[int] = None,
        max_attempts: int = 3,
        delay: float = 1.0,
        detached: bool = False
    ) -> subprocess.CompletedProcess:
        """
        Execute command in a running container with automatic retry on failure.
        
        Args:
            container: Target container name
            command: Command to execute
            workdir: Optional working directory
            env: Optional environment variables
            capture_output: Whether to capture stdout/stderr
            timeout: Command timeout
            max_attempts: Number of retry attempts (default 3)
            delay: Initial delay between retries
        """
        last_exception = None
        cmd = ["docker", "exec"]
        if detached:
            cmd.append("-d")
        if workdir:
            cmd.extend(["-w", workdir])
        if env:
            for k, v in env.items():
                cmd.extend(["-e", f"{k}={v}"])
        cmd.append(container)
        cmd.extend(command)

        current_delay = delay
        for attempt in range(1, max_attempts + 1):
            try:
                result = subprocess.run(cmd, capture_output=capture_output, text=True, timeout=timeout)
                # Success if returncode is 0 OR if it's the first attempt and failed (intentional failure check)
                # Note: We only retry if there was an EXCEPTION (like docker daemon issue)
                # or if the command failed and it's a known transient issue.
                # Actually, standard behavior is to only retry on exceptions.
                return result
            except (subprocess.SubprocessError, Exception) as e:
                last_exception = e
                if attempt < max_attempts:
                    logger.warning(
                        f"exec_in_container to {container} failed (attempt {attempt}/{max_attempts}): {e}. "
                        f"Retrying in {current_delay}s..."
                    )
                    time.sleep(current_delay)
                    current_delay *= 2
                else:
                    logger.error(f"exec_in_container to {container} failed after {max_attempts} attempts: {e}")
        
        if last_exception:
            raise last_exception
        raise RuntimeError("exec_in_container failed unexpectedly")



    def get_logs(self, container: str, tail: int = 100) -> str:
        """Get container logs."""
        result = subprocess.run(
            ["docker", "logs", "--tail", str(tail), container],
            capture_output=True,
            text=True
        )
        return result.stdout + result.stderr

    def wait_for_health(self, container: str, timeout: int = 120) -> bool:
        """
        Wait for container to become healthy with progress logging.
        
        Args:
            container: Container name to check
            timeout: Maximum seconds to wait
            
        Returns:
            True if container became healthy, False if timeout
        """
        start = time.time()
        last_status = None
        check_interval = 0.5
        log_interval = 10  # Log every 10 seconds
        last_log = 0
        
        logger.info(f"Waiting for {container} to become healthy (timeout: {timeout}s)...")
        
        while time.time() - start < timeout:
            try:
                result = subprocess.run(
                    ["docker", "inspect", "--format", "{{.State.Health.Status}}", container],
                    capture_output=True,
                    text=True,
                    timeout=5
                )
                status = result.stdout.strip()
                
                # Log status changes
                if status != last_status:
                    elapsed = int(time.time() - start)
                    logger.info(f"Container {container} status: {status} (after {elapsed}s)")
                    last_status = status
                
                if status == "healthy":
                    elapsed = time.time() - start
                    logger.info(f"✅ Container {container} healthy after {elapsed:.1f}s")
                    return True
                elif status == "unhealthy":
                    # Try to get logs for debugging
                    logs = self.get_logs(container, tail=20)
                    logger.warning(f"Container {container} is unhealthy. Recent logs:\n{logs}")
                    
                # Periodic progress logging
                elapsed = time.time() - start
                if elapsed - last_log >= log_interval:
                    logger.info(f"Still waiting for {container}... ({int(elapsed)}s/{timeout}s)")
                    last_log = elapsed
                    
            except subprocess.TimeoutExpired:
                logger.warning(f"Docker inspect timed out for {container}")
            except Exception as e:
                logger.warning(f"Error checking {container} health: {e}")
                
            time.sleep(check_interval)
        
        elapsed = time.time() - start
        logger.error(f"❌ Container {container} did not become healthy within {timeout}s (last: {last_status})")
        return False

    def stop_container(self, container: str, timeout: int = 10) -> None:
        """Stop a specific container.
        
        Args:
            container: Container name
            timeout: Seconds to wait before killing (0 for immediate kill, simulates crash)
        """
        subprocess.run(["docker", "stop", "-t", str(timeout), container], check=True)

    def start_container(self, container: str) -> None:
        """Start a stopped container."""
        subprocess.run(["docker", "start", container], check=True)

    def restart_container(self, container: str) -> None:
        """Restart a container."""
        subprocess.run(["docker", "restart", container], check=True)

    def create_file_in_container(
        self,
        container: str,
        path: str,
        content: str = "",
        size_bytes: Optional[int] = None
    ) -> None:
        """Create a file inside container."""
        if size_bytes:
            # Create file with specific size
            self.exec_in_container(container, [
                "dd", "if=/dev/urandom", f"of={path}",
                f"bs={size_bytes}", "count=1"
            ])
        else:
            # Create file with content using base64 to avoid shell escaping issues
            import base64
            b64_content = base64.b64encode(content.encode('utf-8')).decode('utf-8')
            # Use printf to avoid trailing newline from echo if not intended, 
            # but base64 decoding is safer.
            self.exec_in_container(container, [
                "sh", "-c", f"echo '{b64_content}' | base64 -d > {path}"
            ])

    def delete_file_in_container(self, container: str, path: str) -> None:
        """Delete a file inside container."""
        self.exec_in_container(container, ["rm", "-f", path])

    def modify_file_in_container(self, container: str, path: str, append_content: str = "") -> None:
        """Modify a file inside container (append content to trigger mtime update)."""
        self.exec_in_container(container, [
            "sh", "-c", f"echo '{append_content}' >> {path}"
        ])

    def file_exists_in_container(self, container: str, path: str) -> bool:
        """Check if file exists in container."""
        result = self.exec_in_container(container, ["test", "-f", path])
        return result.returncode == 0

    def get_file_mtime(self, container: str, path: str) -> Optional[float]:
        """Get file mtime in container with high precision."""
        result = self.exec_in_container(container, [
            "python3", "-c", f"import os; print(os.stat('{path}').st_mtime)"
        ])
        if result.returncode == 0:
            try:
                return float(result.stdout.strip())
            except ValueError:
                return None
        return None

    def touch_file(self, container: str, path: str) -> None:
        """Update file mtime."""
        self.exec_in_container(container, ["touch", path])

    def sync_nfs_cache(self, container: str) -> None:
        """Force NFS cache sync by remounting or using sync command."""
        # Drop caches to force NFS re-read
        self.exec_in_container(container, [
            "sh", "-c", "sync && echo 3 > /proc/sys/vm/drop_caches 2>/dev/null || true"
        ])


    def cleanup_agent_state(self, container: str) -> None:
        """Kill agent and remove state/pid files."""
        # 1. Kill any existing agent processes
        try:
            self.exec_in_container(container, ["pkill", "-9", "-f", "fustor-agent"], timeout=10)
        except Exception:
            pass
        # 2. Remove state files
        state_files = [
            "/root/.fustor/agent.pid",
            "/root/.fustor/agent-state.json",
            "/root/.fustor/agent.log",
            "/root/.fustor/agent.id"
        ]
        try:
            self.exec_in_container(container, ["rm", "-f"] + state_files, timeout=5)
        except Exception:
            pass


# Singleton instance for tests
docker_manager = DockerManager()
