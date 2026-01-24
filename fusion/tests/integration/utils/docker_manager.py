"""
Docker Compose environment manager for integration tests.
"""
import os
import subprocess
import time
from pathlib import Path
from typing import Optional

COMPOSE_FILE = Path(__file__).parent.parent / "docker-compose.yml"


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
        capture_output: bool = True
    ) -> subprocess.CompletedProcess:
        """Execute command in a running container."""
        cmd = ["docker", "exec"]
        if workdir:
            cmd.extend(["-w", workdir])
        if env:
            for k, v in env.items():
                cmd.extend(["-e", f"{k}={v}"])
        cmd.append(container)
        cmd.extend(command)
        return subprocess.run(cmd, capture_output=capture_output, text=True)

    def get_logs(self, container: str, tail: int = 100) -> str:
        """Get container logs."""
        result = subprocess.run(
            ["docker", "logs", "--tail", str(tail), container],
            capture_output=True,
            text=True
        )
        return result.stdout + result.stderr

    def wait_for_health(self, container: str, timeout: int = 120) -> bool:
        """Wait for container to become healthy."""
        start = time.time()
        while time.time() - start < timeout:
            result = subprocess.run(
                ["docker", "inspect", "--format", "{{.State.Health.Status}}", container],
                capture_output=True,
                text=True
            )
            status = result.stdout.strip()
            if status == "healthy":
                return True
            time.sleep(2)
        return False

    def stop_container(self, container: str) -> None:
        """Stop a specific container."""
        subprocess.run(["docker", "stop", container], check=True)

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
            # Create file with content
            self.exec_in_container(container, [
                "sh", "-c", f"echo '{content}' > {path}"
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
        """Get file mtime in container."""
        result = self.exec_in_container(container, [
            "stat", "-c", "%Y", path
        ])
        if result.returncode == 0:
            return float(result.stdout.strip())
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


# Singleton instance for tests
docker_manager = DockerManager()
