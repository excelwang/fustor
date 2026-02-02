import os
import shutil
import click
import sys
import yaml
import subprocess
import time
import requests


class ServiceManager:
    def __init__(self, run_dir: str, base_port: int = 18100):
        self.run_dir = os.path.abspath(run_dir)
        # 监控目标数据目录
        self.data_dir = os.path.join(self.run_dir, "data")
        # 系统环境主目录 (FUSTOR_HOME)
        self.env_dir = os.path.join(self.run_dir, ".fustor")
        
        self.fusion_port = base_port + 2
        self.agent_port = base_port
        
        self.fusion_process = None
        self.agent_process = None
        self.processes = [] # Backwards compatibility for stop_all fallback

    def setup_env(self):
        # Safety Check: Only allow operations in directories ending with 'fustor-benchmark-run'
        if not self.run_dir.endswith("fustor-benchmark-run"):
            click.echo(click.style(f"FATAL: Environment setup denied. Target run-dir '{self.run_dir}' must end with 'fustor-benchmark-run' for safety.", fg="red", bold=True))
            sys.exit(1)

        if os.path.exists(self.env_dir):
            shutil.rmtree(self.env_dir)
        os.makedirs(self.env_dir, exist_ok=True)
        
        # Generate a random token for internal communication
        import secrets
        self.client_token = secrets.token_urlsafe(32)
        
        # Environment config
        with open(os.path.join(self.env_dir, ".env"), "w") as f:
            f.write(f"FUSTOR_HOME={self.env_dir}\n")
            f.write(f"FUSTOR_LOG_LEVEL=DEBUG\n")
        
        # V2: Inject Receivers Config for Fusion (replacing views-config)
        self.api_key = "bench-api-key-123456"
        receivers_config = {
            "receivers": {
                "bench-http": {
                    "driver": "http",
                    "port": self.fusion_port,
                    "host": "0.0.0.0",
                    "api_keys": {
                        self.api_key: {
                            "role": "admin",
                            "view_mappings": ["bench-view"]
                        }
                    }
                }
            }
        }
        with open(os.path.join(self.env_dir, "receivers-config.yaml"), "w") as f:
            yaml.dump(receivers_config, f)
        
        # Inject View Config for Fusion
        os.makedirs(os.path.join(self.env_dir, "views-config"), exist_ok=True)
        view_config = {
            "bench-view": {
                "view_id": 1,
                "driver": "fs",
                "disabled": False,
                "driver_params": {"uri": "/tmp/bench-view"}
            }
        }
        with open(os.path.join(self.env_dir, "views-config/bench-view.yaml"), "w") as f:
            yaml.dump(view_config, f)

    def _wait_for_service(self, url: str, name: str, timeout: int = 30):
        click.echo(f"Waiting for {name} at {url}...")
        start = time.time()
        while time.time() - start < timeout:
            try:
                requests.get(url, timeout=1)
                click.echo(f"{name} is up.")
                return True
            except Exception:
                time.sleep(0.5)
        click.echo(f"Error: {name} failed to start.")
        return False


    def configure_system(self):
        click.echo("System configured via static YAML.")
        return self.api_key

    def start_fusion(self):
        cmd = [
            "fustor-fusion", "start",
            "-p", str(self.fusion_port)
        ]
        log_file = open(os.path.join(self.env_dir, "fusion.log"), "a")
        env = os.environ.copy()
        env["FUSTOR_HOME"] = self.env_dir
        
        p = subprocess.Popen(cmd, env=env, stdout=log_file, stderr=subprocess.STDOUT)
        log_file.close()
        self.fusion_process = p
        self.processes.append(p)
        
        click.echo(f"Waiting for Fusion at http://localhost:{self.fusion_port}...")
        start = time.time()
        while time.time() - start < 30:
            try:
                requests.get(f"http://localhost:{self.fusion_port}/", timeout=1)
                click.echo("Fusion is up.")
                return
            except requests.ConnectionError:
                time.sleep(0.5)
        raise RuntimeError("Fusion start failed")

    def start_agent(self, api_key: str, **kwargs):
        # Clean up stale PID file to allow restart
        agent_pid = os.path.join(self.env_dir, "agent.pid")
        if os.path.exists(agent_pid):
            os.remove(agent_pid)

        # 1. Sources Config
        sources_config = {
            "bench-fs": {
                "driver": "fs",
                "uri": self.data_dir,
                "credential": {"key": "dummy"}, 
                "disabled": False,
                "is_transient": True,
                "driver_params": {
                    "max_queue_size": 100000,
                    "max_retries": 1,
                    "min_monitoring_window_days": 1
                }
            }
        }
        with open(os.path.join(self.env_dir, "sources-config.yaml"), "w") as f:
            yaml.dump(sources_config, f)

        # 2. Senders Config
        senders_config = {
            "bench-fusion": {
                "driver": "http",
                "endpoint": f"http://127.0.0.1:{self.fusion_port}",
                "credential": {"key": api_key},
                "disabled": False,
                "config": {
                    "batch_size": 1000,
                    "max_retries": 10,
                    "retry_delay_sec": 5
                }
            }
        }
        with open(os.path.join(self.env_dir, "senders-config.yaml"), "w") as f:
            yaml.dump(senders_config, f)

        # 3. Pipelines Config
        os.makedirs(os.path.join(self.env_dir, "pipelines-config"), exist_ok=True)
        pipe_config = {
            "pipeline_id": "bench-pipe",
            "source": "bench-fs",
            "sender": "bench-fusion",
            "disabled": False,
            "audit_interval_sec": kwargs.get("audit_interval", 0),
            "sentinel_interval_sec": kwargs.get("sentinel_interval", 0)
        }
        with open(os.path.join(self.env_dir, "pipelines-config/bench-pipe.yaml"), "w") as f:
            yaml.dump(pipe_config, f)
            
        cmd = [
            "fustor-agent", "start",
            "-p", str(self.agent_port)
        ]
        log_file = open(os.path.join(self.env_dir, "agent.log"), "a")
        env = os.environ.copy()
        env["FUSTOR_HOME"] = self.env_dir
        
        p = subprocess.Popen(cmd, env=env, stdout=log_file, stderr=subprocess.STDOUT)
        log_file.close() # Close in parent
        self.agent_process = p
        self.processes.append(p)
        
        self._wait_for_service(f"http://localhost:{self.agent_port}/", "Agent")

    def check_agent_logs(self, lines=100):
        log_path = os.path.join(self.env_dir, "agent.log")
        if not os.path.exists(log_path):
            return False, "Log file not found yet"
        
        try:
            with open(log_path, "r") as f:
                content = f.readlines()[-lines:]
            
            error_keywords = ["ERROR", "Exception", "Traceback", "404 -", "failed to start", "ConfigurationError", "崩溃"]
            success_keywords = ["initiated successfully", "Uvicorn running", "Application startup complete"]
            
            has_error = False
            error_msg = ""
            has_success = False

            for line in content:
                if any(kw in line for kw in error_keywords):
                    has_error = True
                    error_msg = line.strip()
                if any(kw in line for kw in success_keywords):
                    has_success = True

            if has_error:
                return False, f"Detected Error: {error_msg}"
            
            if not has_success:
                return True, "Starting up... (no success signal yet)"
                
            return True, "OK (Success signals detected)"
        except Exception as e:
            return True, f"Could not read log: {e}"

    def get_agent_log_path(self):
        return os.path.join(self.env_dir, "agent.log")

    def get_fusion_log_path(self):
        return os.path.join(self.env_dir, "fusion.log")

    def get_log_size(self, log_path):
        if not os.path.exists(log_path): return 0
        return os.path.getsize(log_path)

    def grep_log(self, log_path, pattern, start_offset=0):
        """Search for a regex pattern in log file starting from offset."""
        if not os.path.exists(log_path): return None
        import re
        regex = re.compile(pattern)
        with open(log_path, "r") as f:
            f.seek(start_offset)
            for line in f:
                match = regex.search(line)
                if match:
                    return match
        return None
        
    def wait_for_log(self, log_path, pattern, start_offset=0, timeout=30):
        """Wait for a pattern to appear in log."""
        start = time.time()
        while time.time() - start < timeout:
            match = self.grep_log(log_path, pattern, start_offset)
            if match: return match
            time.sleep(0.5)
        return None

    def trigger_agent_audit(self, pipeline_id="bench-pipe"):
        """Triggers audit for a sync instance via Agent API."""
        url = f"http://localhost:{self.agent_port}/api/instances/pipelines/{pipeline_id}/_actions/trigger_audit"
        res = requests.post(url)
        res.raise_for_status()
        return res.json()

    def trigger_agent_sentinel(self, pipeline_id="bench-pipe"):
        """Triggers sentinel for a sync instance via Agent API."""
        url = f"http://localhost:{self.agent_port}/api/instances/pipelines/{pipeline_id}/_actions/trigger_sentinel"
        res = requests.post(url)
        res.raise_for_status()
        return res.json()

    def wait_for_leader(self, pipeline_id="bench-pipe", timeout=30, start_offset=0):
        click.echo(f"Waiting for {pipeline_id} to become LEADER...")
        pattern = rf"Assigned LEADER role for {pipeline_id}"
        return self.wait_for_log(self.get_agent_log_path(), pattern, start_offset=start_offset, timeout=timeout)

    def stop_agent(self):
        """Safely stop only the benchmark agent process."""
        if self.agent_process:
            click.echo("Stopping benchmark agent...")
            try:
                self.agent_process.terminate()
                self.agent_process.wait(timeout=5)
            except subprocess.TimeoutExpired:
                self.agent_process.kill()
            self.agent_process = None
        
        # Remove PID file
        agent_pid = os.path.join(self.env_dir, "agent.pid")
        if os.path.exists(agent_pid):
            os.remove(agent_pid)
        time.sleep(1)

    def stop_all(self):
        click.echo("Stopping all benchmark services...")
        # No more global pkill!
        
        for p in self.processes:
            try:
                p.terminate()
                p.wait(timeout=2)
            except Exception:
                p.kill()
        self.processes = []

