import os
import shutil
import click
import sys
import yaml
import subprocess
import time
import requests


class ServiceManager:
    def __init__(self, run_dir: str):
        self.run_dir = os.path.abspath(run_dir)
        # 监控目标数据目录
        self.data_dir = os.path.join(self.run_dir, "data")
        # 系统环境主目录 (FUSTOR_HOME)
        self.env_dir = os.path.join(self.run_dir, ".fustor")
        
        self.registry_port = 18101
        self.fusion_port = 18102
        self.agent_port = 18100
        self.processes = []

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
        
        # Registry DB config
        with open(os.path.join(self.env_dir, ".env"), "w") as f:
            f.write(f"FUSTOR_REGISTRY_DB_URL=sqlite+aiosqlite:///{self.env_dir}/registry.db\n")
            f.write(f"FUSTOR_FUSION_REGISTRY_URL=http://localhost:{self.registry_port}\n")
            f.write(f"FUSTOR_REGISTRY_CLIENT_TOKEN={self.client_token}\n")

    def _wait_for_service(self, url: str, name: str, timeout: int = 30):
        click.echo(f"Waiting for {name} at {url}...")
        start = time.time()
        while time.time() - start < timeout:
            try:
                requests.get(url, timeout=1)
                click.echo(f"{name} is up.")
                return True
            except:
                time.sleep(0.5)
        click.echo(f"Error: {name} failed to start.")
        return False

    def start_registry(self):
        cmd = [
            "fustor-registry", "start",
            "-p", str(self.registry_port)
        ]
        log_file = open(os.path.join(self.env_dir, "registry.log"), "a")
        env = os.environ.copy()
        env["FUSTOR_HOME"] = self.env_dir
        env["FUSTOR_REGISTRY_CLIENT_TOKEN"] = self.client_token
        
        p = subprocess.Popen(cmd, env=env, stdout=log_file, stderr=subprocess.STDOUT)
        log_file.close()
        self.processes.append(p)
        
        if not self._wait_for_service(f"http://localhost:{self.registry_port}/health", "Registry"):
            raise RuntimeError("Registry start failed")

    def configure_system(self):
        reg_url = f"http://localhost:{self.registry_port}/v1"
        click.echo("Logging in to Registry...")
        try:
            res = requests.post(f"{reg_url}/auth/login", data={
                "username": "admin@admin.com",
                "password": "admin"
            })
            if res.status_code != 200:
                raise RuntimeError(f"Login failed: {res.text}")
            
            token = res.json()["access_token"]
            headers = {"Authorization": f"Bearer {token}"}
            
            click.echo("Creating Datastore...")
            res = requests.post(f"{reg_url}/datastores/", json={
                "name": "BenchmarkDS", "description": "Auto-generated"
            }, headers=headers)
            if res.status_code not in (200, 201):
                 raise RuntimeError(f"DS creation failed: {res.text}")
            ds_id = res.json()["id"]
            
            # Check if key exists
            res_keys = requests.get(f"{reg_url}/keys/", params={"datastore_id": ds_id}, headers=headers)
            if res_keys.status_code == 200:
                for k in res_keys.json():
                    if k["name"] == "bench-key":
                        self.api_key = k["key"]
                        click.echo(f"Reusing existing API Key: {self.api_key[:8]}...")
                        return self.api_key

            click.echo("Creating API Key...")
            res = requests.post(f"{reg_url}/keys/", json={
                "datastore_id": ds_id, "name": "bench-key"
            }, headers=headers)
            if res.status_code not in (200, 201):
                 raise RuntimeError(f"API Key creation failed: {res.text}")
            
            self.api_key = res.json()["key"]
            click.echo(f"API Key generated: {self.api_key[:8]}...")
            
            return self.api_key
        except Exception as e:
            raise RuntimeError(f"Failed to configure system: {e}")

    def start_fusion(self):
        cmd = [
            "fustor-fusion", "start",
            "-p", str(self.fusion_port)
        ]
        log_file = open(os.path.join(self.env_dir, "fusion.log"), "a")
        env = os.environ.copy()
        env["FUSTOR_HOME"] = self.env_dir
        env["FUSTOR_FUSION_REGISTRY_URL"] = f"http://localhost:{self.registry_port}"
        env["FUSTOR_REGISTRY_CLIENT_TOKEN"] = self.client_token
        
        p = subprocess.Popen(cmd, env=env, stdout=log_file, stderr=subprocess.STDOUT)
        log_file.close()
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

        config = {
            "sources": {
                "bench-fs": {
                    "driver": "fs",
                    "uri": self.data_dir,
                    "credential": {"key": "dummy"}, # Use valid-looking key for source
                    "disabled": False,
                    "is_transient": True,
                    "max_queue_size": 100000,
                    "max_retries": 1,
                    "driver_params": {"min_monitoring_window_days": 1}
                }
            },
            "pushers": {
                "bench-fusion": {
                    "driver": "fusion",
                    "endpoint": f"http://127.0.0.1:{self.fusion_port}",
                    "credential": {"key": api_key},
                    "disabled": False,
                    "batch_size": 1000,
                    "max_retries": 10,
                    "retry_delay_sec": 5
                }
            },
            "syncs": {
                "bench-sync": {
                    "source": "bench-fs",
                    "pusher": "bench-fusion",
                    "disabled": False,
                    "audit_interval_sec": kwargs.get("audit_interval", 0),
                    "sentinel_interval_sec": kwargs.get("sentinel_interval", 0)
                }
            }
        }
        with open(os.path.join(self.env_dir, "agent-config.yaml"), "w") as f:
            yaml.dump(config, f)
            
        cmd = [
            "fustor-agent", "start",
            "-p", str(self.agent_port)
        ]
        log_file = open(os.path.join(self.env_dir, "agent.log"), "a")
        env = os.environ.copy()
        env["FUSTOR_HOME"] = self.env_dir
        
        p = subprocess.Popen(cmd, env=env, stdout=log_file, stderr=subprocess.STDOUT)
        log_file.close() # Close in parent
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

    def trigger_agent_audit(self, sync_id="bench-sync"):
        """Triggers audit for a sync instance via Agent API."""
        url = f"http://localhost:{self.agent_port}/api/instances/syncs/{sync_id}/_actions/trigger_audit"
        res = requests.post(url)
        res.raise_for_status()
        return res.json()

    def trigger_agent_sentinel(self, sync_id="bench-sync"):
        """Triggers sentinel for a sync instance via Agent API."""
        url = f"http://localhost:{self.agent_port}/api/instances/syncs/{sync_id}/_actions/trigger_sentinel"
        res = requests.post(url)
        res.raise_for_status()
        return res.json()

    def wait_for_leader(self, sync_id="bench-sync", timeout=30, start_offset=0):
        click.echo(f"Waiting for {sync_id} to become LEADER...")
        pattern = rf"Assigned LEADER role for {sync_id}"
        return self.wait_for_log(self.get_agent_log_path(), pattern, start_offset=start_offset, timeout=timeout)

    def stop_agent(self):
        # Find Agent process in self.processes (index 2 usually, but look for it)
        # Since we append, it's the last one? Or we track index?
        # Simpler: Kill all fustor-agent processes
        subprocess.run(["pkill", "-9", "-f", "fustor-agent"])
        
        # Also clean up internal process list if possible, but it's hard to map Popen to 'agent'
        # just assume stop_all will clean up objects.
        
        # Remove PID file
        agent_pid = os.path.join(self.env_dir, "agent.pid")
        if os.path.exists(agent_pid):
            os.remove(agent_pid)
        time.sleep(1)

    def stop_all(self):
        click.echo("Stopping all services...")
        # Deep kill
        subprocess.run(["pkill", "-f", "fustor-registry"])
        subprocess.run(["pkill", "-f", "fustor-fusion"])
        subprocess.run(["pkill", "-f", "fustor-agent"])
        
        for p in self.processes:
            try:
                p.terminate()
                p.wait(timeout=2)
            except:
                p.kill()
        self.processes = []

