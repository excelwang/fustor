import os
import time
import shutil
import signal
import subprocess
import requests
import yaml
import click

class ServiceManager:
    def __init__(self, env_dir: str, data_dir: str):
        self.env_dir = os.path.abspath(env_dir)
        self.data_dir = os.path.abspath(data_dir)
        self.registry_port = 18101
        self.fusion_port = 18102
        self.agent_port = 18100
        self.processes = []
        
        # Paths
        self.venv_bin = os.path.abspath(".venv/bin") # Assuming run from repo root

    def setup_env(self):
        if os.path.exists(self.env_dir):
            shutil.rmtree(self.env_dir)
        os.makedirs(self.env_dir, exist_ok=True)
        
        # Generate a random token for internal communication
        import secrets
        self.client_token = secrets.token_urlsafe(32)
        
        # Registry DB config
        with open(os.path.join(self.env_dir, ".env"), "w") as f:
            f.write(f"DATABASE_URL=sqlite+aiosqlite:///{self.env_dir}/registry.db\n")
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
            f"{self.venv_bin}/fustor-registry", "start",
            "-p", str(self.registry_port)
        ]
        # We don't use -D, we manage the process ourselves
        log_file = open(os.path.join(self.env_dir, "registry.log"), "w")
        env = os.environ.copy()
        env["FUSTOR_HOME"] = self.env_dir
        env["FUSTOR_REGISTRY_CLIENT_TOKEN"] = self.client_token
        
        p = subprocess.Popen(cmd, env=env, stdout=log_file, stderr=subprocess.STDOUT)
        self.processes.append(p)
        
        if not self._wait_for_service(f"http://localhost:{self.registry_port}/health", "Registry"):
            raise RuntimeError("Registry start failed")

    def configure_system(self):
        # 1. Login to get token
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
            
            # 2. Create Datastore
            click.echo("Creating Datastore...")
            res = requests.post(f"{reg_url}/datastores/", json={
                "name": "BenchmarkDS", "description": "Auto-generated"
            }, headers=headers)
            if res.status_code not in (200, 201):
                 raise RuntimeError(f"DS creation failed: {res.text}")
            ds_id = res.json()["id"]
            
            # 3. Create API Key
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
            f"{self.venv_bin}/fustor-fusion", "start",
            "-p", str(self.fusion_port)
        ]
        log_file = open(os.path.join(self.env_dir, "fusion.log"), "w")
        env = os.environ.copy()
        env["FUSTOR_HOME"] = self.env_dir
        env["FUSTOR_FUSION_REGISTRY_URL"] = f"http://localhost:{self.registry_port}"
        env["FUSTOR_REGISTRY_CLIENT_TOKEN"] = self.client_token
        
        p = subprocess.Popen(cmd, env=env, stdout=log_file, stderr=subprocess.STDOUT)
        self.processes.append(p)
        
        # Wait for Fusion port to open
        click.echo(f"Waiting for Fusion at http://localhost:{self.fusion_port}...")
        start = time.time()
        while time.time() - start < 30:
            try:
                # Even if 401/403, it means the server is responding
                requests.get(f"http://localhost:{self.fusion_port}/", timeout=1)
                click.echo("Fusion is up.")
                return
            except requests.ConnectionError:
                time.sleep(0.5)
        raise RuntimeError("Fusion start failed")

    def start_agent(self, api_key: str):
        # 1. Write Config
        # Agent expects a Dict structure {id: config}, not a List
        # Matching SourceConfig, PusherConfig, SyncConfig models
        config = {
            "sources": {
                "bench-fs": {
                    "driver": "fs",
                    "uri": self.data_dir,
                    "credential": {"user": "admin", "passwd": ""},
                    "disabled": False,
                    "driver_params": {"min_monitoring_window_days": 1}
                }
            },
            "pushers": {
                "bench-fusion": {
                    "driver": "fusion",
                    "endpoint": f"http://localhost:{self.fusion_port}/ingestor-api/v1/events",
                    "credential": {"key": api_key},
                    "disabled": False
                }
            },
            "syncs": {
                "bench-sync": {
                    "source": "bench-fs",
                    "pusher": "bench-fusion",
                    "disabled": False
                }
            }
        }
        dot_fustor = os.path.join(self.env_dir, ".fustor")
        os.makedirs(dot_fustor, exist_ok=True)
        with open(os.path.join(dot_fustor, "agent-config.yaml"), "w") as f:
            yaml.dump(config, f)
            
        # 2. Start Process
        cmd = [
            f"{self.venv_bin}/fustor-agent", "start",
            "-p", str(self.agent_port)
        ]
        log_file = open(os.path.join(self.env_dir, "agent.log"), "w")
        env = os.environ.copy()
        env["FUSTOR_HOME"] = self.env_dir
        
        p = subprocess.Popen(cmd, env=env, stdout=log_file, stderr=subprocess.STDOUT)
        self.processes.append(p)
        
        self._wait_for_service(f"http://localhost:{self.agent_port}/", "Agent")

    def stop_all(self):
        click.echo("Stopping all services...")
        for p in self.processes:
            try:
                p.terminate()
                p.wait(timeout=5)
            except:
                p.kill()
        self.processes = []
