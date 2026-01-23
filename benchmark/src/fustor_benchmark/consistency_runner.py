import time
import os
import requests
import click
import logging
from .services import ServiceManager

logger = logging.getLogger(__name__)

class ConsistencyRunner:
    def __init__(self, run_dir, target_dir, fusion_api_url=None, api_key=None):
        self.run_dir = os.path.abspath(run_dir)
        self.data_dir = os.path.abspath(target_dir)
        self.external_api_url = fusion_api_url
        self.external_api_key = api_key
        self.services = ServiceManager(self.run_dir)
        self.services.data_dir = self.data_dir

    def get_fusion_url(self):
        return self.external_api_url or f"http://localhost:{self.services.fusion_port}"

    def wait_for_file_metadata(self, api_key, rel_path, timeout=30):
        url = self.get_fusion_url()
        start = time.time()
        while time.time() - start < timeout:
            try:
                # API expects absolute path in param, referring to ROOT of the tree (which is data_dir)
                search_api_path = "/"
                target_name = os.path.basename(rel_path)
                
                # Simple implementation: we just list root and look for file. 
                # Bench tests are flat usually.
                
                res = requests.get(f"{url}/api/v1/views/fs/tree", 
                                   params={"path": search_api_path, "max_depth": 1}, 
                                   headers={"X-API-Key": api_key},
                                   timeout=2)
                
                if res.status_code == 200:
                    data = res.json()
                    children = data.get('children', {})
                    if target_name in children:
                        return children[target_name]
            except Exception as e:
                pass
            time.sleep(1)
        return None

    def run_tests(self):
        # Clean data dir
        if os.path.exists(self.data_dir):
            import shutil
            shutil.rmtree(self.data_dir)
        os.makedirs(self.data_dir, exist_ok=True)

        click.echo("Starting services for consistency tests...")
        self.services.setup_env()
        self.services.start_registry()
        api_key = self.services.configure_system()
        self.services.start_fusion()
        
        # Start Agent with fast audit interval (10s)
        self.services.start_agent(api_key, extra_env={"FUSTOR_AUDIT_INTERVAL": "10"}) 
        
        click.echo("Waiting for initial sync...")
        time.sleep(5)
        
        try:
             self.test_active_write(api_key)
             self.test_audit_recovery(api_key)
        except Exception as e:
             click.echo(click.style(f"Tests failed with error: {e}", fg="red"))
             import traceback
             traceback.print_exc()
        finally:
             self.services.stop_all()

    def test_active_write(self, api_key):
        click.echo("\n" + "="*60)
        click.echo("TEST 1: Active File Write Integrity")
        click.echo("="*60)
        filename = "streaming_test.log"
        filepath = os.path.join(self.data_dir, filename)
        
        # Start writing
        with open(filepath, "w") as f:
            f.write("Start\n")
            
        click.echo("  File created. Simulating continuous writes...")
        for i in range(10):
            with open(filepath, "a") as f:
                f.write(f"Line {i} " * 100 + "\n")
                f.flush()
                # fsync to make sure OS sees it, inotify triggers
                os.fsync(f.fileno())
            
            # Brief wait to allow propagation
            time.sleep(0.5)

            # Check
            meta = self.wait_for_file_metadata(api_key, filename, timeout=2)
            if meta:
                click.echo(f"    Write {i}: Fusion Size={meta.get('size')}")
            else:
                click.echo(f"    Write {i}: File not found in Fusion yet.")
            
        click.echo("  Writes finished. Verifying final state...")
        time.sleep(2)
        final_os_size = os.path.getsize(filepath)
        meta = self.wait_for_file_metadata(api_key, filename)
        if meta and meta.get('size') == final_os_size:
            click.echo(click.style(f"  [PASS] Final size matches: {final_os_size}", fg="green"))
        else:
            click.echo(click.style(f"  [FAIL] Size mismatch. OS={final_os_size}, Fusion={meta.get('size') if meta else 'None'}", fg="red"))
            if not meta:
                click.echo("    File metadata not found in Fusion.")

    def test_audit_recovery(self, api_key):
        click.echo("\n" + "="*60)
        click.echo("TEST 2: Audit Recovery (Blind Spot)")
        click.echo("="*60)
        # Create a file causing an event
        filename = "audit_target.txt"
        filepath = os.path.join(self.data_dir, filename)
        with open(filepath, "w") as f:
            f.write("Initial Content")
            
        # Wait for sync
        time.sleep(2)
        if not self.wait_for_file_metadata(api_key, filename):
             click.echo("  [SETUP FAIL] Initial file not synced.")
             return

        # Kill Agent to create blind spot
        click.echo("  Killing Agent...")
        self.services.stop_agent()
        
        # Modify file while agent is dead
        click.echo("  Modifying file during downtime...")
        with open(filepath, "a") as f:
            f.write(" + Appended Content During Downtime")
        expected_size = os.path.getsize(filepath)
            
        # Restart Agent
        click.echo("  Restarting Agent (fast audit enabled)...")
        # Ensure we wait enough for audit interval
        self.services.start_agent(api_key, extra_env={"FUSTOR_AUDIT_INTERVAL": "5"})
        
        # Wait for audit
        click.echo("  Waiting for audit convergence (max 20s)...")
        converged = False
        final_meta = None
        for _ in range(20):
            meta = self.wait_for_file_metadata(api_key, filename)
            final_meta = meta
            if meta and meta.get('size') == expected_size:
                converged = True
                break
            time.sleep(1)
            
        if converged:
            click.echo(click.style(f"  [PASS] File state reconciled. Size={expected_size}", fg="green"))
        else:
            click.echo(click.style(f"  [FAIL] Audit failed. OS Size={expected_size}, Fusion Size={final_meta.get('size') if final_meta else 'None'}", fg="red"))
