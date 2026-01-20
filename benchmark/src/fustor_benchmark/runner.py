import time
import subprocess
import requests
import click
import statistics
import random
import os
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor, as_completed
from .generator import DataGenerator
from .services import ServiceManager

def run_find_recursive_metadata_task(args):
    """
    Simulates a recursive metadata retrieval: `find <dir> -type f -ls`
    This matches the recursive nature of Fusion's tree API for a subdirectory.
    """
    data_dir, subdir = args
    # subdir is a path from Fusion API (e.g., /upload/submit/...)
    # We strip the leading slash to join correctly with data_dir
    target = os.path.join(data_dir, subdir.lstrip("/"))
    
    # Recursive search for all descendant files with metadata
    cmd = ["find", target, "-type", "f", "-ls"]
        
    start = time.time()
    subprocess.run(cmd, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    return time.time() - start

class BenchmarkRunner:
    def __init__(self, data_dir):
        self.data_dir = os.path.abspath(data_dir) # Ensure absolute path
        # Unified environment directory: {data-dir}/.fustor
        self.env_dir = os.path.join(self.data_dir, ".fustor")
        self.services = ServiceManager(self.data_dir) 
        self.generator = DataGenerator(self.data_dir)

    def _discover_leaf_targets_via_api(self, api_key: str, depth: int):
        """Finds directories at the specified depth using a single Fusion API call with max_depth."""
        click.echo(f"Discovering target directories at depth {depth} via Fusion API (optimized single-call)...")
        
        fusion_url = f"http://localhost:{self.services.fusion_port}"
        headers = {"X-API-Key": api_key}
        
        try:
            # Call API with max_depth and only_path to get the structure as fast as possible
            res = requests.get(
                f"{fusion_url}/views/fs/tree", 
                params={"path": "/", "max_depth": depth, "only_path": "true"}, 
                headers=headers, 
                timeout=30
            )
            if res.status_code != 200:
                click.echo(click.style(f"API discovery failed with status {res.status_code}. Falling back to root.", fg="red"))
                return ["/"]
            
            tree_data = res.json()
            targets = []

            # Local recursive walker to find nodes at the exact depth
            def walk(node, current_depth):
                if current_depth == depth:
                    if node.get('content_type') == 'directory':
                        targets.append(node.get('path'))
                    return

                children = node.get('children', {})
                # Handle both dict (recursive=True) and list (recursive=False) formats just in case
                if isinstance(children, dict):
                    for child_node in children.values():
                        walk(child_node, current_depth + 1)
                elif isinstance(children, list):
                    for child_node in children:
                        walk(child_node, current_depth + 1)

            walk(tree_data, 0)

        except Exception as e:
            click.echo(click.style(f"Discovery error: {e}. Falling back to root.", fg="yellow"))
            return ["/"]

        if not targets:
            click.echo(click.style(f"No targets found at depth {depth} in the returned tree.", fg="yellow"))
            targets = ["/"]
        else:
            example_path = random.choice(targets)
            click.echo(f"  [Check] Example target path at depth {depth}: '{example_path}'")
            
        click.echo(f"Discovered {len(targets)} candidate directories via single-call API.")
        return targets

    def run_concurrent_baseline(self, targets, concurrency=20, requests_count=100):
        click.echo(f"Running Concurrent OS Baseline (Recursive find -ls): {concurrency} workers, {requests_count} requests...")
        
        tasks = [(self.data_dir, random.choice(targets)) for _ in range(requests_count)]
        
        latencies = []
        start_total = time.time()
        
        with ProcessPoolExecutor(max_workers=concurrency) as executor:
            futures = [executor.submit(run_find_recursive_metadata_task, t) for t in tasks]
            for f in as_completed(futures):
                latencies.append(f.result())
                
        total_time = time.time() - start_total
        qps = requests_count / total_time
        avg = statistics.mean(latencies) if latencies else 0
        p99 = statistics.quantiles(latencies, n=100)[98] if len(latencies) >= 100 else max(latencies) if latencies else 0
        
        return qps, avg, p99

    def _run_single_fusion_req(self, url, headers, path):
        start = time.time()
        try:
            # Fusion's tree API is naturally recursive
            res = requests.get(f"{url}/views/fs/tree", params={"path": path}, headers=headers, timeout=10)
            if res.status_code != 200:
                return None
        except Exception:
            return None
        return time.time() - start

    def run_concurrent_fusion(self, api_key, targets, concurrency=20, requests_count=100):
        click.echo(f"Running Concurrent Fusion API (Recursive Tree): {concurrency} workers, {requests_count} requests...")
        
        url = f"http://localhost:{self.services.fusion_port}"
        headers = {"X-API-Key": api_key}
        tasks = [random.choice(targets) for _ in range(requests_count)]
        
        latencies = []
        start_total = time.time()
        
        with ThreadPoolExecutor(max_workers=concurrency) as executor:
            futures = [executor.submit(self._run_single_fusion_req, url, headers, t) for t in tasks]
            for f in as_completed(futures):
                res = f.result()
                if res is not None:
                    latencies.append(res)
        
        total_time = time.time() - start_total
        if not latencies:
            return 0, 0, 0

        qps = requests_count / total_time
        avg = statistics.mean(latencies)
        p99 = statistics.quantiles(latencies, n=100)[98] if len(latencies) >= 100 else max(latencies)
        
        return qps, avg, p99

    def wait_for_sync(self, api_key: str):
        click.echo("Waiting for Fusion readiness (Alternating between API status and Agent logs)...")
        fusion_url = f"http://localhost:{self.services.fusion_port}"
        headers = {"X-API-Key": api_key}
        start_wait = time.time()
        
        loop_count = 0
        while True:
            elapsed = time.time() - start_wait
                
            if loop_count % 2 == 0:
                is_ok, log_msg = self.services.check_agent_logs()
                if not is_ok:
                    raise RuntimeError(f"Agent reported error during sync: {log_msg}")
                if int(elapsed) % 30 < 5:
                    click.echo(f"  [Agent] Status: {log_msg}")

            try:
                res = requests.get(f"{fusion_url}/views/fs/tree", params={"path": "/"}, headers=headers, timeout=5)
                
                if res.status_code == 200:
                    click.echo(f"  [Fusion] READY (200 OK) after {elapsed:.1f}s.")
                    break
                elif res.status_code == 503:
                    if int(elapsed) % 5 == 0:
                        click.echo(f"  [Fusion] Still syncing... (Elapsed: {int(elapsed)}s)")
                else:
                    raise RuntimeError(f"  [Fusion] Unexpected API response: {res.status_code}")
            except requests.ConnectionError:
                pass
            except Exception as e:
                click.echo(f"  [Fusion] Warning: Connection glitch ({e})")
            
            loop_count += 1
            time.sleep(5)
        
        click.echo("Sync complete. Proceeding to benchmark.")

    def run(self, concurrency=20, reqs=200, target_depth=5, force_gen=False, custom_target=False):
        if not custom_target:
            if os.path.exists(self.data_dir) and not force_gen:
                click.echo(f"Data directory '{self.data_dir}' exists. Skipping generation.")
            else:
                self.generator.generate()
        else:
             click.echo(f"Benchmarking target directory: {self.data_dir}")
        
        try:
            self.services.setup_env()
            self.services.start_registry()
            api_key = self.services.configure_system()
            self.services.start_fusion()
            self.services.start_agent(api_key)
            
            time.sleep(2)
            is_ok, msg = self.services.check_agent_logs()
            if not is_ok:
                raise RuntimeError(f"Agent failed to initialize correctly: {msg}")
            click.echo("Agent health check passed.")
            
            self.wait_for_sync(api_key)
            
            # 4. Discover targets at specified depth via Fusion API
            targets = self._discover_leaf_targets_via_api(api_key, target_depth)
            
            # 5. OS Baseline (Concurrent Recursive Metadata Retrieval)
            qps_base, avg_base, p99_base = self.run_concurrent_baseline(targets, concurrency, reqs)
            
            # 6. Fusion Benchmark (Concurrent Recursive Metadata Retrieval)
            qps_fusion, avg_fusion, p99_fusion = self.run_concurrent_fusion(api_key, targets, concurrency, reqs)
            
            click.echo("\n" + "="*60)
            click.echo(f"RECURSIVE METADATA RETRIEVAL PERFORMANCE (DEPTH {target_depth})")
            click.echo("="*60)
            click.echo(f"{ 'Metric':<25} | {'OS (find -ls)':<18} | {'Fusion API':<18}")
            click.echo("-" * 65)
            click.echo(f"{ 'Avg Latency':<25} | {avg_base*1000:10.2f} ms      | {avg_fusion*1000:10.2f} ms")
            click.echo(f"{ 'P99 Latency':<25} | {p99_base*1000:10.2f} ms      | {p99_fusion*1000:10.2f} ms")
            click.echo(f"{ 'Throughput (QPS)':<25} | {qps_base:10.1f}         | {qps_fusion:10.1f}")
            click.echo("-" * 65)

            click.echo("\n=== CSV Output ===")
            click.echo("Metric,OS_Find_ls,Fusion_API")
            click.echo(f"Avg_Latency_ms,{avg_base*1000:.2f},{avg_fusion*1000:.2f}")
            click.echo(f"P99_Latency_ms,{p99_base*1000:.2f},{p99_fusion*1000:.2f}")
            click.echo(f"Throughput_QPS,{qps_base:.2f},{qps_fusion:.2f}")
            
        finally:
            self.services.stop_all()

