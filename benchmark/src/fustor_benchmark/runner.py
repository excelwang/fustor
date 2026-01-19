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

def run_find_task(args):
    """
    Standalone function for multiprocessing to avoid pickling 'self'.
    args: (data_dir, subdir, with_meta)
    """
    data_dir, subdir, with_meta = args
    target = os.path.join(data_dir, subdir)
    
    cmd = ["find", target, "-type", "f"]
    if with_meta:
        cmd.append("-ls")
        
    start = time.time()
    subprocess.run(cmd, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    return time.time() - start

class BenchmarkRunner:
    def __init__(self, data_dir="benchmark_data", env_dir="benchmark_env"):
        self.data_dir = os.path.abspath(data_dir) # Ensure absolute path
        self.env_dir = env_dir
        self.services = ServiceManager(env_dir, self.data_dir) # Pass updated data_dir
        self.generator = DataGenerator(self.data_dir)

    def _discover_targets(self, api_key):
        """Discover valid target paths from Fusion API (Generic BFS)"""
        click.echo("Discovering test targets via Fusion API...")
        fusion_url = f"http://localhost:{self.services.fusion_port}"
        headers = {"X-API-Key": api_key}
        
        # Queue for BFS: (path, depth)
        queue = [("/", 0)]
        found_targets = []
        
        start_time = time.time()
        
        while queue and len(found_targets) < 100:
            if time.time() - start_time > 60: # Max discovery time
                break
                
            curr_path, depth = queue.pop(0)
            if depth > 4: continue # Don't go too deep
            
            try:
                res = requests.get(f"{fusion_url}/views/fs/tree", params={"path": curr_path}, headers=headers, timeout=5)
                if res.status_code != 200: continue
                
                items = res.json()
                subdirs = [n['name'] for n in items if n.get('type') == 'directory']
                
                for sd in subdirs:
                    # Construct full API path
                    full_path = f"{curr_path.rstrip('/')}/{sd}"
                    
                    # Add to targets (relative path for find, remove leading slash)
                    rel_path = full_path.lstrip("/")
                    found_targets.append(rel_path)
                    
                    queue.append((full_path, depth + 1))
                    
                    if len(found_targets) >= 100: break
            except:
                pass
        
        click.echo(f"Discovered {len(found_targets)} target directories.")
        # Fallback to root if nothing found (e.g. flat directory)
        return found_targets if found_targets else ["."]

    def run_baseline_find(self):
        click.echo("-" * 40)
        click.echo(f"Running Baseline: `find {self.data_dir} -type f` (Path Only)...")
        start = time.time()
        subprocess.run(["find", self.data_dir, "-type", "f"], stdout=subprocess.DEVNULL)
        duration_simple = time.time() - start
        click.echo(f"  Time: {duration_simple:.4f} seconds")
        
        click.echo(f"Running Baseline: `find {self.data_dir} -type f -ls` (With Metadata)...")
        start = time.time()
        subprocess.run(["find", self.data_dir, "-type", "f", "-ls"], stdout=subprocess.DEVNULL)
        duration_meta = time.time() - start
        click.echo(f"  Time: {duration_meta:.4f} seconds")
        click.echo("-" * 40)
        return duration_simple, duration_meta

    def run_concurrent_baseline(self, targets, concurrency=20, requests_count=100, with_meta=False):
        mode = "(With Metadata)" if with_meta else "(Path Only)"
        click.echo(f"Running Concurrent Baseline {mode}: {concurrency} workers, {requests_count} requests...")
        
        # Prepare task arguments for standalone function
        # each task: (data_dir, subdir, with_meta)
        tasks = [(self.data_dir, random.choice(targets), with_meta) for _ in range(requests_count)]
        
        latencies = []
        start_total = time.time()
        
        with ProcessPoolExecutor(max_workers=concurrency) as executor:
            futures = [executor.submit(run_find_task, t) for t in tasks]
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
            api_path = "/" + path.lstrip("/") # Ensure leading slash
            res = requests.get(f"{url}/views/fs/tree", params={"path": api_path}, headers=headers, timeout=5)
            if res.status_code != 200:
                return None
        except Exception:
            return None
        return time.time() - start

    def run_concurrent_fusion(self, api_key, targets, concurrency=20, requests_count=100):
        click.echo(f"Running Concurrent Fusion API: {concurrency} workers, {requests_count} requests...")
        
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
        click.echo("Waiting for data sync to Fusion (Timeout: 300s)...")
        fusion_url = f"http://localhost:{self.services.fusion_port}"
        headers = {"X-API-Key": api_key}
        start_wait = time.time()
        
        while True:
            if time.time() - start_wait > 300:
                raise RuntimeError("Sync wait timed out (300s). Data not ready for benchmark.")
                
            try:
                res = requests.get(f"{fusion_url}/views/fs/tree", params={"path": "/"}, headers=headers)
                if res.status_code == 200:
                    data = res.json()
                    if isinstance(data, list) and len(data) > 0:
                        # Simple logic: if 'upload' exists (generated data) or any data (custom), wait for propagation
                        if any(n.get('name') == 'upload' for n in data):
                             click.echo("Root 'upload' folder detected. Waiting 30s for full propagation...")
                             time.sleep(30)
                             break
                        else:
                             click.echo("Data detected. Waiting 10s for stability...")
                             time.sleep(10)
                             break
            except:
                pass
            time.sleep(5)
        click.echo("Sync warmup complete.")

    def run(self, concurrency=20, reqs=200, force_gen=False, custom_target=False):
        # 1. Generate Data (only if not custom target)
        if not custom_target:
            if os.path.exists(self.data_dir) and not force_gen:
                click.echo(f"Data directory '{self.data_dir}' exists. Skipping generation.")
            else:
                self.generator.generate()
        else:
             click.echo(f"Benchmarking custom target directory: {self.data_dir}")
        
        try:
            # 2. Start System
            self.services.setup_env()
            self.services.start_registry()
            api_key = self.services.configure_system()
            self.services.start_fusion()
            self.services.start_agent(api_key)
            
            # 3. Wait for Sync
            self.wait_for_sync(api_key)
            
            # 4. Discover Targets
            targets = self._discover_targets(api_key)
            
            # 5. Baseline (Single)
            t_base_simple, t_base_meta = self.run_baseline_find()
            
            # 6. Baseline (Concurrent)
            qps_base_simple, avg_base_simple, _ = self.run_concurrent_baseline(targets, concurrency, reqs, with_meta=False)
            qps_base_meta, avg_base_meta, _ = self.run_concurrent_baseline(targets, concurrency, reqs, with_meta=True)
            
            # 7. Fusion Benchmark
            # Warmup single
            warmup_target = random.choice(targets) if targets else "/"
            fusion_url = f"http://localhost:{self.services.fusion_port}"
            start = time.time()
            requests.get(f"{fusion_url}/views/fs/tree", params={"path": "/" + warmup_target.lstrip("/")}, headers={"X-API-Key": api_key})
            t_fusion_single = time.time() - start
            
            # Concurrent
            qps_fusion, avg_fusion, _ = self.run_concurrent_fusion(api_key, targets, concurrency, reqs)
            
            click.echo("\n" + "="*60)
            click.echo("FINAL SCORECARD")
            click.echo("="*60)
            click.echo(f"{ 'Metric':<25} | {'Find (Path)':<15} | {'Find (Meta)':<15} | {'Fusion API':<15}")
            click.echo("-" * 78)
            click.echo(f"{ 'Latency (Full Scan)':<25} | {t_base_simple*1000:.1f} ms        | {t_base_meta*1000:.1f} ms        | {t_fusion_single*1000:.1f} ms")
            click.echo(f"{ 'Latency (Concurrent)':<25} | {avg_base_simple*1000:.2f} ms       | {avg_base_meta*1000:.2f} ms       | {avg_fusion*1000:.2f} ms")
            click.echo(f"{ 'Throughput (QPS)':<25} | {qps_base_simple:.1f}           | {qps_base_meta:.1f}           | {qps_fusion:.1f}")
            click.echo("-" * 78)

            click.echo("\n=== CSV Output for Charting ===")
            click.echo("Category,Metric,Find_Path_Only,Find_With_Meta,Fusion_API")
            click.echo(f"Full_Scan,Latency_ms,{t_base_simple*1000:.2f},{t_base_meta*1000:.2f},{t_fusion_single*1000:.2f}")
            click.echo(f"Concurrent_Access,Latency_ms,{avg_base_simple*1000:.2f},{avg_base_meta*1000:.2f},{avg_fusion*1000:.2f}")
            click.echo(f"Concurrent_Access,Throughput_QPS,{qps_base_simple:.2f},{qps_base_meta:.2f},{qps_fusion:.2f}")
            
        finally:
            self.services.stop_all()
