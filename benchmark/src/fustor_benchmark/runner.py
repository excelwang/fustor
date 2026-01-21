import time
import requests
import click
import random
import os
import json
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor, as_completed

from .generator import DataGenerator
from .services import ServiceManager
from .tasks import run_find_recursive_metadata_task, run_single_fusion_req, run_find_integrity_task
from .reporter import calculate_stats, generate_html_report

class BenchmarkRunner:
    def __init__(self, run_dir, target_dir, fusion_api_url=None, api_key=None):
        self.run_dir = os.path.abspath(run_dir)
        self.env_dir = os.path.join(self.run_dir, ".fustor")
        self.data_dir = os.path.abspath(target_dir)
        
        # External API support
        self.external_api_url = fusion_api_url.rstrip('/') if fusion_api_url else None
        self.external_api_key = api_key
        
        self.services = ServiceManager(self.run_dir) 
        self.services.data_dir = self.data_dir
        self.generator = DataGenerator(self.data_dir)

    def _discover_leaf_targets_via_api(self, api_key: str, depth: int):
        """Finds directories at the specified depth relative to data_dir using Fusion API."""
        prefix_depth = len(self.data_dir.strip('/').split('/')) if self.data_dir != '/' else 0
        max_fetch_depth = depth + prefix_depth
        
        click.echo(f"Discovering target directories at depth {depth} (prefix: {prefix_depth}, total: {max_fetch_depth}) via Fusion API...")
        
        fusion_url = self.external_api_url or f"http://localhost:{self.services.fusion_port}"
        headers = {"X-API-Key": api_key}
        
        try:
            res = requests.get(
                f"{fusion_url}/views/fs/tree", 
                params={"path": "/", "max_depth": max_fetch_depth, "only_path": "true"}, 
                headers=headers, 
                timeout=30
            )
            if res.status_code != 200: return ["/"]
            
            tree_data = res.json()
            targets = []

            def find_and_walk(node, current_rel_depth, inside_mount):
                path = node.get('path', '')
                if not inside_mount:
                    if os.path.abspath(path) == os.path.abspath(self.data_dir):
                        inside_mount = True
                        current_rel_depth = 0
                    else:
                        children = node.get('children', {})
                        if isinstance(children, dict):
                            for child in children.values(): find_and_walk(child, 0, False)
                        elif isinstance(children, list):
                            for child in children: find_and_walk(child, 0, False)
                        return
                if current_rel_depth == depth:
                    if node.get('content_type') == 'directory': targets.append(path)
                    return
                children = node.get('children', {})
                if isinstance(children, dict):
                    for child in children.values(): find_and_walk(child, current_rel_depth + 1, True)
                elif isinstance(children, list):
                    for child in children: find_and_walk(child, current_rel_depth + 1, True)

            find_and_walk(tree_data, 0, False)
        except Exception as e:
            click.echo(click.style(f"Discovery error: {e}. Falling back to root.", fg="yellow"))
            return ["/"]

        if not targets:
            click.echo(click.style(f"No targets found at relative depth {depth}."), fg="yellow")
            targets = ["/"]
        else:
            example_path = random.choice(targets)
            click.echo(f"  [Check] Example target path at relative depth {depth}: '{example_path}'")
            
        click.echo(f"Discovered {len(targets)} candidate directories via API.")
        return targets

    def run_concurrent_baseline(self, targets, concurrency=20, requests_count=100):
        click.echo(f"Running Concurrent OS Baseline (Recursive find + parsing): {concurrency} workers, {requests_count} requests...")
        
        shuffled_targets = list(targets)
        random.shuffle(shuffled_targets)
        sampled_paths = [shuffled_targets[i % len(shuffled_targets)] for i in range(requests_count)]
        tasks = [(self.data_dir, t) for t in sampled_paths]
        
        latencies = []
        start_total = time.time()
        with ProcessPoolExecutor(max_workers=concurrency) as executor:
            futures = [executor.submit(run_find_recursive_metadata_task, t) for t in tasks]
            for f in as_completed(futures): latencies.append(f.result())
        total_time = time.time() - start_total
        return calculate_stats(latencies, total_time, requests_count)

    def run_concurrent_find_integrity(self, targets, concurrency=20, requests_count=100, interval=60.0):
        click.echo(f"Running Concurrent OS Integrity (Double-Sampling @ {interval}s wait): {concurrency} workers, {requests_count} requests...")
        
        shuffled_targets = list(targets)
        random.shuffle(shuffled_targets)
        sampled_paths = [shuffled_targets[i % len(shuffled_targets)] for i in range(requests_count)]
        # Pass interval to task
        tasks = [(self.data_dir, t, interval) for t in sampled_paths]
        
        latencies = []
        start_total = time.time()
        with ProcessPoolExecutor(max_workers=concurrency) as executor:
            futures = [executor.submit(run_find_integrity_task, t) for t in tasks]
            for f in as_completed(futures): latencies.append(f.result())
        total_time = time.time() - start_total
        return calculate_stats(latencies, total_time, requests_count)

    def run_concurrent_fusion(self, api_key, targets, concurrency=20, requests_count=100):
        click.echo(f"Running Concurrent Fusion API (Recursive Tree): {concurrency} workers, {requests_count} requests...")
        fusion_url = self.external_api_url or f"http://localhost:{self.services.fusion_port}"
        headers = {"X-API-Key": api_key}
        
        shuffled_targets = list(targets)
        random.shuffle(shuffled_targets)
        tasks = [shuffled_targets[i % len(shuffled_targets)] for i in range(requests_count)]
            
        latencies = []
        start_total = time.time()
        with ThreadPoolExecutor(max_workers=concurrency) as executor:
            futures = [executor.submit(run_single_fusion_req, fusion_url, headers, t, False, False) for t in tasks]
            for f in as_completed(futures):
                res = f.result()
                if res is not None: latencies.append(res)
        total_time = time.time() - start_total
        return calculate_stats(latencies, total_time, requests_count)

    def run_concurrent_fusion_dry_run(self, api_key, targets, concurrency=20, requests_count=100):
        click.echo(f"Running Concurrent Fusion API (Dry-run): {concurrency} workers, {requests_count} requests...")
        fusion_url = self.external_api_url or f"http://localhost:{self.services.fusion_port}"
        headers = {"X-API-Key": api_key}
        
        # We still send paths to simulate realistic request sizes, though server will ignore them
        shuffled_targets = list(targets)
        random.shuffle(shuffled_targets)
        tasks = [shuffled_targets[i % len(shuffled_targets)] for i in range(requests_count)]
            
        latencies = []
        start_total = time.time()
        with ThreadPoolExecutor(max_workers=concurrency) as executor:
            futures = [executor.submit(run_single_fusion_req, fusion_url, headers, t, True, False) for t in tasks]
            for f in as_completed(futures):
                res = f.result()
                if res is not None: latencies.append(res)
        total_time = time.time() - start_total
        return calculate_stats(latencies, total_time, requests_count)

    def run_concurrent_fusion_dry_net(self, concurrency=20, requests_count=100):
        click.echo(f"Running Concurrent Fusion API (Dry-net): {concurrency} workers, {requests_count} requests...")
        fusion_url = self.external_api_url or f"http://localhost:{self.services.fusion_port}"
            
        latencies = []
        start_total = time.time()
        with ThreadPoolExecutor(max_workers=concurrency) as executor:
            # path and headers are not used for dry_net
            futures = [executor.submit(run_single_fusion_req, fusion_url, {}, None, False, True) for _ in range(requests_count)]
            for f in as_completed(futures):
                res = f.result()
                if res is not None: latencies.append(res)
        total_time = time.time() - start_total
        return calculate_stats(latencies, total_time, requests_count)

    def wait_for_sync(self, api_key: str):
        click.echo("Waiting for Fusion readiness (Alternating between API status and Agent logs)...")
        fusion_url = self.external_api_url or f"http://localhost:{self.services.fusion_port}"
        headers = {"X-API-Key": api_key}
        start_wait = time.time()
        while True:
            elapsed = time.time() - start_wait
            try:
                res = requests.get(f"{fusion_url}/views/fs/tree", params={"path": "/", "max_depth": 1, "only_path": "true"}, headers=headers, timeout=5)
                if res.status_code == 200:
                    click.echo(f"  [Fusion] READY (200 OK) after {elapsed:.1f}s.")
                    break
                elif res.status_code == 503:
                    if int(elapsed) % 5 == 0: click.echo(f"  [Fusion] Still syncing... (Elapsed: {int(elapsed)}s)")
                else: raise RuntimeError(f"  [Fusion] Unexpected API response: {res.status_code}")
            except requests.ConnectionError: pass
            except Exception as e: click.echo(f"  [Fusion] Warning: Connection glitch ({e})")
            time.sleep(5)

    def run(self, concurrency=20, reqs=200, target_depth=5, integrity_interval=60.0):
        data_exists = os.path.exists(self.data_dir) and len(os.listdir(self.data_dir)) > 0
        if not data_exists:
            click.echo(click.style(f"FATAL: Data directory '{self.data_dir}' is empty or missing.", fg="red", bold=True))
            raise RuntimeError("Benchmark data missing")

        click.echo(f"Using data directory: {self.data_dir}")
        try:
            if self.external_api_url:
                if not self.external_api_key: raise RuntimeError("--api-key is required when using --fusion-api")
                api_key = self.external_api_key
            else:
                self.services.setup_env()
                self.services.start_registry()
                api_key = self.services.configure_system()
                self.services.start_fusion()
                self.services.start_agent(api_key)
                time.sleep(2)
            
            self.wait_for_sync(api_key)
            targets = self._discover_leaf_targets_via_api(api_key, target_depth)
            
            os_stats = self.run_concurrent_baseline(targets, concurrency, reqs)
            os_integrity_stats = self.run_concurrent_find_integrity(targets, concurrency, reqs, interval=integrity_interval)
            fusion_dry_net_stats = self.run_concurrent_fusion_dry_net(concurrency, reqs)
            fusion_dry_stats = self.run_concurrent_fusion_dry_run(api_key, targets, concurrency, reqs)
            fusion_stats = self.run_concurrent_fusion(api_key, targets, concurrency, reqs)
            
            fusion_url = self.external_api_url or f"http://localhost:{self.services.fusion_port}"
            res_stats = requests.get(f"{fusion_url}/views/fs/stats", headers={"X-API-Key": api_key})
            stats_data = res_stats.json() if res_stats.status_code == 200 else {}
            
            final_results = {
                "metadata": {
                    "total_files_in_scope": stats_data.get("total_files", 0), 
                    "total_directories_in_scope": stats_data.get("total_directories", 0), 
                    "source_path": self.data_dir, 
                    "api_endpoint": fusion_url,
                    "integrity_interval": integrity_interval
                },
                "depth": target_depth, "requests": reqs, "concurrency": concurrency, "target_directory_count": len(targets),
                "os": os_stats, "os_integrity": os_integrity_stats, 
                "fusion_dry_net": fusion_dry_net_stats, "fusion_dry": fusion_dry_stats, "fusion": fusion_stats, "timestamp": time.strftime("%Y-%m-%d %H:%M:%S")
            }

            os.makedirs(os.path.join(self.run_dir, "results"), exist_ok=True)
            with open(os.path.join(self.run_dir, "results/stress-find.json"), "w") as f: json.dump(final_results, f, indent=2)
            generate_html_report(final_results, os.path.join(self.run_dir, "results/stress-find.html"))

            click.echo("\n" + "="*135)
            click.echo(f"RECURSIVE METADATA RETRIEVAL PERFORMANCE (DEPTH {target_depth})")
            click.echo(f"Data Scale: {stats_data.get('total_files', 0):,} files | Targets: {len(targets)}")
            click.echo("="*135)
            click.echo(f"{ 'Metric (ms)':<25} | {'OS Baseline':<18} | {'OS Integrity':<18} | {'Fusion Dry-Net':<18} | {'Fusion Dry-Run':<18} | {'Fusion API':<18}")
            click.echo("-" * 135)
            click.echo(f"{ 'Avg Latency':<25} | {os_stats['avg']:10.2f} ms      | {os_integrity_stats['avg']:10.2f} ms      | {fusion_dry_net_stats['avg']:10.2f} ms      | {fusion_dry_stats['avg']:10.2f} ms      | {fusion_stats['avg']:10.2f} ms")
            click.echo(f"{ 'P50 Latency':<25} | {os_stats['p50']:10.2f} ms      | {os_integrity_stats['p50']:10.2f} ms      | {fusion_dry_net_stats['p50']:10.2f} ms      | {fusion_dry_stats['p50']:10.2f} ms      | {fusion_stats['p50']:10.2f} ms")
            click.echo(f"{ 'P99 Latency':<25} | {os_stats['p99']:10.2f} ms      | {os_integrity_stats['p99']:10.2f} ms      | {fusion_dry_net_stats['p99']:10.2f} ms      | {fusion_dry_stats['p99']:10.2f} ms      | {fusion_stats['p99']:10.2f} ms")
            click.echo(f"{ 'Throughput (QPS)':<25} | {os_stats['qps']:10.1f}         | {os_integrity_stats['qps']:10.1f}         | {fusion_dry_net_stats['qps']:10.1f}         | {fusion_dry_stats['qps']:10.1f}         | {fusion_stats['qps']:10.1f}")
            click.echo("-" * 135)
            click.echo(click.style(f"\nJSON results saved to: {os.path.join(self.run_dir, 'results/stress-find.json')}", fg="cyan"))
            click.echo(click.style(f"Visual HTML report saved to: {os.path.join(self.run_dir, 'results/stress-find.html')}", fg="green", bold=True))
        finally:
            if not self.external_api_url: self.services.stop_all()
