import time
import requests
import click
import random
import os
import json
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor, as_completed

from .generator import DataGenerator
from .services import ServiceManager
from .tasks import (
    run_find_recursive_metadata_task,
    run_single_fusion_req,
    run_find_sampling_phase,
    run_find_validation_phase
)
from .reporter import calculate_stats, generate_html_report

class BenchmarkRunner:
    def __init__(self, run_dir, target_dir, fusion_api_url=None, api_key=None):
        self.run_dir = os.path.abspath(run_dir)
        self.env_dir = os.path.join(self.run_dir, ".fustor")
        self.data_dir = os.path.abspath(target_dir)
        self.external_api_url = fusion_api_url.rstrip('/') if fusion_api_url else None
        self.external_api_key = api_key
        self.services = ServiceManager(self.run_dir) 
        self.services.data_dir = self.data_dir
        self.generator = DataGenerator(self.data_dir)

    def _discover_leaf_targets_via_api(self, api_key: str, depth: int):
        """Finds directories at the specified depth using Fusion API."""
        prefix_depth = len(self.data_dir.strip('/').split('/')) if self.data_dir != '/' else 0
        max_fetch_depth = depth + prefix_depth
        fusion_url = self.external_api_url or f"http://localhost:{self.services.fusion_port}"
        headers = {"X-API-Key": api_key}
        
        try:
            res = requests.get(f"{fusion_url}/views/fs/tree", params={"path": "/", "max_depth": max_fetch_depth, "only_path": "true"}, headers=headers, timeout=30)
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
                        for child in (node.get('children', {}).values() if isinstance(node.get('children'), dict) else node.get('children', [])):
                            find_and_walk(child, 0, False)
                        return
                if current_rel_depth == depth:
                    if node.get('content_type') == 'directory': targets.append(path)
                    return
                for child in (node.get('children', {}).values() if isinstance(node.get('children'), dict) else node.get('children', [])):
                    find_and_walk(child, current_rel_depth + 1, True)
            find_and_walk(tree_data, 0, False)
        except Exception: return ["/"]
        return targets if targets else ["/"]

    def run_concurrent_baseline(self, targets, concurrency=20, requests_count=100):
        click.echo(f"Running Concurrent OS Baseline: {concurrency} workers, {requests_count} reqs...")
        shuffled_targets = list(targets); random.shuffle(shuffled_targets)
        sampled_paths = [shuffled_targets[i % len(shuffled_targets)] for i in range(requests_count)]
        tasks = [(self.data_dir, t) for t in sampled_paths]
        latencies = []
        start_total = time.time()
        with ProcessPoolExecutor(max_workers=concurrency) as executor:
            futures = [executor.submit(run_find_recursive_metadata_task, t) for t in tasks]
            for f in as_completed(futures): latencies.append(f.result())
        return calculate_stats(latencies, time.time() - start_total, requests_count)

    def run_concurrent_find_integrity(self, targets, concurrency=20, requests_count=100, interval=60.0):
        """
        Optimized Batch OS Integrity Check.
        1. Concurrent Sampling.
        2. Single Global Sleep.
        3. Concurrent Validation.
        """
        click.echo(f"Running Optimized OS Integrity ({requests_count} total reqs, {concurrency} workers, {interval}s global wait)...")
        shuffled_targets = list(targets); random.shuffle(shuffled_targets)
        sampled_paths = [shuffled_targets[i % len(shuffled_targets)] for i in range(requests_count)]
        
        # --- Phase 1: Batch Sampling ---
        click.echo(f"  [Pass 1/2] Batch sampling {requests_count} targets...")
        sampling_latencies = []
        metadata_dicts = []
        start_p1 = time.time()
        with ProcessPoolExecutor(max_workers=concurrency) as executor:
            futures = [executor.submit(run_find_sampling_phase, (self.data_dir, t)) for t in sampled_paths]
            for f in as_completed(futures):
                lat, data = f.result()
                sampling_latencies.append(lat)
                metadata_dicts.append(data)
        wall_time_p1 = time.time() - start_p1

        # --- Phase 2: Global Silence Window ---
        click.echo(f"  [Wait] Global silence window: {interval}s...")
        time.sleep(interval)

        # --- Phase 3: Batch Validation ---
        click.echo(f"  [Pass 2/2] Batch validating {requests_count} targets...")
        validation_latencies = []
        start_p2 = time.time()
        with ProcessPoolExecutor(max_workers=concurrency) as executor:
            futures = [executor.submit(run_find_validation_phase, (data, interval)) for data in metadata_dicts]
            for f in as_completed(futures):
                validation_latencies.append(f.result())
        wall_time_p2 = time.time() - start_p2

        # --- Aggregate: Logic Total Latency per request ---
        total_latencies = [l1 + interval + l2 for l1, l2 in zip(sampling_latencies, validation_latencies)]
        total_wall_time = wall_time_p1 + interval + wall_time_p2
        
        return calculate_stats(total_latencies, total_wall_time, requests_count)

    def run_concurrent_fusion(self, api_key, targets, concurrency=20, requests_count=100):
        click.echo(f"Running Concurrent Fusion API: {concurrency} workers, {requests_count} reqs...")
        fusion_url = self.external_api_url or f"http://localhost:{self.services.fusion_port}"
        headers = {"X-API-Key": api_key}
        shuffled_targets = list(targets); random.shuffle(shuffled_targets)
        tasks = [shuffled_targets[i % len(shuffled_targets)] for i in range(requests_count)]
        latencies = []
        start_total = time.time()
        with ThreadPoolExecutor(max_workers=concurrency) as executor:
            futures = [executor.submit(run_single_fusion_req, fusion_url, headers, t, False, False) for t in tasks]
            for f in as_completed(futures):
                res = f.result()
                if res is not None: latencies.append(res)
        return calculate_stats(latencies, time.time() - start_total, requests_count)

    def run_concurrent_fusion_dry_run(self, api_key, targets, concurrency=20, requests_count=100):
        click.echo(f"Running Concurrent Fusion Dry-run: {concurrency} workers, {requests_count} reqs...")
        fusion_url = self.external_api_url or f"http://localhost:{self.services.fusion_port}"
        headers = {"X-API-Key": api_key}
        shuffled_targets = list(targets); random.shuffle(shuffled_targets)
        tasks = [shuffled_targets[i % len(shuffled_targets)] for i in range(requests_count)]
        latencies = []
        start_total = time.time()
        with ThreadPoolExecutor(max_workers=concurrency) as executor:
            futures = [executor.submit(run_single_fusion_req, fusion_url, headers, t, True, False) for t in tasks]
            for f in as_completed(futures):
                res = f.result()
                if res is not None: latencies.append(res)
        return calculate_stats(latencies, time.time() - start_total, requests_count)

    def run_concurrent_fusion_dry_net(self, concurrency=20, requests_count=100):
        click.echo(f"Running Concurrent Fusion Dry-net: {concurrency} workers, {requests_count} reqs...")
        fusion_url = self.external_api_url or f"http://localhost:{self.services.fusion_port}"
        latencies = []
        start_total = time.time()
        with ThreadPoolExecutor(max_workers=concurrency) as executor:
            futures = [executor.submit(run_single_fusion_req, fusion_url, {}, None, False, True) for _ in range(requests_count)]
            for f in as_completed(futures):
                res = f.result()
                if res is not None: latencies.append(res)
        return calculate_stats(latencies, time.time() - start_total, requests_count)

    def wait_for_sync(self, api_key: str):
        click.echo("Waiting for Fusion readiness...")
        fusion_url = self.external_api_url or f"http://localhost:{self.services.fusion_port}"
        headers = {"X-API-Key": api_key}
        start_wait = time.time()
        while True:
            elapsed = time.time() - start_wait
            try:
                res = requests.get(f"{fusion_url}/views/fs/tree", params={"path": "/", "max_depth": 1, "only_path": "true"}, headers=headers, timeout=5)
                if res.status_code == 200:
                    click.echo(f"  [Fusion] READY after {elapsed:.1f}s.")
                    break
                elif res.status_code == 503:
                    if int(elapsed) % 5 == 0: click.echo(f"  [Fusion] Still syncing... ({int(elapsed)}s)")
                else: raise RuntimeError(f"Unexpected response: {res.status_code}")
            except Exception: pass
            time.sleep(5)

    def run(self, concurrency=20, reqs=200, target_depth=5, integrity_interval=60.0):
        data_exists = os.path.exists(self.data_dir) and len(os.listdir(self.data_dir)) > 0
        if not data_exists: raise RuntimeError("Benchmark data missing")
        click.echo(f"Using data directory: {self.data_dir}")
        try:
            if self.external_api_url:
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
                "metadata": {"total_files_in_scope": stats_data.get("total_files", 0), "total_directories_in_scope": stats_data.get("total_directories", 0), "source_path": self.data_dir, "api_endpoint": fusion_url, "integrity_interval": integrity_interval},
                "depth": target_depth, "requests": reqs, "concurrency": concurrency, "target_directory_count": len(targets),
                "os": os_stats, "os_integrity": os_integrity_stats, "fusion_dry_net": fusion_dry_net_stats, "fusion_dry": fusion_dry_stats, "fusion": fusion_stats, "timestamp": time.strftime("%Y-%m-%d %H:%M:%S")
            }
            os.makedirs(os.path.join(self.run_dir, "results"), exist_ok=True)
            with open(os.path.join(self.run_dir, "results/stress-find.json"), "w") as f: json.dump(final_results, f, indent=2)
            generate_html_report(final_results, os.path.join(self.run_dir, "results/stress-find.html"))

            click.echo("\n" + "="*135)
            click.echo(f"RECURSIVE METADATA RETRIEVAL PERFORMANCE (DEPTH {target_depth}, INTERVAL {integrity_interval}s)")
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