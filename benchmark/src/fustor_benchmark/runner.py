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
from .reporter import calculate_stats, generate_html_report, generate_fs_scan_report

class BenchmarkRunner:
    def __init__(self, run_dir, target_dir, fusion_api_url=None, api_key=None, base_port=18100):
        self.run_dir = os.path.abspath(run_dir)
        self.env_dir = os.path.join(self.run_dir, ".fustor")
        self.data_dir = os.path.abspath(target_dir)
        self.external_api_url = fusion_api_url.rstrip('/') if fusion_api_url else None
        self.external_api_key = api_key
        self.services = ServiceManager(self.run_dir, base_port=base_port) 
        self.services.data_dir = self.data_dir
        self.generator = DataGenerator(self.data_dir)

    def _discover_leaf_targets_via_api(self, api_key: str, depth: int):
        """Finds directories at the specified depth using Fusion API."""
        prefix_depth = len(self.data_dir.strip('/').split('/')) if self.data_dir != '/' else 0
        max_fetch_depth = depth + prefix_depth
        fusion_url = self.external_api_url or f"http://localhost:{self.services.fusion_port}"
        headers = {"X-API-Key": api_key}
        
        try:
            res = requests.get(f"{fusion_url}/api/v1/views/bench-view/tree", params={"path": "/", "max_depth": max_fetch_depth, "only_path": "true"}, headers=headers, timeout=30)
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
        Optimized Batch OS Integrity Check (NFS-Safe).
        """
        click.echo(f"Running Optimized OS Integrity ({requests_count} reqs, {concurrency} workers, {interval}s global wait)...")
        shuffled_targets = list(targets); random.shuffle(shuffled_targets)
        sampled_paths = [shuffled_targets[i % len(shuffled_targets)] for i in range(requests_count)]
        
        start_wall_clock = time.time()

        # --- Phase 1: Batch Sampling ---
        sampling_latencies = []
        metadata_dicts = []
        with ProcessPoolExecutor(max_workers=concurrency) as executor:
            futures = [executor.submit(run_find_sampling_phase, (self.data_dir, t)) for t in sampled_paths]
            for f in as_completed(futures):
                lat, data = f.result()
                sampling_latencies.append(lat)
                metadata_dicts.append(data)

        # --- Phase 2: Global Silence Window (Shared across batch) ---
        click.echo(f"  [Wait] Global silence window: {interval}s...")
        time.sleep(interval)

        # --- Phase 3: Batch Validation ---
        validation_latencies = []
        with ProcessPoolExecutor(max_workers=concurrency) as executor:
            futures = [executor.submit(run_find_validation_phase, (data, interval)) for data in metadata_dicts]
            for f in as_completed(futures):
                validation_latencies.append(f.result())

        total_wall_time = time.time() - start_wall_clock
        
        # Per-request logical latency still includes the full wait
        total_latencies = [l1 + interval + l2 for l1, l2 in zip(sampling_latencies, validation_latencies)]
        
        # QPS is calculated as Count / Total Wall Time of the whole process
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
                res = requests.get(f"{fusion_url}/api/v1/views/bench-view/tree", params={"path": "/", "max_depth": 1, "only_path": "true"}, headers=headers, timeout=5)
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
            res_stats = requests.get(f"{fusion_url}/api/v1/views/bench-view/stats", headers={"X-API-Key": api_key})
            stats_data = res_stats.json() if res_stats.status_code == 200 else {}
            final_results = {
                "metadata": {"total_files_in_scope": stats_data.get("total_files", 0), "total_directories_in_scope": stats_data.get("total_directories", 0), "source_path": self.data_dir, "api_endpoint": fusion_url, "integrity_interval": integrity_interval},
                "depth": target_depth, "requests": reqs, "concurrency": concurrency, "target_directory_count": len(targets),
                "os": os_stats, "os_integrity": os_integrity_stats, "fusion_dry_net": fusion_dry_net_stats, "fusion_dry": fusion_dry_stats, "fusion": fusion_stats, "timestamp": time.strftime("%Y-%m-%d %H:%M:%S")
            }
            os.makedirs(os.path.join(self.run_dir, "results"), exist_ok=True)
            with open(os.path.join(self.run_dir, "results/query-find.json"), "w") as f: json.dump(final_results, f, indent=2)
            generate_html_report(final_results, os.path.join(self.run_dir, "results/query-find.html"))

            click.echo("\n" + "="*135)
            click.echo(f"RECURSIVE METADATA RETRIEVAL PERFORMANCE (DEPTH {target_depth}, INTERVAL {integrity_interval}s)")
            click.echo(f"Data Scale: {stats_data.get('total_files', 0):,} files | Targets: {len(targets)}")
            click.echo("="*135)
            click.echo(f"{ 'Metric':<25} | {'OS Baseline':<18} | {'OS Integrity':<18} | {'Fusion Dry-Net':<18} | {'Fusion Dry-Run':<18} | {'Fusion API':<18}")
            click.echo("-" * 135)
            def fmt(s): return f"{s['avg']:10.2f} ms"
            def fmt_q(s): return f"{s['qps']:10.1f}"
            click.echo(f"{ 'Avg Latency':<25} | {fmt(os_stats)}      | {fmt(os_integrity_stats)}      | {fmt(fusion_dry_net_stats)}      | {fmt(fusion_dry_stats)}      | {fmt(fusion_stats)}")
            click.echo(f"{ 'P50 Latency':<25} | {os_stats['p50']:10.2f} ms      | {os_integrity_stats['p50']:10.2f} ms      | {fusion_dry_net_stats['p50']:10.2f} ms      | {fusion_dry_stats['p50']:10.2f} ms      | {fusion_stats['p50']:10.2f} ms")
            click.echo(f"{ 'P99 Latency':<25} | {os_stats['p99']:10.2f} ms      | {os_integrity_stats['p99']:10.2f} ms      | {fusion_dry_net_stats['p99']:10.2f} ms      | {fusion_dry_stats['p99']:10.2f} ms      | {fusion_stats['p99']:10.2f} ms")
            click.echo(f"{ 'Throughput (QPS)':<25} | {fmt_q(os_stats)}         | {fmt_q(os_integrity_stats)}         | {fmt_q(fusion_dry_net_stats)}         | {fmt_q(fusion_dry_stats)}         | {fmt_q(fusion_stats)}")
            click.echo("-" * 135)
            click.echo(click.style(f"\nJSON results saved to: {os.path.join(self.run_dir, 'results/query-find.json')}", fg="cyan"))
            click.echo(click.style(f"Visual HTML report saved to: {os.path.join(self.run_dir, 'results/query-find.html')}", fg="green", bold=True))
        finally:
            if not self.external_api_url: self.services.stop_all()

    def run_fs_scan(self):
        """Runs the file system scanning benchmarks: Pre-scan, Snapshot, Audit, Sentinel."""
        data_exists = os.path.exists(self.data_dir) and len(os.listdir(self.data_dir)) > 0
        if not data_exists: raise RuntimeError("Benchmark data missing")
        click.echo(f"Using data directory: {self.data_dir}")
        
        results = {}
        
        try:
            # --- Phase 1: Setup & Initial Sync (Pre-scan + Snapshot) ---
            click.echo("\n" + "="*80)
            click.echo("PHASE 1: PRE-SCAN & INITIAL SNAPSHOT SYNC")
            click.echo("="*80)
            
            self.services.setup_env()
            api_key = self.services.configure_system()
            self.services.start_fusion()
            
            # Start Agent once and keep it running
            start_time = time.time()
            self.services.start_agent(api_key, audit_interval=0, sentinel_interval=0)
            
            # 1. Measure Pre-scan Time
            prescan_match = self.services.wait_for_log(
                self.services.get_agent_log_path(), 
                r"Pre-scan completed: processed (\d+) entries", 
                timeout=300
            )
            prescan_time = time.time()
            prescan_duration = prescan_time - start_time
            if prescan_match:
                click.echo(f"  [Agent] Pre-scan completed in {prescan_duration:.2f}s (Processed {prescan_match.group(1)} entries).")
            else:
                click.echo("  [Agent] Pre-scan log not found (Timeout).")
                prescan_duration = None

            # 2. Measure Total Ingestion Time (Fusion Ready)
            self.wait_for_sync(api_key)
            ingestion_end_time = time.time()
            total_ingestion_duration = ingestion_end_time - start_time
            
            phase_duration = total_ingestion_duration - prescan_duration if prescan_duration else None
            
            click.echo(f"  [Fusion] Setup & Ingestion completed in {total_ingestion_duration:.2f}s.")
            if phase_duration:
                click.echo(f"  [Calculated] Snapshot Phase Duration (Net): {phase_duration:.2f}s")
                
            results["prescan"] = {"duration": prescan_duration}
            results["snapshot_phase"] = {"duration": phase_duration, "total_ingestion": total_ingestion_duration}
            
            # MUST ensure we are Leader before proceeding to Phase 2/3
            if not self.services.wait_for_leader():
                click.echo("  [Agent] Agent failed to become LEADER. Cannot proceed with Audit/Sentinel.")
                return results

            # --- Phase 2: Audit Performance ---
            click.echo("\n" + "="*80)
            click.echo("PHASE 2: AUDIT PERFORMANCE")
            click.echo("="*80)
            
            # Capture log offset before triggering
            log_offset = self.services.get_log_size(self.services.get_agent_log_path())
            click.echo("Triggering Audit cycle manually...")
            # Trigger via API (no restart)
            self.services.trigger_agent_audit()
            
            # Wait for Audit Start
            audit_start_match = self.services.wait_for_log(
                self.services.get_agent_log_path(), 
                r"Audit phase started", 
                start_offset=log_offset, timeout=60
            )
            audit_start_time = time.time()
            
            if audit_start_match:
                click.echo("  [Agent] Audit started.")
                # Wait for Audit End
                audit_end_match = self.services.wait_for_log(
                    self.services.get_agent_log_path(), 
                    r"Audit phase completed", 
                    start_offset=log_offset, timeout=120
                )
                audit_end_time = time.time()
                
                if audit_end_match:
                    audit_duration = audit_end_time - audit_start_time
                    click.echo(f"  [Agent] Audit cycle completed in ~{audit_duration:.2f}s.")
                    results["audit"] = {"duration": audit_duration}
                else:
                    click.echo("  [Agent] Audit completion log not found (Timeout).")
                    results["audit"] = {"status": "completion_timeout"}
            else:
                 click.echo("  [Agent] Audit start log not found (Timeout).")
                 results["audit"] = {"status": "start_timeout"}

            # --- Phase 3: Sentinel Performance ---
            click.echo("\n" + "="*80)
            click.echo("PHASE 3: SENTINEL PERFORMANCE")
            click.echo("="*80)
            
            # Capture log offset before triggering
            log_offset = self.services.get_log_size(self.services.get_agent_log_path())
            click.echo("Triggering Sentinel cycle manually...")
            # Trigger via API (no restart)
            self.services.trigger_agent_sentinel()
            
            # Wait for Sentinel Check Completed
            sentinel_end_match = self.services.wait_for_log(
                self.services.get_agent_log_path(),
                r"Sentinel check completed",
                start_offset=log_offset, timeout=120
            )
            
            if sentinel_end_match:
                 click.echo(f"  [Agent] Sentinel check completed.")
                 results["sentinel"] = {"status": "completed"}
            else:
                 click.echo("  [Agent] Sentinel check log not found (Maybe no tasks?).")
                 results["sentinel"] = {"status": "skipped_or_timeout"}

            # Save Results
            os.makedirs(os.path.join(self.run_dir, "results"), exist_ok=True)
            with open(os.path.join(self.run_dir, "results/fs_scan.json"), "w") as f:
                 json.dump(results, f, indent=2)
            
            # Get total entries for the report
            fusion_url = self.external_api_url or f"http://localhost:{self.services.fusion_port}"
            res_stats = requests.get(f"{fusion_url}/api/v1/views/bench-view/stats", headers={"X-API-Key": api_key})
            stats_data = res_stats.json() if res_stats.status_code == 200 else {}
            total_entries = stats_data.get("total_files", 0) + stats_data.get("total_directories", 0)

            os.makedirs(os.path.join(self.run_dir, "results"), exist_ok=True)
            report_path = os.path.join(self.run_dir, "results/fs_scan.html")
            generate_fs_scan_report(results, report_path, total_entries=total_entries)

            click.echo("\n" + "="*80)
            click.echo("FS-SCAN BENCHMARK RESULTS")
            click.echo("="*80)
            click.echo(json.dumps(results, indent=2))
            click.echo(click.style(f"\nVisual HTML report saved to: {report_path}", fg="green", bold=True))
            
        finally:
            self.services.stop_all()
