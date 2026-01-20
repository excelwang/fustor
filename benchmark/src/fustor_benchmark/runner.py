import time
import subprocess
import requests
import click
import statistics
import random
import os
import json
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor, as_completed
from .generator import DataGenerator
from .services import ServiceManager

def run_find_recursive_metadata_task(args):
    """
    Simulates a recursive metadata retrieval: `find <dir> -type f -ls`
    This matches the recursive nature of Fusion's tree API for a subdirectory.
    """
    data_dir, subdir = args
    target = os.path.join(data_dir, subdir.lstrip('/'))
    
    # Recursive search for all descendant files with metadata
    cmd = ["find", target, "-type", "f", "-ls"]
        
    start = time.time()
    subprocess.run(cmd, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    return time.time() - start

class BenchmarkRunner:
    def __init__(self, run_dir):
        self.run_dir = os.path.abspath(run_dir) # Ensure absolute path
        # Derived directories
        self.data_dir = os.path.join(self.run_dir, "data")
        self.env_dir = os.path.join(self.run_dir, ".fustor")
        
        # Pass the common root run_dir to ServiceManager
        self.services = ServiceManager(self.run_dir) 
        self.generator = DataGenerator(self.data_dir)

    def _calculate_stats(self, latencies, total_time, count):
        """Calculate rich statistics from a list of latencies (in seconds)."""
        if not latencies:
            return {
                "qps": 0, "avg": 0, "min": 0, "max": 0, "stddev": 0,
                "p50": 0, "p95": 0, "p99": 0
            }
        
        # Convert to milliseconds for presentation
        l_ms = sorted([l * 1000 for l in latencies])
        qps = count / total_time
        
        # Calculate percentiles
        qs = statistics.quantiles(l_ms, n=100) if len(l_ms) >= 2 else [l_ms[0]] * 100
        
        return {
            "qps": qps,
            "avg": statistics.mean(l_ms),
            "min": min(l_ms),
            "max": max(l_ms),
            "stddev": statistics.stdev(l_ms) if len(l_ms) >= 2 else 0,
            "p50": statistics.median(l_ms),
            "p95": qs[94],
            "p99": qs[98],
            "raw": l_ms # Keep raw data for charting
        }

    def _discover_leaf_targets_via_api(self, api_key: str, depth: int):
        """Finds directories at the specified depth using a single Fusion API call with max_depth."""
        click.echo(f"Discovering target directories at depth {depth} via Fusion API (optimized single-call)...")
        
        fusion_url = f"http://localhost:{self.services.fusion_port}"
        headers = {"X-API-Key": api_key}
        
        try:
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

            def walk(node, current_depth):
                if current_depth == depth:
                    if node.get('content_type') == 'directory':
                        targets.append(node.get('path'))
                    return
                children = node.get('children', {})
                if isinstance(children, dict):
                    for child_node in children.values(): walk(child_node, current_depth + 1)
                elif isinstance(children, list):
                    for child_node in children: walk(child_node, current_depth + 1)

            walk(tree_data, 0)
        except Exception as e:
            click.echo(click.style(f"Discovery error: {e}. Falling back to root.", fg="yellow"))
            return ["/"]

        if not targets:
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
            futures = [executor.submit(run_find_recursive_metadata_task, t) for t in tasks] # noqa
            for f in as_completed(futures): latencies.append(f.result())
        total_time = time.time() - start_total
        return self._calculate_stats(latencies, total_time, requests_count)

    def _run_single_fusion_req(self, url, headers, path):
        start = time.time()
        try:
            res = requests.get(f"{url}/views/fs/tree", params={"path": path}, headers=headers, timeout=10)
            if res.status_code != 200: return None
        except Exception: return None
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
                if res is not None: latencies.append(res)
        total_time = time.time() - start_total
        return self._calculate_stats(latencies, total_time, requests_count)

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
                if not is_ok: raise RuntimeError(f"Agent reported error during sync: {log_msg}")
                if int(elapsed) % 30 < 5: click.echo(f"  [Agent] Status: {log_msg}")
            try:
                res = requests.get(f"{fusion_url}/views/fs/tree", params={"path": "/"}, headers=headers, timeout=5)
                if res.status_code == 200:
                    click.echo(f"  [Fusion] READY (200 OK) after {elapsed:.1f}s.")
                    break
                elif res.status_code == 503:
                    if int(elapsed) % 5 == 0:
                        click.echo(f"  [Fusion] Still syncing... (Elapsed: {int(elapsed)}s)")
                else: raise RuntimeError(f"  [Fusion] Unexpected API response: {res.status_code}")
            except requests.ConnectionError: pass
            except Exception as e: click.echo(f"  [Fusion] Warning: Connection glitch ({e})")
            loop_count += 1
            time.sleep(5)
        click.echo("Sync complete. Proceeding to benchmark.")

    def generate_html_report(self, results, output_path):
        """Generates a rich HTML report with charts using Chart.js."""
        template = """
<!DOCTYPE html>
<html>
<head>
    <meta charset="utf-8">
    <title>Fustor Benchmark Report</title>
    <script src="https://cdn.jsdelivr.net/npm/chart.js"></script>
    <style>
        body { font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, sans-serif; margin: 40px; background: #f4f7f6; color: #333; }
        .container { max-width: 1100px; margin: auto; background: white; padding: 40px; border-radius: 12px; box-shadow: 0 4px 20px rgba(0,0,0,0.08); }
        h1 { color: #2c3e50; border-bottom: 2px solid #eee; padding-bottom: 15px; margin-top: 0; }
        .summary { display: flex; justify-content: space-between; margin: 30px -10px; }
        .stat-card { background: #fff; padding: 20px; border-radius: 10px; flex: 1; margin: 0 10px; text-align: center; border: 1px solid #eee; border-top: 5px solid #3498db; transition: transform 0.2s; }
        .stat-card:hover { transform: translateY(-5px); }
        .stat-card.fusion { border-top-color: #2ecc71; }
        .stat-card.gain { border-top-color: #f1c40f; }
        .stat-value { font-size: 28px; font-weight: bold; margin: 10px 0; color: #2c3e50; }
        .stat-label { font-size: 14px; color: #7f8c8d; text-transform: uppercase; letter-spacing: 1px; }
        .chart-row { display: flex; gap: 20px; margin-bottom: 40px; }
        .chart-box { flex: 1; background: #fff; padding: 20px; border-radius: 10px; border: 1px solid #eee; }
        .chart-container { position: relative; height: 350px; width: 100%; }
        table { width: 100%; border-collapse: collapse; margin-top: 20px; font-size: 15px; }
        th, td { text-align: left; padding: 15px; border-bottom: 1px solid #eee; }
        th { background: #f8f9fa; color: #2c3e50; font-weight: 600; }
        .winning { color: #2ecc71; font-weight: bold; }
        .info-bar { background: #e8f4fd; padding: 15px; border-radius: 8px; margin-bottom: 30px; display: flex; gap: 20px; font-size: 14px; }
    </style>
</head>
<body>
    <div class="container">
        <h1>Fustor Performance Benchmark</h1>
        
        <div class="info-bar">
            <span>📅 Time: <strong>{{timestamp}}</strong></span>
            <span>🚀 Requests: <strong>{{reqs}}</strong></span>
            <span>👥 Concurrency: <strong>{{concurrency}}</strong></span>
        </div>
        
        <div class="summary">
            <div class="stat-card">
                <div class="stat-label">OS Baseline (Avg)</div>
                <div class="stat-value">{{os_avg}} ms</div>
            </div>
            <div class="stat-card fusion">
                <div class="stat-label">Fusion API (Avg)</div>
                <div class="stat-value">{{fusion_avg}} ms</div>
            </div>
            <div class="stat-card gain">
                <div class="stat-label">Speedup Factor</div>
                <div class="stat-value" style="color: #2ecc71">{{gain}}x</div>
            </div>
        </div>

        <div class="chart-row">
            <div class="chart-box">
                <h3>Latency Distribution (Bar)</h3>
                <div class="chart-container">
                    <canvas id="barChart"></canvas>
                </div>
            </div>
            <div class="chart-box">
                <h3>Latency Percentiles (Line)</h3>
                <div class="chart-container">
                    <canvas id="lineChart"></canvas>
                </div>
            </div>
        </div>

        <h2>Detailed Metrics Comparison</h2>
        <table>
            <thead>
                <tr>
                    <th>Metric (ms)</th>
                    <th>OS (find -ls)</th>
                    <th>Fusion API</th>
                    <th>Improvement</th>
                </tr>
            </thead>
            <tbody>
                <tr><td>Average Latency</td><td>{{os_avg}}</td><td>{{fusion_avg}}</td><td class="winning">{{gain_avg}}x faster</td></tr>
                <tr><td>Median (P50)</td><td>{{os_p50}}</td><td>{{fusion_p50}}</td><td class="winning">{{gain_p50}}x faster</td></tr>
                <tr><td>P95 Latency</td><td>{{os_p95}}</td><td>{{fusion_p95}}</td><td class="winning">{{gain_p95}}x faster</td></tr>
                <tr><td>P99 Latency</td><td>{{os_p99}}</td><td>{{fusion_p99}}</td><td class="winning">{{gain_p99}}x faster</td></tr>
                <tr><td>Throughput (QPS)</td><td>{{os_qps}}</td><td>{{fusion_qps}}</td><td class="winning">{{gain_qps}}x higher</td></tr>
                <tr><td>Min / Max</td><td>{{os_min}} / {{os_max}}</td><td>{{fusion_min}} / {{fusion_max}}</td><td>-</td></tr>
            </tbody>
        </table>
    </div>

    <script>
        // Bar Chart
        new Chart(document.getElementById('barChart'), {
            type: 'bar',
            data: {
                labels: ['Average', 'Median', 'P95', 'P99'],
                datasets: [
                    { label: 'OS Baseline', data: [{{os_avg}}, {{os_p50}}, {{os_p95}}, {{os_p99}}], backgroundColor: '#3498db' },
                    { label: 'Fusion API', data: [{{fusion_avg}}, {{fusion_p50}}, {{fusion_p95}}, {{fusion_p99}}], backgroundColor: '#2ecc71' }
                ]
            },
            options: { responsive: true, maintainAspectRatio: false, scales: { y: { beginAtZero: true } } }
        });

        // Percentile Line Chart
        new Chart(document.getElementById('lineChart'), {
            type: 'line',
            data: {
                labels: ['Min', 'P50', 'P75', 'P90', 'P95', 'P99', 'Max'],
                datasets: [
                    { label: 'OS Baseline', data: [{{os_min}}, {{os_p50}}, {{os_p75}}, {{os_p90}}, {{os_p95}}, {{os_p99}}, {{os_max}}], borderColor: '#3498db', fill: false, tension: 0.1 },
                    { label: 'Fusion API', data: [{{fusion_min}}, {{fusion_p50}}, {{fusion_p75}}, {{fusion_p90}}, {{fusion_p95}}, {{fusion_p99}}, {{fusion_max}}], borderColor: '#2ecc71', fill: false, tension: 0.1 }
                ]
            },
            options: { responsive: true, maintainAspectRatio: false, scales: { y: { type: 'logarithmic', title: { display: true, text: 'Latency (ms) - Log Scale' } } } }
        });
    </script>
</body>
</html>
"""
        # Calculate speedups
        g_avg = results['os']['avg'] / results['fusion']['avg'] if results['fusion']['avg'] > 0 else 0
        
        # Helper for percentiles in line chart
        def get_p(stats, p):
            raw = stats['raw']
            idx = int(len(raw) * p / 100)
            return raw[min(idx, len(raw)-1)]

        html = template.replace("{{timestamp}}", results['timestamp']) \
                       .replace("{{depth}}", str(results['depth'])) \
                       .replace("{{reqs}}", str(results['requests'])) \
                       .replace("{{concurrency}}", str(results['concurrency'])) \
                       .replace("{{os_avg}}", f"{results['os']['avg']:.2f}") \
                       .replace("{{fusion_avg}}", f"{results['fusion']['avg']:.2f}") \
                       .replace("{{os_p50}}", f"{results['os']['p50']:.2f}") \
                       .replace("{{fusion_p50}}", f"{results['fusion']['p50']:.2f}") \
                       .replace("{{os_p95}}", f"{results['os']['p95']:.2f}") \
                       .replace("{{fusion_p95}}", f"{results['fusion']['p95']:.2f}") \
                       .replace("{{os_p99}}", f"{results['os']['p99']:.2f}") \
                       .replace("{{fusion_p99}}", f"{results['fusion']['p99']:.2f}") \
                       .replace("{{os_qps}}", f"{results['os']['qps']:.1f}") \
                       .replace("{{fusion_qps}}", f"{results['fusion']['qps']:.1f}") \
                       .replace("{{os_min}}", f"{results['os']['min']:.2f}") \
                       .replace("{{os_max}}", f"{results['os']['max']:.2f}") \
                       .replace("{{fusion_min}}", f"{results['fusion']['min']:.2f}") \
                       .replace("{{fusion_max}}", f"{results['fusion']['max']:.2f}") \
                       .replace("{{os_p75}}", f"{get_p(results['os'], 75):.2f}") \
                       .replace("{{os_p90}}", f"{get_p(results['os'], 90):.2f}") \
                       .replace("{{fusion_p75}}", f"{get_p(results['fusion'], 75):.2f}") \
                       .replace("{{fusion_p90}}", f"{get_p(results['fusion'], 90):.2f}") \
                       .replace("{{gain}}", f"{g_avg:.1f}") \
                       .replace("{{gain_avg}}", f"{g_avg:.1f}") \
                       .replace("{{gain_p50}}", f"{results['os']['p50']/results['fusion']['p50']:.1f}" if results['fusion']['p50'] > 0 else "N/A") \
                       .replace("{{gain_p95}}", f"{results['os']['p95']/results['fusion']['p95']:.1f}" if results['fusion']['p95'] > 0 else "N/A") \
                       .replace("{{gain_p99}}", f"{results['os']['p99']/results['fusion']['p99']:.1f}" if results['fusion']['p99'] > 0 else "N/A") \
                       .replace("{{gain_qps}}", f"{results['fusion']['qps']/results['os']['qps']:.1f}" if results['os']['qps'] > 0 else "N/A")

        with open(output_path, "w") as f: f.write(html)

    def run(self, concurrency=20, reqs=200, target_depth=5, force_gen=False, custom_target=False):
        if not custom_target:
            if os.path.exists(self.data_dir) and not force_gen: click.echo(f"Data directory '{self.data_dir}' exists. Skipping generation.")
            else: self.generator.generate()
        else: click.echo(f"Benchmarking target directory: {self.data_dir}")
        
        try:
            self.services.setup_env()
            self.services.start_registry(); api_key = self.services.configure_system()
            self.services.start_fusion(); self.services.start_agent(api_key)
            time.sleep(2)
            is_ok, msg = self.services.check_agent_logs()
            if not is_ok: raise RuntimeError(f"Agent failed to initialize correctly: {msg}")
            click.echo("Agent health check passed.")
            
            self.wait_for_sync(api_key)
            targets = self._discover_leaf_targets_via_api(api_key, target_depth)
            
            # Run benchmarks
            os_stats = self.run_concurrent_baseline(targets, concurrency, reqs)
            fusion_stats = self.run_concurrent_fusion(api_key, targets, concurrency, reqs)
            
            # Prepare results object
            final_results = {
                "depth": target_depth, "requests": reqs, "concurrency": concurrency,
                "os": os_stats, "fusion": fusion_stats,
                "timestamp": time.strftime("%Y-%m-%d %H:%M:%S")
            }

            # Save JSON and HTML
            json_path = os.path.join(self.env_dir, "benchmark_results.json")
            html_path = os.path.join(self.env_dir, "report.html")
            with open(json_path, "w") as f: json.dump(final_results, f, indent=2)
            self.generate_html_report(final_results, html_path)

            # Output Scorecard to console
            click.echo("\n" + "="*60)
            click.echo(f"RECURSIVE METADATA RETRIEVAL PERFORMANCE (DEPTH {target_depth})")
            click.echo("="*60)
            click.echo(f"{ 'Metric (ms)':<25} | {'OS (find -ls)':<18} | {'Fusion API':<18}")
            click.echo("-" * 65)
            click.echo(f"{ 'Avg Latency':<25} | {os_stats['avg']:10.2f} ms      | {fusion_stats['avg']:10.2f} ms")
            click.echo(f"{ 'P50 Latency':<25} | {os_stats['p50']:10.2f} ms      | {fusion_stats['p50']:10.2f} ms")
            click.echo(f"{ 'P99 Latency':<25} | {os_stats['p99']:10.2f} ms      | {fusion_stats['p99']:10.2f} ms")
            click.echo(f"{ 'Throughput (QPS)':<25} | {os_stats['qps']:10.1f}         | {fusion_stats['qps']:10.1f}")
            click.echo("-" * 65)
            click.echo(click.style(f"\nJSON results saved to: {json_path}", fg="cyan"))
            click.echo(click.style(f"Visual HTML report saved to: {html_path}", fg="green", bold=True))
            
        finally:
            self.services.stop_all()