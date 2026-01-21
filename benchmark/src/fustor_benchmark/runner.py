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
    Simulates a recursive metadata retrieval and realistic parsing.
    Includes type detection, path splitting, and object instantiation.
    """
    data_dir, subdir = args
    target = os.path.join(data_dir, subdir.lstrip('/'))
    
    # %p: path, %y: type, %s: size, %T@: mtime, %C@: ctime
    cmd = ["find", target, "-printf", "%p|%y|%s|%T@|%C@\n"]
        
    start = time.time()
    result = subprocess.run(cmd, capture_output=True, text=True)
    
    lines = result.stdout.splitlines()
    parsed_data = []
    for line in lines:
        parts = line.split('|')
        if len(parts) == 5:
            path = parts[0]
            name = os.path.basename(path)
            content_type = "directory" if parts[1] == 'd' else "file"
            
            node = {
                "name": name,
                "path": path,
                "content_type": content_type,
                "size": int(parts[2]),
                "modified_time": float(parts[3]),
                "created_time": float(parts[4])
            }
            parsed_data.append(node)
            
    return time.time() - start

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

    def _calculate_stats(self, latencies, total_time, count):
        """Calculate rich statistics from a list of latencies (in seconds)."""
        if not latencies:
            return {"qps": 0, "avg": 0, "min": 0, "max": 0, "stddev": 0, "p50": 0, "p95": 0, "p99": 0}
        
        l_ms = sorted([l * 1000 for l in latencies])
        qps = count / total_time
        qs = statistics.quantiles(l_ms, n=100) if len(l_ms) >= 2 else [l_ms[0]] * 100
        
        return {
            "qps": qps, "avg": statistics.mean(l_ms), "min": min(l_ms), "max": max(l_ms),
            "stddev": statistics.stdev(l_ms) if len(l_ms) >= 2 else 0,
            "p50": statistics.median(l_ms), "p95": qs[94], "p99": qs[98], "raw": l_ms
        }

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
        return self._calculate_stats(latencies, total_time, requests_count)

    def _run_single_fusion_req(self, url, headers, path, dry_run=False):
        start = time.time()
        params = {"path": path}
        if dry_run:
            params["dry_run"] = "true"
        try:
            res = requests.get(f"{url}/views/fs/tree", params=params, headers=headers, timeout=10)
            if res.status_code != 200: return None
        except Exception: return None
        return time.time() - start

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
            futures = [executor.submit(self._run_single_fusion_req, fusion_url, headers, t, False) for t in tasks]
            for f in as_completed(futures):
                res = f.result()
                if res is not None: latencies.append(res)
        total_time = time.time() - start_total
        return self._calculate_stats(latencies, total_time, requests_count)

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
            futures = [executor.submit(self._run_single_fusion_req, fusion_url, headers, t, True) for t in tasks]
            for f in as_completed(futures):
                res = f.result()
                if res is not None: latencies.append(res)
        total_time = time.time() - start_total
        return self._calculate_stats(latencies, total_time, requests_count)

    def wait_for_sync(self, api_key: str):
        click.echo("Waiting for Fusion readiness (Alternating between API status and Agent logs)...")
        fusion_url = self.external_api_url or f"http://localhost:{self.services.fusion_port}"
        headers = {"X-API-Key": api_key}
        start_wait = time.time()
        loop_count = 0
        while True:
            elapsed = time.time() - start_wait
            if not self.external_api_url and loop_count % 2 == 0:
                is_ok, log_msg = self.services.check_agent_logs()
                if not is_ok: raise RuntimeError(f"Agent reported error during sync: {log_msg}")
                if int(elapsed) % 30 < 5: click.echo(f"  [Agent] Status: {log_msg}")
            
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
        body { font-family: -apple-system, BlinkMacSystemFont, \"Segoe UI\", Roboto, sans-serif; margin: 40px; background: #f4f7f6; color: #333; }
        .container { max-width: 1200px; margin: auto; background: white; padding: 40px; border-radius: 12px; box-shadow: 0 4px 20px rgba(0,0,0,0.08); }
        h1 { color: #2c3e50; border-bottom: 2px solid #eee; padding-bottom: 15px; margin-top: 0; }
        .summary { display: flex; justify-content: space-between; margin: 30px -10px; }
        .stat-card { background: #fff; padding: 20px; border-radius: 10px; flex: 1; margin: 0 10px; text-align: center; border: 1px solid #eee; border-top: 5px solid #3498db; transition: transform 0.2s; }
        .stat-card:hover { transform: translateY(-5px); }
        .stat-card.fusion { border-top-color: #2ecc71; }
        .stat-card.dryrun { border-top-color: #95a5a6; }
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
        .info-bar { background: #e8f4fd; padding: 15px; border-radius: 8px; margin-bottom: 30px; display: grid; grid-template-columns: 1fr 1fr; gap: 15px; font-size: 14px; }
        .info-item { display: flex; gap: 10px; }
        .info-label { color: #5a6c7a; font-weight: 600; min-width: 120px; }
    </style>
</head>
<body>
    <div class="container">
        <h1>Fustor Performance Benchmark</h1>
        <div class="info-bar">
            <div class="info-item"><span class="info-label">📅 Timestamp:</span> <strong>{{timestamp}}</strong></div>
            <div class="info-item"><span class="info-label">⚙️ Op Type:</span> <strong>{{op_type}}</strong></div>
            <div class="info-item"><span class="info-label">📂 Data Scale:</span> <strong>{{total_files}} files ({{total_dirs}} dirs)</strong></div>
            <div class="info-item"><span class="info-label">📂 Target Depth:</span> <strong>{{depth}}</strong></div>
            <div class="info-item"><span class="info-label">🚀 Requests:</span> <strong>{{reqs}}</strong></div>
            <div class="info-item"><span class="info-label">👥 Concurrency:</span> <strong>{{concurrency}}</strong></div>
            <div class="info-item"><span class="info-label">🏷️ Target Folders:</span> <strong>{{target_count}}</strong></div>
        </div>
        <div class="summary">
            <div class="stat-card">
                <div class="stat-label">OS Baseline (Avg)</div>
                <div class="stat-value">{{os_avg}} ms</div>
            </div>
            <div class="stat-card dryrun">
                <div class="stat-label">Fusion Dry-Run (Avg)</div>
                <div class="stat-value">{{dry_avg}} ms</div>
            </div>
            <div class="stat-card fusion">
                <div class="stat-label">Fusion API (Avg)</div>
                <div class="stat-value">{{fusion_avg}} ms</div>
            </div>
            <div class="stat-card gain">
                <div class="stat-label">Speedup (vs OS)</div>
                <div class="stat-value" style="color: #2ecc71">{{gain}}x</div>
            </div>
        </div>
        <div class="chart-row">
            <div class="chart-box">
                <h3>Latency Distribution (Bar)</h3>
                <div class="chart-container"><canvas id="barChart"></canvas></div>
            </div>
            <div class="chart-box">
                <h3>Latency Percentiles (Line)</h3>
                <div class="chart-container"><canvas id="lineChart"></canvas></div>
            </div>
        </div>
        <h2>Detailed Metrics Comparison</h2>
        <table>
            <thead>
                <tr>
                    <th>Metric (ms)</th>
                    <th>OS (find + Parsing)</th>
                    <th>Fusion Dry-Run (Net only)</th>
                    <th>Fusion API (Structured)</th>
                    <th>Net Overhead %</th>
                </tr>
            </thead>
            <tbody>
                <tr>
                    <td>Average Latency</td>
                    <td>{{os_avg}}</td>
                    <td>{{dry_avg}}</td>
                    <td>{{fusion_avg}}</td>
                    <td>{{net_overhead_avg}}%</td>
                </tr>
                <tr>
                    <td>Median (P50)</td>
                    <td>{{os_p50}}</td>
                    <td>{{dry_p50}}</td>
                    <td>{{fusion_p50}}</td>
                    <td>{{net_overhead_p50}}%</td>
                </tr>
                <tr>
                    <td>P95 Latency</td>
                    <td>{{os_p95}}</td>
                    <td>{{dry_p95}}</td>
                    <td>{{fusion_p95}}</td>
                    <td>-</td>
                </tr>
                <tr>
                    <td>P99 Latency</td>
                    <td>{{os_p99}}</td>
                    <td>{{dry_p99}}</td>
                    <td>{{fusion_p99}}</td>
                    <td>-</td>
                </tr>
                <tr>
                    <td>Throughput (QPS)</td>
                    <td>{{os_qps}}</td>
                    <td>{{dry_qps}}</td>
                    <td>{{fusion_qps}}</td>
                    <td>-</td>
                </tr>
                <tr>
                    <td>Min / Max</td>
                    <td>{{os_min}} / {{os_max}}</td>
                    <td>{{dry_min}} / {{dry_max}}</td>
                    <td>{{fusion_min}} / {{fusion_max}}</td>
                    <td>-</td>
                </tr>
            </tbody>
        </table>

        <div class="methodology">
            <h2>Test Methodology & Documentation</h2>
            
            <section>
                <h3>1. 测试目标 (Objectives)</h3>
                <p>量化 Fustor Fusion 在百万级元数据规模下的检索性能，并诊断网络与应用层开销。本次测试引入了 Dry-Run 对照组，以分离网络传输延迟与实际数据处理延迟。</p>
            </section>

            <section>
                <h3>2. 对照组说明 (Test Groups)</h3>
                <ul>
                    <li><strong>OS Baseline</strong>: 模拟业务通过原生 <code>find</code> 命令获取数据并解析的完整成本。</li>
                    <li><strong>Fusion Dry-Run</strong>: 调用 API 但跳过数据检索逻辑，仅返回空响应。此指标代表了网络往返 (RTT)、HTTP 握手、FastAPI 框架路由、鉴权中间件以及并发连接池排队的<strong>纯系统开销</strong>。</li>
                    <li><strong>Fusion API</strong>: 完整的结构化元数据检索。<strong>实际数据处理耗时 = (Fusion API Latency) - (Fusion Dry-Run Latency)</strong>。</li>
                </ul>
            </section>
        </div>
    </div>
    <script>
        new Chart(document.getElementById('barChart'), {
            type: 'bar',
            data: {
                labels: ['Average', 'Median', 'P95', 'P99'],
                datasets: [
                    { label: 'OS Baseline', data: [{{os_avg}}, {{os_p50}}, {{os_p95}}, {{os_p99}}], backgroundColor: '#3498db' },
                    { label: 'Fusion Dry-Run', data: [{{dry_avg}}, {{dry_p50}}, {{dry_p95}}, {{dry_p99}}], backgroundColor: '#95a5a6' },
                    { label: 'Fusion API', data: [{{fusion_avg}}, {{fusion_p50}}, {{fusion_p95}}, {{fusion_p99}}], backgroundColor: '#2ecc71' }
                ]
            },
            options: { responsive: true, maintainAspectRatio: false, scales: { y: { beginAtZero: true } } }
        });
        new Chart(document.getElementById('lineChart'), {
            type: 'line',
            data: {
                labels: ['Min', 'P50', 'P75', 'P90', 'P95', 'P99', 'Max'],
                datasets: [
                    { label: 'OS Baseline', data: [{{os_min}}, {{os_p50}}, {{os_p75}}, {{os_p90}}, {{os_p95}}, {{os_p99}}, {{os_max}}], borderColor: '#3498db', fill: false, tension: 0.1 },
                    { label: 'Fusion Dry-Run', data: [{{dry_min}}, {{dry_p50}}, {{dry_p75}}, {{dry_p90}}, {{dry_p95}}, {{dry_p99}}, {{dry_max}}], borderColor: '#95a5a6', fill: false, tension: 0.1 },
                    { label: 'Fusion API', data: [{{fusion_min}}, {{fusion_p50}}, {{fusion_p75}}, {{fusion_p90}}, {{fusion_p95}}, {{fusion_p99}}, {{fusion_max}}], borderColor: '#2ecc71', fill: false, tension: 0.1 }
                ]
            },
            options: { responsive: true, maintainAspectRatio: false, scales: { y: { type: 'logarithmic', title: { display: true, text: 'Latency (ms) - Log Scale' } } } }
        });
    </script>
</body>
</html>
""";
        g_avg = results['os']['avg'] / results['fusion']['avg'] if results['fusion']['avg'] > 0 else 0
        
        # Calculate net overhead percentage: (dry / total) * 100
        net_overhead_avg = (results['fusion_dry']['avg'] / results['fusion']['avg'] * 100) if results['fusion']['avg'] > 0 else 0
        net_overhead_p50 = (results['fusion_dry']['p50'] / results['fusion']['p50'] * 100) if results['fusion']['p50'] > 0 else 0

        def get_p(stats, p):
            raw = stats['raw']; idx = int(len(raw) * p / 100) # Corrected index calculation
            return raw[min(idx, len(raw)-1)]

        html = template.replace("{{timestamp}}", results['timestamp']) \
            .replace("{{op_type}}", results['metadata']['operation_type']) \
            .replace("{{total_files}}", f"{results['metadata']['total_files_in_scope']:,}") \
            .replace("{{total_dirs}}", f"{results['metadata'].get('total_directories_in_scope', 0):,}") \
            .replace("{{target_count}}", str(results['target_directory_count'])) \
            .replace("{{depth}}", str(results['depth'])) \
            .replace("{{reqs}}", str(results['requests'])) \
            .replace("{{concurrency}}", str(results['concurrency'])) \
            .replace("{{os_avg}}", f"{results['os']['avg']:.2f}") \
            .replace("{{dry_avg}}", f"{results['fusion_dry']['avg']:.2f}") \
            .replace("{{fusion_avg}}", f"{results['fusion']['avg']:.2f}") \
            .replace("{{os_p50}}", f"{results['os']['p50']:.2f}") \
            .replace("{{dry_p50}}", f"{results['fusion_dry']['p50']:.2f}") \
            .replace("{{fusion_p50}}", f"{results['fusion']['p50']:.2f}") \
            .replace("{{os_p95}}", f"{results['os']['p95']:.2f}") \
            .replace("{{dry_p95}}", f"{results['fusion_dry']['p95']:.2f}") \
            .replace("{{fusion_p95}}", f"{results['fusion']['p95']:.2f}") \
            .replace("{{os_p99}}", f"{results['os']['p99']:.2f}") \
            .replace("{{dry_p99}}", f"{results['fusion_dry']['p99']:.2f}") \
            .replace("{{fusion_p99}}", f"{results['fusion']['p99']:.2f}") \
            .replace("{{os_qps}}", f"{results['os']['qps']:.1f}") \
            .replace("{{dry_qps}}", f"{results['fusion_dry']['qps']:.1f}") \
            .replace("{{fusion_qps}}", f"{results['fusion']['qps']:.1f}") \
            .replace("{{os_min}}", f"{results['os']['min']:.2f}") \
            .replace("{{os_max}}", f"{results['os']['max']:.2f}") \
            .replace("{{dry_min}}", f"{results['fusion_dry']['min']:.2f}") \
            .replace("{{dry_max}}", f"{results['fusion_dry']['max']:.2f}") \
            .replace("{{fusion_min}}", f"{results['fusion']['min']:.2f}") \
            .replace("{{fusion_max}}", f"{results['fusion']['max']:.2f}") \
            .replace("{{os_p75}}", f"{get_p(results['os'], 75):.2f}") \
            .replace("{{os_p90}}", f"{get_p(results['os'], 90):.2f}") \
            .replace("{{dry_p75}}", f"{get_p(results['fusion_dry'], 75):.2f}") \
            .replace("{{dry_p90}}", f"{get_p(results['fusion_dry'], 90):.2f}") \
            .replace("{{fusion_p75}}", f"{get_p(results['fusion'], 75):.2f}") \
            .replace("{{fusion_p90}}", f"{get_p(results['fusion'], 90):.2f}") \
            .replace("{{gain}}", f"{g_avg:.1f}") \
            .replace("{{net_overhead_avg}}", f"{net_overhead_avg:.1f}") \
            .replace("{{net_overhead_p50}}", f"{net_overhead_p50:.1f}")

        with open(output_path, "w") as f: f.write(html)

    def run(self, concurrency=20, reqs=200, target_depth=5):
        data_exists = os.path.exists(self.data_dir) and len(os.listdir(self.data_dir)) > 0 if os.path.exists(self.data_dir) else False
        if not data_exists:
            click.echo(click.style(f"FATAL: Data directory '{self.data_dir}' is empty or missing.", fg="red", bold=True))
            click.echo(click.style("Please run 'generate' first.", fg="yellow"))
            raise RuntimeError("Benchmark data missing")

        click.echo(f"Using data directory: {self.data_dir}")
        try:
            if self.external_api_url:
                if not self.external_api_key: raise RuntimeError("--api-key is required when using --fusion-api")
                click.echo(click.style(f"Using external Fusion API: {self.external_api_url}", fg="cyan", bold=True))
                api_key = self.external_api_key
            else:
                self.services.setup_env()
                self.services.start_registry()
                api_key = self.services.configure_system()
                self.services.start_fusion()
                self.services.start_agent(api_key)
                time.sleep(2)
                is_ok, msg = self.services.check_agent_logs()
                if not is_ok: raise RuntimeError(f"Agent failed: {msg}")
                click.echo("Agent health check passed.")
            
            self.wait_for_sync(api_key)
            targets = self._discover_leaf_targets_via_api(api_key, target_depth)
            
            os_stats = self.run_concurrent_baseline(targets, concurrency, reqs)
            
            # New Step: Run Dry-Run
            fusion_dry_stats = self.run_concurrent_fusion_dry_run(api_key, targets, concurrency, reqs)
            
            fusion_stats = self.run_concurrent_fusion(api_key, targets, concurrency, reqs)
            
            fusion_url = self.external_api_url or f"http://localhost:{self.services.fusion_port}"
            res_stats = requests.get(f"{fusion_url}/views/fs/stats", headers={"X-API-Key": api_key})
            stats_data = res_stats.json() if res_stats.status_code == 200 else {}
            total_files = stats_data.get("total_files", 0)
            total_dirs = stats_data.get("total_directories", 0)
            click.echo(f"Final Stats: {total_files:,} files, {total_dirs:,} directories.")
            
            final_results = {
                "metadata": {"operation_type": "RECURSIVE_METADATA_GET", "total_files_in_scope": total_files, "total_directories_in_scope": total_dirs, "source_path": self.data_dir, "api_endpoint": fusion_url},
                "depth": target_depth, "requests": reqs, "concurrency": concurrency, "target_directory_count": len(targets),
                "os": os_stats, "fusion_dry": fusion_dry_stats, "fusion": fusion_stats, "timestamp": time.strftime("%Y-%m-%d %H:%M:%S")
            }

            results_dir = os.path.join(self.run_dir, "results")
            os.makedirs(results_dir, exist_ok=True)
            json_path = os.path.join(results_dir, "stress-find.json")
            html_path = os.path.join(results_dir, "stress-find.html")
            with open(json_path, "w") as f: json.dump(final_results, f, indent=2)
            self.generate_html_report(final_results, html_path)

            click.echo("\n" + "="*85)
            click.echo(f"RECURSIVE METADATA RETRIEVAL PERFORMANCE (DEPTH {target_depth})")
            click.echo(f"Data Scale: {total_files:,} files | Targets: {len(targets)}")
            click.echo("="*85)
            click.echo(f"{'Metric (ms)':<25} | {'OS (find + Parsing)':<20} | {'Fusion Dry-Run':<20} | {'Fusion API':<20}")
            click.echo("-" * 85)
            click.echo(f"{ 'Avg Latency':<25} | {os_stats['avg']:10.2f} ms        | {fusion_dry_stats['avg']:10.2f} ms        | {fusion_stats['avg']:10.2f} ms")
            click.echo(f"{ 'P50 Latency':<25} | {os_stats['p50']:10.2f} ms        | {fusion_dry_stats['p50']:10.2f} ms        | {fusion_stats['p50']:10.2f} ms")
            click.echo(f"{ 'P99 Latency':<25} | {os_stats['p99']:10.2f} ms        | {fusion_dry_stats['p99']:10.2f} ms        | {fusion_stats['p99']:10.2f} ms")
            click.echo(f"{ 'Throughput (QPS)':<25} | {os_stats['qps']:10.1f}           | {fusion_dry_stats['qps']:10.1f}           | {fusion_stats['qps']:10.1f}")
            click.echo("-" * 85)
            click.echo(click.style(f"\nJSON results saved to: {json_path}", fg="cyan"))
            click.echo(click.style(f"Visual HTML report saved to: {html_path}", fg="green", bold=True))
        finally:
            if not self.external_api_url: self.services.stop_all()
