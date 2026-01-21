import statistics
import os
import json
from pathlib import Path

def calculate_stats(latencies, total_time, count):
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

def generate_html_report(results, output_path):
    """Generates a rich HTML report by loading an external template."""
    template_path = Path(__file__).parent / "report_template.html"
    try:
        with open(template_path, "r", encoding="utf-8") as f:
            template = f.read()
    except Exception as e:
        print(f"Error loading HTML template: {e}")
        return

    g_avg = results['os']['avg'] / results['fusion']['avg'] if results['fusion']['avg'] > 0 else 0
    
    # Calculate net overhead percentage: (dry / total) * 100
    net_overhead_avg = (results['fusion_dry']['avg'] / results['fusion']['avg'] * 100) if results['fusion']['avg'] > 0 else 0
    net_overhead_p50 = (results['fusion_dry']['p50'] / results['fusion']['p50'] * 100) if results['fusion']['p50'] > 0 else 0

    def get_p(stats, p):
        raw = stats['raw']; idx = int(len(raw) * p / 100)
        return raw[min(idx, len(raw)-1)]

    html = template.replace("{{timestamp}}", results['timestamp']) \
        .replace("{{op_type}}", results['metadata'].get('operation_type', 'RECURSIVE_METADATA_GET')) \
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

    with open(output_path, "w", encoding="utf-8") as f:
        f.write(html)