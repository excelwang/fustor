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
        "qps": qps, 
        "avg": statistics.mean(l_ms), 
        "min": min(l_ms), 
        "max": max(l_ms),
        "stddev": statistics.stdev(l_ms) if len(l_ms) >= 2 else 0,
        "p50": statistics.median(l_ms), 
        "p75": qs[74],
        "p90": qs[89],
        "p95": qs[94], 
        "p99": qs[98], 
        "raw": l_ms
    }

def generate_html_report(results, output_path):
    """Generates a rich HTML report by injecting a JSON data object."""
    template_path = Path(__file__).parent / "query_template.html"
    try:
        with open(template_path, "r", encoding="utf-8") as f:
            template = f.read()
    except Exception as e:
        print(f"Error loading HTML template: {e}")
        return

    # Calculate gains against OS Integrity
    gain_latency = results['os_integrity']['avg'] / results['fusion']['avg'] if results['fusion']['avg'] > 0 else 0
    gain_qps = results['fusion']['qps'] / results['os_integrity']['qps'] if results['os_integrity']['qps'] > 0 else 0
    
    summary = {
        "timestamp": results['timestamp'],
        "total_files": f"{results['metadata']['total_files_in_scope']:,}",
        "total_dirs": f"{results['metadata'].get('total_directories_in_scope', 0):,}",
        "depth": str(results['depth']),
        "reqs": str(results['requests']),
        "concurrency": str(results['concurrency']),
        "integrity_interval": str(results['metadata'].get('integrity_interval', 60.0)),
        
        "os_avg": f"{results['os']['avg']:.2f}",
        "os_integrity_avg": f"{results['os_integrity']['avg']:.2f}",
        "fusion_avg": f"{results['fusion']['avg']:.2f}",
        "gain_latency": f"{gain_latency:.1f}x",
        
        "os_qps": f"{results['os']['qps']:.1f}",
        "os_integrity_qps": f"{results['os_integrity']['qps']:.1f}",
        "fusion_qps": f"{results['fusion']['qps']:.1f}",
        "gain_qps": f"{gain_qps:.1f}x"
    }

    html = template
    for key, val in summary.items():
        html = html.replace(f"{{{{{key}}}}}", val)

    results_json = json.dumps(results)
    html = html.replace("/* RESULTS_JSON_DATA */", results_json)

    with open(output_path, "w", encoding="utf-8") as f:
        f.write(html)

def generate_fs_scan_report(results, output_path, total_entries=0):
    """Generates a file system scanning performance report."""
    template_path = Path(__file__).parent / "fs_scan_report.html"
    try:
        with open(template_path, "r", encoding="utf-8") as f:
            template = f.read()
    except Exception as e:
        print(f"Error loading Lifecycle template: {e}")
        return

    prescan_data = results.get('prescan', {})
    prescan = prescan_data.get('duration', 0) if prescan_data else 0
    
    snapshot_data = results.get('snapshot_sync', {})
    snapshot = snapshot_data.get('duration', 0) if snapshot_data else 0
    ingestion = snapshot_data.get('total_ingestion', 0) if snapshot_data else 0
    
    audit_data = results.get('audit', {})
    audit = audit_data.get('duration', 0) if isinstance(audit_data, dict) and 'duration' in audit_data else 0
    
    total_duration = (ingestion or 0) + (audit or 0)
    avg_speed = total_entries / total_duration if total_duration > 0 else 0

    summary = {
        "total_entries": f"{total_entries:,}",
        "total_duration": f"{total_duration:.2f}",
        "avg_speed": f"{avg_speed:.1f}",
        "prescan_duration": f"{prescan:.2f}" if prescan else "0.00",
        "snapshot_duration": f"{snapshot:.2f}" if snapshot else "0.00",
        "ingestion_total": f"{ingestion:.2f}" if ingestion else "0.00",
        "audit_duration": f"{audit:.2f}" if audit else "0.00",
    }

    html = template
    for key, val in summary.items():
        html = html.replace(f"{{{{{key}}}}}", val)

    with open(output_path, "w", encoding="utf-8") as f:
        f.write(html)