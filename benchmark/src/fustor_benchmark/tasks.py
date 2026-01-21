import os
import subprocess
import time
import requests

def run_find_recursive_metadata_task(args):
    """
    Simulates a recursive metadata retrieval and realistic parsing.
    """
    data_dir, subdir = args
    target = os.path.join(data_dir, subdir.lstrip('/'))
    
    cmd = ["find", target, "-printf", "%p|%y|%s|%T@|%C@\n"]
    start = time.time()
    result = subprocess.run(cmd, capture_output=True, text=True)
    
    lines = result.stdout.splitlines()
    parsed_data = []
    for line in lines:
        parts = line.split('|')
        if len(parts) == 5:
            node = {
                "name": os.path.basename(parts[0]),
                "path": parts[0],
                "content_type": "directory" if parts[1] == 'd' else "file",
                "size": int(parts[2]),
                "modified_time": float(parts[3]),
                "created_time": float(parts[4])
            }
            parsed_data.append(node)
    return time.time() - start

def run_single_fusion_req(url, headers, path, dry_run=False, dry_net=False):
    """
    Executes a single Fusion API request.
    """
    start = time.time()
    try:
        if dry_net:
            res = requests.get(f"{url}/", timeout=10)
        else:
            params = {"path": path}
            if dry_run:
                params["dry_run"] = "true"
            res = requests.get(f"{url}/views/fs/tree", params=params, headers=headers, timeout=10)
        if res.status_code != 200: return None
    except Exception: return None
    return time.time() - start

def run_find_sampling_phase(args):
    """
    OS Integrity - Phase 1: Initial Sampling.
    Returns (latency, metadata_dict)
    """
    data_dir, subdir = args
    target = os.path.join(data_dir, subdir.lstrip('/'))
    
    start = time.time()
    cmd = ["find", target, "-type", "f", "-printf", "%p\t%C@\t%s\n"]
    result = subprocess.run(cmd, capture_output=True, text=True)
    lines = result.stdout.splitlines()
    
    metadata = {}
    for line in lines:
        parts = line.split('\t')
        if len(parts) == 3:
            metadata[parts[0]] = (parts[1], parts[2]) # ctime, size
            
    return time.time() - start, metadata

def run_find_validation_phase(args):
    """
    OS Integrity - Phase 2: Secondary Validation.
    Returns latency
    """
    metadata, interval = args
    
    start = time.time()
    stable_count = 0
    now = time.time()
    for path, (old_ctime, old_size) in metadata.items():
        try:
            if now - float(old_ctime) < interval:
                continue
            st = os.stat(path)
            if str(st.st_size) == old_size and f"{st.st_ctime:.6f}" == old_ctime:
                stable_count += 1
        except (OSError, ValueError):
            pass
    return time.time() - start