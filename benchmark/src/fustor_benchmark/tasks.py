import os
import subprocess
import time
import requests

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

def run_single_fusion_req(url, headers, path, dry_run=False, dry_net=False):
    """
    Executes a single Fusion API request and returns the latency.
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
        
        if res.status_code != 200:
            return None
    except Exception:
        return None
    return time.time() - start

def run_find_integrity_task(args):
    """
    Simulates the 'Double Sampling' method for NFS environments.
    Criteria for 'Complete':
    1. Size and ctime remain unchanged between two scans.
    2. ctime must be older than the 'interval' (default 60s) from now.
    """
    data_dir, subdir, interval = args
    target = os.path.join(data_dir, subdir.lstrip('/'))
    
    start = time.time()
    
    # Phase 1: First scan (Sampling ctime and size)
    # Using %p (path), %C@ (ctime), %s (size)
    cmd = ["find", target, "-type", "f", "-printf", "%p\t%C@\t%s\n"]
    result = subprocess.run(cmd, capture_output=True, text=True)
    lines = result.stdout.splitlines()
    
    initial_metadata = {}
    for line in lines:
        parts = line.split('\t')
        if len(parts) == 3:
            initial_metadata[parts[0]] = (parts[1], parts[2])
            
    # Phase 2: Silence Window / Sync Wait
    # This is the interval required for NFS consistency and stability check
    time.sleep(interval)
    
    # Phase 3: Secondary Validation
    # In real logic, we would run find again or stat each. 
    # To simulate the O(N) cost of re-verifying N files:
    stable_count = 0
    now = time.time()
    for path, (old_ctime, old_size) in initial_metadata.items():
        try:
            # Check age > interval (e.g. 60s)
            if now - float(old_ctime) < interval:
                continue # Too young, skip
                
            st = os.stat(path)
            # Compare size and ctime
            if str(st.st_size) == old_size and f"{st.st_ctime:.6f}" == old_ctime:
                stable_count += 1
        except (OSError, ValueError):
            pass
            
    return time.time() - start
