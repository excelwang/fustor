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
    Simulates the 'Double Sampling' method to ensure file integrity.
    1. First find to get candidates.
    2. Sleep for a silence window (1s).
    3. Re-stat each file to confirm stability.
    """
    data_dir, subdir = args
    target = os.path.join(data_dir, subdir.lstrip('/'))
    
    start = time.time()
    
    # Phase 1: Initial find
    cmd = ["find", target, "-printf", "%p|%s|%T@\n"]
    result = subprocess.run(cmd, capture_output=True, text=True)
    lines = result.stdout.splitlines()
    
    initial_metadata = {}
    for line in lines:
        parts = line.split('|')
        if len(parts) == 3:
            initial_metadata[parts[0]] = (parts[1], parts[2])
            
    # Phase 2: Silence Window (Crucial for the 'Double Sampling' logic)
    time.sleep(1.0)
    
    # Phase 3: Secondary Validation (The O(N) overhead point)
    stable_count = 0
    for path, (old_size, old_mtime) in initial_metadata.items():
        try:
            st = os.stat(path)
            # In a real scenario, we compare: 
            # if str(st.st_size) == old_size and str(st.st_mtime) == old_mtime:
            stable_count += 1
        except OSError:
            pass
            
    return time.time() - start
