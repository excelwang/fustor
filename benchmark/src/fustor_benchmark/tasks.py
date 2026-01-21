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
