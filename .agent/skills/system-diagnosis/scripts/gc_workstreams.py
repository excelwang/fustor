#!/usr/bin/env python3
import os
import shutil
import subprocess
import argparse
import sys

# Configuration
REPO_ROOT = os.getcwd()
WORKSTREAMS_DIR = os.path.join(REPO_ROOT, ".agent", "workstreams")
PROTECTED_ITEMS = {
    "reflection.json", 
    "wk-init" # specific protection for initialization workspace
}

def get_git_branches():
    """Returns a set of local git branch names."""
    try:
        result = subprocess.run(
            ["git", "branch", "--format=%(refname:short)"],
            cwd=REPO_ROOT,
            capture_output=True,
            text=True,
            check=True
        )
        return set(result.stdout.strip().splitlines())
    except subprocess.CalledProcessError as e:
        print(f"Error getting git branches: {e}")
        sys.exit(1)

def clean_workstreams(dry_run=True):
    """Scans and deletes orphaned workstream directories."""
    if not os.path.exists(WORKSTREAMS_DIR):
        print(f"Workstreams directory not found: {WORKSTREAMS_DIR}")
        return

    active_branches = get_git_branches()
    print(f"Active branches: {', '.join(active_branches)}")

    workstream_items = os.listdir(WORKSTREAMS_DIR)
    
    for item in workstream_items:
        item_path = os.path.join(WORKSTREAMS_DIR, item)
        
        # Skip protected items
        if item in PROTECTED_ITEMS:
            print(f"[SKIP] Protected item: {item}")
            continue
            
        # We only work on directories (workspaces)
        if not os.path.isdir(item_path):
            continue

        # Check if corresponding branch exists
        if item in active_branches:
            print(f"[KEEP] Branch exists: {item}")
        else:
            if dry_run:
                print(f"[DRY-RUN] Would delete orphaned workstream: {item}")
            else:
                try:
                    shutil.rmtree(item_path)
                    print(f"[DELETE] Removed orphaned workstream: {item}")
                except Exception as e:
                    print(f"[ERROR] Failed to remove {item}: {e}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Garbage collect orphaned workstream directories.")
    parser.add_argument("--apply", action="store_true", help="Actually perform the deletions (default is dry-run).")
    args = parser.parse_args()

    # If --apply is NOT present, it's a dry run
    is_dry_run = not args.apply
    
    print(f"Starting Workstream GC (Dry Run: {is_dry_run})...")
    clean_workstreams(dry_run=is_dry_run)
    print("Done.")
