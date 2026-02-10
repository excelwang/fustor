import requests
import time
import os

FUSION_API = "http://localhost:8101/api/v1"
API_KEY = "key-1"
HEADERS = {"X-API-Key": API_KEY}

def test_metadata_limit():
    print("--- Testing Metadata Limit ---")
    
    # 1. Check current stats
    print("Checking current stats for shared-view...")
    resp = requests.get(f"{FUSION_API}/views/shared-view/stats", headers=HEADERS)
    if resp.status_code != 200:
        print(f"Failed to get stats: {resp.text}")
        return
    
    stats = resp.json()
    item_count = stats.get("item_count", 0)
    print(f"Current count: {item_count}")
    
    # 2. Try to set a strict limit in config
    # We will manually edit the config file for this test
    config_path = "/home/huajin/fustor-1/fustor_home/fusion-config/default.yaml"
    with open(config_path, 'r') as f:
        config_content = f.read()
    
    # Add a small limit for shared-view
    limit = item_count - 1 if item_count > 0 else 0
    if limit < 0: limit = 0
    
    # Parse and modify config using yaml to be robust
    import yaml
    
    try:
        config_data = yaml.safe_load(config_content)
        
        # Modify shared-view limit
        if 'views' in config_data and 'shared-view' in config_data['views']:
             view = config_data['views']['shared-view']
             if 'driver_params' not in view:
                 view['driver_params'] = {}
             # Update the limit
             view['driver_params']['max_tree_items'] = limit
             print(f"Setting max_tree_items to {limit} for shared-view via yaml parser...")
        else:
            print("Error: Could not find shared-view in config")
            return

        with open(config_path, 'w') as f:
            yaml.dump(config_data, f, default_flow_style=False)
        
        # Verify content on disk
        with open(config_path, 'r') as f:
            print(f"DEBUG: Config on disk:\n{f.read()}")
            
    except Exception as e:
        print(f"Error parsing/modifying config: {e}")
        return
    
    # Wait for file system sync
    time.sleep(2)
    
    try:
        # Give Fusion a moment to pick up config OR we can stop/start
        # Fusion reloads config on every API call in some places, 
        # but the router setup is at startup. However, my guard uses fusion_config.get_view
        # which should use the reloaded config if fusion_config.reload() is called.
        # My MetadataGuard calls fusion_config.get_view which calls ensure_loaded.
        # Let's try calling it immediately.
        
        print(f"Querying /tree for shared-view (expecting 400)...")
        resp = requests.get(f"{FUSION_API}/views/shared-view/tree", headers=HEADERS)
        
        print(f"Status Code: {resp.status_code}")
        if resp.status_code == 400:
            print("SUCCESS: Request rejected as expected!")
            print(f"Response: {resp.text}")
        else:
            print(f"FAILURE: Expected 400, got {resp.status_code}")
            print(f"Response: {resp.text}")
            
    finally:
        # Restore config
        with open(config_path, 'w') as f:
            f.write(config_content)
        print("Restored original configuration.")

if __name__ == "__main__":
    test_metadata_limit()
