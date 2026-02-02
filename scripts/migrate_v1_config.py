#!/usr/bin/env python3
"""
Migration script to convert Fustor V1 configuration (datastores-config.yaml) 
to V2 format (receivers-config.yaml + views-config/*.yaml).

Usage:
    python scripts/migrate_v1_config.py [FUSTOR_HOME]

"""
import os
import sys
import yaml
import logging
from pathlib import Path

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s')
logger = logging.getLogger(__name__)

def migrate_config(fustor_home: Path):
    legacy_file = fustor_home / "datastores-config.yaml"
    
    if not legacy_file.exists():
        logger.error(f"No datastores-config.yaml found at {legacy_file}")
        return

    logger.info(f"Reading legacy config: {legacy_file}")
    with open(legacy_file, 'r') as f:
        data = yaml.safe_load(f)

    datastores = data.get('datastores', {})
    if not datastores:
        logger.warning("No datastores found in config.")
        return

    # Prepare V2 structures
    receivers_config = {
        "receivers": {
            "default-http": {
                "driver": "http",
                "port": 8000,
                "api_keys": {}
            }
        }
    }
    
    views_dir = fustor_home / "views-config"
    views_dir.mkdir(exist_ok=True)
    
    api_keys_map = receivers_config["receivers"]["default-http"]["api_keys"]

    for name, config in datastores.items():
        logger.info(f"Processing datastore: {name}")
        
        # 1. Access Control (Receiver)
        api_key = config.get('api_key')
        if api_key:
            if api_key not in api_keys_map:
                api_keys_map[api_key] = {
                    "role": "admin", # Default role
                    "view_mappings": []
                }
            api_keys_map[api_key]["view_mappings"].append(name)
        
        # 2. View Definition
        # In V1, the storage path wasn't explicit in datastores-config, 
        # usually it was implied or in another file. We will create a default FS view.
        view_file = views_dir / f"{name}.yaml"
        if view_file.exists():
            logger.warning(f"View file {view_file} already exists, skipping overwrite.")
        else:
            view_content = {
                "view_id": name,
                "driver": "fs",
                "config": {
                    "root_path": f"/var/lib/fustor/views/{name}", # Default guess
                    "create_dirs": True
                }
            }
            with open(view_file, 'w') as vf:
                yaml.dump(view_content, vf, sort_keys=False)
            logger.info(f"Created view config: {view_file}")

    # Write receivers-config.yaml
    receivers_file = fustor_home / "receivers-config.yaml"
    if receivers_file.exists():
         logger.warning(f"{receivers_file} already exists. Merging is not supported by this script.")
         receivers_file = fustor_home / "receivers-config.yaml.new"
    
    with open(receivers_file, 'w') as rf:
        yaml.dump(receivers_config, rf, sort_keys=False)
    
    logger.info(f"Generated receivers config: {receivers_file}")
    logger.info("Migration Preview Complete. Please review generated files and paths.")

if __name__ == "__main__":
    home_dir = Path(sys.argv[1]) if len(sys.argv) > 1 else Path(os.getcwd())
    migrate_config(home_dir)
