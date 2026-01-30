import yaml
import logging
import os
from pathlib import Path
from typing import Dict, Any, Optional
from fustor_common.paths import get_fustor_home_dir

logger = logging.getLogger(__name__)

class FusionLocalConfig:
    def __init__(self, config_path: str = None):
        # Default to FUSTOR_HOME/fusion-config.yaml
        if not config_path:
            config_path = str(get_fustor_home_dir() / "fusion-config.yaml")

        # Allow override via environment variable
        env_path = os.getenv("FUSTOR_FUSION_CONFIG_PATH")
        self.config_path = Path(env_path) if env_path else Path(config_path)
        self.config: Dict[str, Any] = {}
        self.load()

    def load(self):
        if not self.config_path.exists():
            logger.debug(f"Fusion config file not found at {self.config_path}. Using manual discovery defaults.")
            return

        try:
            with open(self.config_path, "r") as f:
                self.config = yaml.safe_load(f) or {}
            logger.info(f"Loaded fusion config from {self.config_path}")
        except Exception as e:
            logger.error(f"Failed to load fusion config from {self.config_path}: {e}")

    def get_datastore_views(self, datastore_id: int) -> Dict[str, Any]:
        """
        Get view configurations for a specific datastore.
        Returns a dict of view_instance_name -> config
        """
        if not self.config:
            return {}

        views_config = self.config.get("views", {})
        result = {}

        target_ds_id = str(datastore_id)
        
        for view_name, cfg in views_config.items():
            # Skip disabled
            if cfg.get("disabled", False):
                continue
                
            # Check datastore_id match
            # Config might use int or str for ID
            cfg_ds_id = str(cfg.get("datastore_id", ""))
            
            if cfg_ds_id == target_ds_id:
                result[view_name] = cfg
                
        return result

    def is_configured(self) -> bool:
        """Returns True if a valid configuration file was loaded."""
        return bool(self.config)

local_config = FusionLocalConfig()

