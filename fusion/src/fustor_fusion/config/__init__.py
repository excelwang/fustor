# fusion/src/fustor_fusion/config/__init__.py
"""
Configuration loaders for YAML-based configuration files.

Also re-exports FusionServiceConfig for backward compatibility.
"""
import os
from pathlib import Path
from pydantic import BaseModel
from dotenv import load_dotenv
from fustor_common.paths import get_fustor_home_dir

# --- Backward compatibility: Load env and create fusion_config ---
# Standardize Fustor home directory across all services
home_fustor_dir = get_fustor_home_dir()
home_dotenv_path = home_fustor_dir / ".env"

# Load environment variables from the home directory .env file if it exists
if home_dotenv_path.is_file():
    load_dotenv(home_dotenv_path)

# Determine the path to the .env file in the fustor root directory
fustor_root_dir = Path(__file__).resolve().parents[4]
project_dotenv_path = fustor_root_dir / ".env"

# Load environment variables from the project root .env file if it exists
if project_dotenv_path.is_file():
    load_dotenv(project_dotenv_path)


class FusionServiceConfig(BaseModel):
    FUSTOR_FUSION_REGISTRY_URL: str = os.getenv("FUSTOR_FUSION_REGISTRY_URL", "http://127.0.0.1:8101")
    FUSTOR_REGISTRY_CLIENT_TOKEN: str = os.getenv("FUSTOR_REGISTRY_CLIENT_TOKEN", "")
    FUSTOR_FUSION_API_KEY_CACHE_SYNC_INTERVAL_SECONDS: int = int(os.getenv("FUSTOR_FUSION_API_KEY_CACHE_SYNC_INTERVAL_SECONDS", 60))
    FUSTOR_FUSION_SESSION_TIMEOUT_SECONDS: int = int(os.getenv("FUSTOR_FUSION_SESSION_TIMEOUT_SECONDS", 30))


fusion_config = FusionServiceConfig()

# --- New YAML config loaders ---
from .datastores import DatastoresConfigLoader, DatastoreConfig, datastores_config
from .views import ViewsConfigLoader, ViewConfig, views_config
from .validators import validate_url_safe_id

__all__ = [
    # Backward compatibility
    "FusionServiceConfig",
    "fusion_config",
    # New loaders
    "DatastoresConfigLoader",
    "DatastoreConfig", 
    "datastores_config",
    "ViewsConfigLoader",
    "ViewConfig",
    "views_config",
    "validate_url_safe_id",
]

