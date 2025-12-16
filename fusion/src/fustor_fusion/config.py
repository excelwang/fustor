import os
from pathlib import Path
from pydantic import BaseModel
from dotenv import load_dotenv

# Determine the path to the .env file in the user's home directory
home_fustor_dir = Path.home() / ".fustor"
home_dotenv_path = home_fustor_dir / ".env"

# Load environment variables from the home directory .env file if it exists
if home_dotenv_path.is_file():
    load_dotenv(home_dotenv_path)

# Determine the path to the .env file in the fustor root directory
fustor_root_dir = Path(__file__).resolve().parents[3]
project_dotenv_path = fustor_root_dir / ".env"

# Load environment variables from the project root .env file if it exists (will not override already loaded vars)
if project_dotenv_path.is_file():
    load_dotenv(project_dotenv_path)

class FusionServiceConfig(BaseModel):
    FUSTOR_FUSION_REGISTRY_URL: str = os.getenv("FUSTOR_FUSION_REGISTRY_URL", "http://127.0.0.1:8101")
    FUSTOR_REGISTRY_CLIENT_TOKEN: str = os.getenv("FUSTOR_REGISTRY_CLIENT_TOKEN", "")
    FUSTOR_FUSION_API_KEY_CACHE_SYNC_INTERVAL_SECONDS: int = int(os.getenv("FUSTOR_FUSION_API_KEY_CACHE_SYNC_INTERVAL_SECONDS", 60))
    # Add other ingestor specific configs here

fusion_config = FusionServiceConfig()
