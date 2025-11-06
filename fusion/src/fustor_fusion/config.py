import os
from pathlib import Path
from pydantic import BaseModel
from dotenv import load_dotenv

# Determine the path to the .env file in the fustor root directory
fustor_root_dir = Path(__file__).resolve().parents[3]
dotenv_path = fustor_root_dir / ".env"

# Load environment variables from the .env file if it exists
if dotenv_path.is_file():
    load_dotenv(dotenv_path)

class IngestorServiceConfig(BaseModel):
    FUSTOR_REGISTER_SERVICE_URL: str = os.getenv("FUSTOR_REGISTER_SERVICE_URL", "http://127.0.0.1:8001")
    API_KEY_CACHE_SYNC_INTERVAL_SECONDS: int = int(os.getenv("API_KEY_CACHE_SYNC_INTERVAL_SECONDS", 60))
    # Add other ingestor specific configs here

ingestor_config = IngestorServiceConfig()
