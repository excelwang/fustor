import os
from pathlib import Path
from pydantic import BaseModel
from dotenv import load_dotenv
from fustor_common.paths import get_fustor_home_dir # NEW import

# Determine the path to the .env file in the user's home directory
HOME_FUSTOR_DIR = get_fustor_home_dir() # Use the common function
home_dotenv_path = HOME_FUSTOR_DIR / ".env" # Correctly derive home_dotenv_path

# Load environment variables from the home directory .env file if it exists
if home_dotenv_path.is_file():
    load_dotenv(home_dotenv_path)

# Determine the path to the .env file in the fustor root directory
fustor_root_dir = Path(__file__).resolve().parents[3]
project_dotenv_path = fustor_root_dir / ".env"

# Load environment variables from the project root .env file if it exists (will not override already loaded vars)
if project_dotenv_path.is_file():
    load_dotenv(project_dotenv_path)

class RegisterServiceConfig(BaseModel):
    FUSTOR_REGISTRY_DB_URL: str = os.getenv("FUSTOR_REGISTRY_DB_URL", f"sqlite+aiosqlite:///{HOME_FUSTOR_DIR / 'registry.db'}") # Default to sqlite in ~/.fustor
    FUSTOR_CORE_SECRET_KEY: str = os.getenv("FUSTOR_CORE_SECRET_KEY", "super-secret-key") # Change this in production
    FUSTOR_CORE_JWT_ALGORITHM: str = os.getenv("FUSTOR_CORE_JWT_ALGORITHM", "HS256")
    FUSTOR_CORE_JWT_ACCESS_TOKEN_EXPIRE_MINUTES: int = int(os.getenv("FUSTOR_CORE_JWT_ACCESS_TOKEN_EXPIRE_MINUTES", 30))
    FUSTOR_CORE_JWT_REFRESH_TOKEN_EXPIRE_DAYS: int = int(os.getenv("FUSTOR_CORE_JWT_REFRESH_TOKEN_EXPIRE_DAYS", 7))
    FUSTOR_REGISTRY_CLIENT_TOKEN: str = os.getenv("FUSTOR_REGISTRY_CLIENT_TOKEN", "") # API token for client services

register_config = RegisterServiceConfig()