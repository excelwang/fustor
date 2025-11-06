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

class RegisterServiceConfig(BaseModel):
    FUSTOR_REGISTER_DB_URL: str = os.getenv("FUSTOR_REGISTER_DB_URL", "sqlite+aiosqlite:///./fustor-register.db") # Default to sqlite for dev
    SECRET_KEY: str = os.getenv("SECRET_KEY", "super-secret-key") # Change this in production
    ALGORITHM: str = os.getenv("ALGORITHM", "HS256")
    ACCESS_TOKEN_EXPIRE_MINUTES: int = int(os.getenv("ACCESS_TOKEN_EXPIRE_MINUTES", 30))
    REFRESH_TOKEN_EXPIRE_DAYS: int = int(os.getenv("REFRESH_TOKEN_EXPIRE_DAYS", 7))

register_config = RegisterServiceConfig()