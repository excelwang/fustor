import os
from pathlib import Path

def get_fustor_home_dir() -> Path:
    """
    Determines the FUSTOR home directory.
    Checks the FUSTOR_HOME environment variable first,
    then defaults to ~/.fustor.
    """
    fustor_home = os.getenv("FUSTOR_HOME")
    if fustor_home:
        return Path(fustor_home).expanduser().resolve()
    return Path.home() / ".fustor"