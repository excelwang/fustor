from pathlib import Path

def get_static_dir() -> Path:
    """Return the absolute path to the static UI files."""
    return Path(__file__).parent / "static"
