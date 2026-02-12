# tests/e2e/utils/path_utils.py
import os
from ..fixtures.constants import MOUNT_POINT

def to_view_path(absolute_path: str) -> str:
    """
    Standardizes a container path to a Fusion View Path.
    Effectively ensures it starts with / and removes MOUNT_POINT prefix.
    """
    rel = os.path.relpath(absolute_path, MOUNT_POINT)
    if rel == ".":
        return "/"
    return "/" + rel.lstrip("/")
