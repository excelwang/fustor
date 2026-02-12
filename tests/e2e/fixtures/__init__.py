# tests/e2e/fixtures/__init__.py
"""
Modular fixtures for integration tests.

This package splits the monolithic conftest.py into focused modules:
- docker.py: Docker environment management
- fusion.py: Fusion client and configuration
- agents.py: Agent setup and configuration
- leadership.py: Leadership management and audit control
"""
import sys
from pathlib import Path

# Ensure parent directory is in path
_fixtures_dir = Path(__file__).parent
_it_dir = _fixtures_dir.parent
if str(_it_dir) not in sys.path:
    sys.path.insert(0, str(_it_dir))

from .docker import docker_env, clean_shared_dir
from .fusion import fusion_client, test_api_key, test_view
from .agents import setup_agents
from .leadership import reset_leadership, wait_for_audit

__all__ = [
    # Docker
    "docker_env",
    "clean_shared_dir",
    # Fusion
    "fusion_client", 
    "test_api_key",
    "test_view",
    # Agents
    "setup_agents",
    # Leadership
    "reset_leadership",
    "wait_for_audit",
]
