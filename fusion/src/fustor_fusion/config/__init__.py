# fusion/src/fustor_fusion/config/__init__.py
"""
Configuration loaders for YAML-based configuration files.

Configuration model:
- receivers-config.yaml: Transport endpoints and API keys
- views-config/*.yaml: View handler configurations
- fusion-pipes-config/*.yaml: Pipeline configurations (Receiver → View binding)
"""
from .views import ViewsConfigLoader, ViewConfig, views_config
from .receivers import ReceiversConfigLoader, ReceiverConfig, receivers_config
from .pipelines import FusionPipelinesConfigLoader, FusionPipelineConfig, fusion_pipelines_config
from .validators import validate_url_safe_id

import os
import logging
from fustor_core.common import get_fustor_home_dir

logger = logging.getLogger(__name__)

def check_deprecated_configs():
    """Check for legacy configuration files and warn user."""
    home = get_fustor_home_dir()
    deprecated_files = [
        "datastores-config.yaml",
        "pushers-config.yaml",
        "ingest-config.yaml"
    ]
    
    found = []
    for f in deprecated_files:
        if (home / f).exists():
            found.append(f)
            
    if found:
        # Using print to ensure it's visible even if logging isn't fully set up
        msg = "\n" + "!" * 80 + "\n"
        msg += " DEPRECATION WARNING: Legacy configuration files found in FUSTOR_HOME:\n"
        for f in found:
            msg += f"  - {f}\n"
        msg += "\n These files are NO LONGER USED in V2 architecture.\n"
        msg += " Please migrate to:\n"
        msg += "  - receivers-config.yaml (API keys)\n"
        msg += "  - views-config/*.yaml (Storage views)\n"
        msg += "  - fusion-pipes-config/*.yaml (Pipeline binding)\n"
        msg += "!" * 80 + "\n"
        print(msg)
        logger.warning(f"Legacy configs found: {found}. They are ignored in V2.")

# Run check on import
check_deprecated_configs()

__all__ = [
    # Receivers (API key management)
    "ReceiversConfigLoader",
    "ReceiverConfig",
    "receivers_config",
    # Pipelines
    "FusionPipelinesConfigLoader",
    "FusionPipelineConfig",
    "fusion_pipelines_config",
    # Views
    "ViewsConfigLoader",
    "ViewConfig",
    "views_config",
    # Validators
    "validate_url_safe_id",
]
