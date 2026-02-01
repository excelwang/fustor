# fusion/src/fustor_fusion/config/__init__.py
"""
Configuration loaders for YAML-based configuration files.

New configuration model (V2):
- receivers-config.yaml: Transport endpoints and API keys
- views-config/*.yaml: View handler configurations
- fusion-pipes-config/*.yaml: Pipeline configurations (planned)

Legacy configuration (deprecated):
- datastores-config.yaml: Combined API keys and settings (use receivers-config.yaml)
"""
from .datastores import DatastoresConfigLoader, DatastoreConfig, datastores_config
from .views import ViewsConfigLoader, ViewConfig, views_config
from .receivers import ReceiversConfigLoader, ReceiverConfig, receivers_config
from .validators import validate_url_safe_id

__all__ = [
    # New V2 configuration
    "ReceiversConfigLoader",
    "ReceiverConfig",
    "receivers_config",
    # Views
    "ViewsConfigLoader",
    "ViewConfig",
    "views_config",
    # Legacy (deprecated)
    "DatastoresConfigLoader",
    "DatastoreConfig", 
    "datastores_config",
    # Validators
    "validate_url_safe_id",
]
