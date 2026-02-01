"""
Fustor Common - DEPRECATED

This package is deprecated. Please use fustor_core.common and fustor_core.clock instead.

All exports are re-exported from fustor_core for backward compatibility.
"""
import warnings

warnings.warn(
    "fustor_common is deprecated. Use fustor_core.common and fustor_core.clock instead.",
    DeprecationWarning,
    stacklevel=2
)

# Re-export from fustor_core for backward compatibility
from fustor_core.common import setup_logging, UvicornAccessFilter, get_fustor_home_dir, start_daemon
from fustor_core.clock import LogicalClock

__all__ = [
    "setup_logging",
    "UvicornAccessFilter",
    "get_fustor_home_dir",
    "start_daemon",
    "LogicalClock",
]
