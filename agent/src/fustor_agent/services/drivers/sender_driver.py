"""
Sender Driver Service.

Sender driver service for discovering and loading sender drivers.
The term "sender" aligns with the V2 architecture terminology.
"""
import logging
from importlib.metadata import entry_points
from typing import Any, Dict, Tuple, List

from fustor_core.exceptions import DriverError, ConfigError
from fustor_agent_sdk.interfaces import SenderDriverServiceInterface

logger = logging.getLogger("fustor_agent")


class SenderDriverService(SenderDriverServiceInterface):
    """
    A service for discovering and interacting with Sender driver classes.
    
    Senders are responsible for transmitting events from Agent to Fusion.
    Sender driver service.
    """
    
    def __init__(self):
        self._driver_cache: Dict[str, Any] = {}
        self._discovered_drivers = self._discover_installed_drivers()
        logger.info(f"Discovered installed sender drivers: {list(self._discovered_drivers.keys())}")

    def _discover_installed_drivers(self) -> Dict[str, Any]:
        """
        Scans for installed packages that register under the sender entry points.
        
        """
        discovered = {}
        
        # Try various entry point groups for compatibility
        groups = [
            "fustor_agent.senders",
            "fustor_agent.drivers.senders"
        ]
        for group in groups:
            try:
                eps = entry_points(group=group)
                for ep in eps:
                    if ep.name in discovered:
                        continue  # Skip if already discovered from senders group
                    try:
                        driver_class = ep.load()
                        discovered[ep.name] = driver_class
                        logger.debug(f"Loaded sender driver '{ep.name}' from {group}")
                    except Exception as e:
                        logger.error(f"Failed to load sender driver plugin '{ep.name}': {e}", exc_info=True)
            except Exception as e:
                logger.debug(f"No entry points found for group {group}: {e}")
        
        return discovered

    def _get_driver_by_type(self, driver_type: str) -> Any:
        """
        Loads a driver class by its name.
        """
        if not driver_type:
            raise ConfigError("Driver type cannot be empty.")

        if driver_type in self._driver_cache:
            return self._driver_cache[driver_type]

        if driver_type in self._discovered_drivers:
            driver_class = self._discovered_drivers[driver_type]
            self._driver_cache[driver_type] = driver_class
            return driver_class
        
        raise DriverError(
            f"Sender driver '{driver_type}' not found. "
            f"Available drivers: {list(self._discovered_drivers.keys())}"
        )



    async def test_connection(self, driver_type: str, **kwargs) -> Tuple[bool, str]:
        """Proxies the call to the driver's test_connection class method."""
        try:
            driver_class = self._get_driver_by_type(driver_type)
            return await driver_class.test_connection(**kwargs)
        except Exception as e:
            logger.error(f"Error during test_connection for driver '{driver_type}': {e}", exc_info=True)
            return (False, f"An exception occurred: {e}")

    async def check_privileges(self, driver_type: str, **kwargs) -> Tuple[bool, str]:
        """Proxies the call to the driver's check_privileges class method."""
        try:
            driver_class = self._get_driver_by_type(driver_type)
            return await driver_class.check_privileges(**kwargs)
        except Exception as e:
            logger.error(f"Error during check_privileges for driver '{driver_type}': {e}", exc_info=True)
            return (False, f"An exception occurred: {e}")

    async def get_needed_fields(self, driver_type: str, **kwargs) -> Dict[str, Any]:
        """Proxies the call to the driver's get_needed_fields class method."""
        try:
            driver_class = self._get_driver_by_type(driver_type)
            return await driver_class.get_needed_fields(**kwargs)
        except Exception as e:
            logger.error(f"Error during get_needed_fields for driver '{driver_type}': {e}", exc_info=True)
            raise RuntimeError(f"Could not retrieve needed fields from endpoint for driver '{driver_type}'.")

    def list_available_drivers(self) -> List[str]:
        """Lists the names of all discovered sender drivers."""
        return list(self._discovered_drivers.keys())


# SenderDriverService is now the primary name.
