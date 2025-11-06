import logging
from importlib.metadata import entry_points
from typing import Any, Dict, Tuple, List

from fustor_core.exceptions import DriverError, ConfigError

logger = logging.getLogger("fustor_agent")

class PusherDriverService:
    """
    A service for discovering and interacting with Pusher driver classes.
    This service only handles non-instance operations, like discovery and pre-flight checks.
    """
    def __init__(self):
        self._driver_cache: Dict[str, Any] = {}
        self._discovered_drivers = self._discover_installed_drivers()
        logger.info(f"Discovered installed pusher drivers: {list(self._discovered_drivers.keys())}")

    def _discover_installed_drivers(self) -> Dict[str, Any]:
        """
        Scans for installed packages that register under the 'fustor_agent.drivers.pushers'
        entry point and loads their driver class.
        """
        discovered = {}
        try:
            eps = entry_points(group="fustor_agent.drivers.pushers")
            for ep in eps:
                try:
                    # The entry point should point to the driver class
                    driver_class = ep.load()
                    # We store the class itself
                    discovered[ep.name] = driver_class
                except Exception as e:
                    logger.error(f"Failed to load pusher driver plugin '{ep.name}': {e}", exc_info=True)
        except Exception as e:
            logger.error(f"Error while discovering entry points: {e}", exc_info=True)
        return discovered

    def _get_driver_by_type(self, driver_type: str) -> Any:
        """
        Loads a driver class by its name.
        This method is intended for internal use by services like SyncInstanceService.
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
            f"Pusher driver '{driver_type}' not found. "
            f"It is not an installed plugin."
        )

    async def get_wizard_definition_by_type(self, driver_type: str) -> Dict[str, Any]:
        """Proxies the call to the driver's get_wizard_steps class method."""
        try:
            driver_class = self._get_driver_by_type(driver_type)
            # Directly call the method, relying on the ABC for a default implementation.
            return await driver_class.get_wizard_steps()
        except (ConfigError, DriverError) as e:
            raise e
        except Exception as e:
            logger.error(f"Unexpected error getting wizard definition for driver '{driver_type}': {e}", exc_info=True)
            raise DriverError(f"Could not retrieve wizard definition for driver '{driver_type}'.")

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
        """Lists the names of all discovered pusher drivers."""
        return list(self._discovered_drivers.keys())

