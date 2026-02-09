import logging
import asyncio
from importlib.metadata import entry_points
from typing import Any, Dict, Tuple, List

from fustor_core.exceptions import DriverError, ConfigError
from fustor_agent_sdk.interfaces import SourceDriverServiceInterface # Import the interface

logger = logging.getLogger("fustor_agent")

class SourceDriverService(SourceDriverServiceInterface): # Inherit from the interface
    """
    A service for discovering and interacting with Source driver classes.
    This service only handles non-instance operations, like discovery and pre-flight checks.
    """
    def __init__(self):
        self._driver_cache: Dict[str, Any] = {}
        self._discovered_drivers = self._discover_installed_drivers()
        logger.info(f"Discovered installed source drivers: {list(self._discovered_drivers.keys())}")

    def _discover_installed_drivers(self) -> Dict[str, Any]:
        """
        Scans for installed packages that register under the source entry points.
        """
        discovered = {}
        # Try various entry point groups for compatibility
        groups = ["fustor_agent.sources", "fustor_agent.drivers.sources"]
        for group in groups:
            try:
                eps = entry_points(group=group)
                for ep in eps:
                    if ep.name in discovered:
                        continue
                    try:
                        discovered[ep.name] = ep.load()
                    except Exception as e:
                        logger.error(f"Failed to load source driver plugin '{ep.name}': {e}", exc_info=True)
            except Exception as e:
                logger.debug(f"No entry points found for group {group}: {e}")
        
        logger.debug(f"DEBUG: Discovered source drivers: {discovered}")
        return discovered

    def _get_driver_by_type(self, driver_type: str) -> Any:
        """
        Loads a driver class by its name.
        This method is intended for internal use by services like PipeInstanceService.
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
            f"Source driver '{driver_type}' not found. "
            f"It is not an installed plugin."
        )

    def list_available_drivers(self) -> List[str]:
        """Returns a list of all discovered driver names."""
        return list(self._discovered_drivers.keys())

    def driver_requires_schema(self, driver_type: str) -> bool:
        """Checks if a driver type requires formal schema discovery."""
        try:
            driver_class = self._get_driver_by_type(driver_type)
            # Default to True (inherited from ABC) if not explicitly set
            return getattr(driver_class, "require_schema_discovery", True)
        except Exception:
            return True

    def is_driver_transient(self, driver_type: str, config: Any = None) -> bool:
        """Checks if a driver type is transient."""
        try:
            driver_class = self._get_driver_by_type(driver_type)
            # If we have config, we can instantiate to check property
            if config:
                probe = driver_class(id="probe", config=config)
                return probe.is_transient
            
            # Fallback: check if the class itself has it (rare) or check property on class
            prop = getattr(driver_class, "is_transient", None)
            if isinstance(prop, property):
                # We really need an instance to check a property
                return False # Default to non-transient
            return bool(prop)
        except Exception:
            return False



    async def get_available_fields(self, driver_type: str, **kwargs) -> Dict[str, Any]:
        """Gets the available fields for a given source driver by calling the class method."""
        try:
            driver_class = self._get_driver_by_type(driver_type)
            return await driver_class.get_available_fields(**kwargs)
        except Exception as e:
            error_message = f"Failed to get available fields from source '{driver_type}'. Original error: {e}"
            logger.error(error_message, exc_info=True)
            raise DriverError(error_message) from e

    async def test_connection(self, driver_type: str, **kwargs) -> Tuple[bool, str]:
        """Tests the connection for a given driver type by calling the class method."""
        try:
            driver_class = self._get_driver_by_type(driver_type)
            return await driver_class.test_connection(**kwargs)
        except Exception as e:
            logger.error(f"Error during test_connection for driver '{driver_type}': {e}", exc_info=True)
            raise DriverError(f"An exception occurred during connection test: {e}")

    async def check_params(self, driver_type: str, **kwargs) -> Tuple[bool, str]:
        """Checks runtime parameters for a given driver type by calling the class method."""
        try:
            driver_class = self._get_driver_by_type(driver_type)
            return await driver_class.check_runtime_params(**kwargs)
        except Exception as e:
            logger.error(f"Error during check_params for driver '{driver_type}': {e}", exc_info=True)
            raise DriverError(f"An exception occurred during parameter check: {e}")

    async def create_agent_user(self, driver_type: str, **kwargs) -> Tuple[bool, str]:
        """Creates an agent user for a given driver type by calling the class method."""
        try:
            driver_class = self._get_driver_by_type(driver_type)
            return await driver_class.create_agent_user(**kwargs)
        except Exception as e:
            logger.error(f"Error during create_agent_user for driver '{driver_type}': {e}", exc_info=True)
            raise DriverError(f"An exception occurred during user creation: {e}")

    async def check_privileges(self, driver_type: str, **kwargs) -> Tuple[bool, str]:
        """Checks privileges for a given driver type by calling the class method."""
        try:
            driver_class = self._get_driver_by_type(driver_type)
            return await driver_class.check_privileges(**kwargs)
        except Exception as e:
            logger.error(f"Error during check_privileges for driver '{driver_type}': {e}", exc_info=True)
            raise DriverError(f"An exception occurred during privilege check: {e}")
