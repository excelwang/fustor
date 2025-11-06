import asyncio
import logging
from functools import wraps
from fustor_core.exceptions import DriverError

logger = logging.getLogger(__name__)

def retry(max_retries_attr: str, delay_sec_attr: str, exceptions: tuple = (DriverError,)):
    def decorator(func):
        @wraps(func)
        async def wrapper(self, *args, **kwargs):
            max_retries = getattr(self.config, max_retries_attr)
            delay_sec = getattr(self.config, delay_sec_attr)
            
            retries = 0
            while True:
                try:
                    return await func(self, *args, **kwargs)
                except asyncio.CancelledError:
                    # Handle cancellation that occurs during function execution
                    logger.info(f"Function {func.__name__} was cancelled during execution.")
                    raise
                except exceptions as e:
                    retries += 1
                    if retries >= max_retries:
                        logger.error(f"Function {func.__name__} failed after {max_retries} retries.")
                        raise
                    logger.warning(f"Function {func.__name__} failed. Retrying in {delay_sec:.2f} seconds... ({retries}/{max_retries})")
                    try:
                        await asyncio.sleep(delay_sec)
                    except asyncio.CancelledError:
                        # Handle cancellation that occurs during the sleep/delay period
                        logger.info(f"Retry loop for {func.__name__} was cancelled during sleep.")
                        raise
        return wrapper
    return decorator
