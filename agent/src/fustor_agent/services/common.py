import asyncio

# A shared lock to ensure that configuration changes are atomic
# and prevent race conditions when multiple API calls modify the config.
config_lock = asyncio.Lock()