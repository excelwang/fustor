"""
Main parsers module that manages different types of parsers.
This module provides a unified interface for processing various event types
and building corresponding data views.
All data is stored in memory only.
"""
from typing import Dict, Any, Optional, Protocol
from .file_directory_parser import DirectoryStructureParser
import logging
import asyncio
from sqlalchemy.ext.asyncio import AsyncSession  # Keep import for compatibility with function signatures

logger = logging.getLogger(__name__)


# --- Global Cache for Parser Managers ---
_parser_manager_cache: Dict[int, 'ParserManager'] = {} # Keyed by datastore_id only
_cache_lock = asyncio.Lock()
# --------------------------------------


class EventParser(Protocol):
    """Protocol defining the interface for event parsers"""
    
    async def process_event(self, event: Dict[str, Any]) -> bool:
        """Process a single event and update the data view"""
        ...
    
    async def get_data_view(self, **kwargs) -> Any:
        """Get the current data view"""
        ...


class ParserManager:
    """
    Manages multiple parsers and routes events to appropriate parsers
    based on event type or content.
    """
    
    def __init__(self, datastore_id: int = None):
        self.parsers: Dict[str, EventParser] = {}
        self.logger = logging.getLogger(__name__)
        self.datastore_id = datastore_id
    
    async def initialize_parsers(self):
        """Initialize parsers with no database dependencies"""
        if self.datastore_id:
            self.logger.info(f"Initializing parser for datastore {self.datastore_id}")
            # Use default parser_id since there's only one parser type
            self.parsers["file_directory"] = DirectoryStructureParser(
                datastore_id=self.datastore_id
            )
            self.logger.info(f"Parser initialized for datastore {self.datastore_id}")
    
    async def process_event(self, event: Dict[str, Any]) -> Dict[str, bool]:
        """Process an event with all applicable parsers and return results"""
        results = {}
        
        self.logger.info(f"Processing event: {event}")
        for parser_name, parser in self.parsers.items():
            try:
                result = await parser.process_event(event)
                results[parser_name] = result
                self.logger.info(f"Processed event with {parser_name}, result: {result}")
            except Exception as e:
                self.logger.error(f"Error processing event with parser {parser_name}: {e}", exc_info=True)
                results[parser_name] = False
        
        return results
    
    async def get_file_directory_parser(self) -> Optional[EventParser]:
        """Get the file directory structure parser"""
        return self.parsers.get("file_directory")
    
    async def get_data_view(self, parser_name: str, **kwargs) -> Optional[Any]:
        """Get the data view from a specific parser"""
        parser = self.parsers.get(parser_name)
        if parser:
            return await parser.get_data_view(**kwargs)
        return None
    
    def get_available_parsers(self) -> list:
        """Get list of available parser names"""
        return list(self.parsers.keys())
    
    async def get_file_directory_parser(self) -> DirectoryStructureParser:
        """Get the file directory structure parser"""
        return self.parsers["file_directory"]  # type: ignore



async def get_cached_parser_manager(datastore_id: int) -> 'ParserManager':
    """
    Gets a cached ParserManager for a given datastore_id.
    If not in cache, it creates, initializes, and caches one.
    """
    cache_key = datastore_id
    # Fast path: check if already cached without locking
    if cache_key in _parser_manager_cache:
        logger.info(f"Returning cached parser manager for datastore {datastore_id}")
        return _parser_manager_cache[cache_key]

    # Slow path: lock and double-check
    async with _cache_lock:
        if cache_key in _parser_manager_cache:
            logger.info(f"Returning cached parser manager for datastore {datastore_id} (double-checked)")
            return _parser_manager_cache[cache_key]
        
        logger.info(f"Creating new parser manager for datastore {datastore_id} and caching it.")
        new_manager = ParserManager(datastore_id=datastore_id)
        await new_manager.initialize_parsers()
        _parser_manager_cache[cache_key] = new_manager
        return new_manager


# Interface for processing events
async def process_event(event: Dict[str, Any], datastore_id: int) -> Dict[str, bool]:
    """Process a single event with all available parsers"""
    logger.info(f"Processing single event in manager for datastore {datastore_id}")
    # Use the cached manager for processing events to keep the in-memory view consistent
    manager = await get_cached_parser_manager(datastore_id)
    return await manager.process_event(event)

# The process_events_batch function is no longer needed as events are processed individually by the polling loop.
# It is effectively replaced by the logic in per_datastore_processing_loop in main.py


async def get_directory_tree(path: str = "/", datastore_id: int = None) -> Optional[Dict[str, Any]]:
    """Get the directory tree from the file directory parser"""
    logger.info(f"Getting directory tree for path '{path}' in datastore {datastore_id}")
    if datastore_id:
        manager = await get_cached_parser_manager(datastore_id)
        parser = await manager.get_file_directory_parser()
        if parser:
            tree = await parser.get_directory_tree(path)
            logger.info(f"Directory tree for path '{path}': {tree}")
            return tree
    logger.info(f"Could not retrieve directory tree for path '{path}' in datastore {datastore_id}")
    return None

async def search_files(pattern: str, datastore_id: int = None) -> list:
    """Search for files using the file directory parser"""
    logger.info(f"Searching for files with pattern '{pattern}' in datastore {datastore_id}")
    if datastore_id:
        manager = await get_cached_parser_manager(datastore_id)
        parser = await manager.get_file_directory_parser()
        if parser:
            files = await parser.search_files(pattern)
            logger.info(f"Found {len(files)} files for pattern '{pattern}' in datastore {datastore_id}")
            return files
    logger.info(f"Could not search files for pattern '{pattern}' in datastore {datastore_id}")
    return []

async def get_directory_stats(datastore_id: int = None) -> Dict[str, Any]:
    """Get directory statistics using the file directory parser"""
    logger.info(f"Getting directory stats for datastore {datastore_id}")
    if datastore_id:
        manager = await get_cached_parser_manager(datastore_id)
        parser = await manager.get_file_directory_parser()
        if parser:
            stats = await parser.get_directory_stats()
            logger.info(f"Directory stats for datastore {datastore_id}: {stats}")
            return stats
    logger.info(f"Could not retrieve directory stats for datastore {datastore_id}")
    return {}


# Additional function to get a fresh parser instance
async def get_parser_with_db(datastore_id: int) -> ParserManager:
    """Get a parser instance with no database dependencies"""
    # This function will now also use the cache
    return await get_cached_parser_manager(datastore_id)


async def get_directory_from_db(datastore_id: int, path: str = "/") -> Optional[Dict[str, Any]]:
    """Get the directory structure from memory (previously from the database)"""
    logger.info(f"Getting directory from memory for path '{path}' in datastore {datastore_id}")
    manager = await get_cached_parser_manager(datastore_id)
    parser = await manager.get_file_directory_parser()
    if parser:
        result = await parser.get_data_view(path=path) if path else await parser.get_data_view()
        logger.info(f"Directory from memory for path '{path}' in datastore {datastore_id}: {result}")
        return result
    logger.info(f"Could not retrieve directory from memory for path '{path}' in datastore {datastore_id}")
    return None


async def reset_directory_tree(datastore_id: int) -> bool:
    """
    Reset the directory tree by clearing all entries for a specific datastore from memory only.
    """
    logger.info(f"Resetting directory tree for datastore {datastore_id}")
    try:
        # Clear the specific parser manager from the cache to effectively reset it
        async with _cache_lock:
            cache_key = datastore_id
            if cache_key in _parser_manager_cache:
                del _parser_manager_cache[cache_key]
                logger.info(f"Removed parser manager for datastore {datastore_id} from cache after reset.")

        logger.info(f"Reset directory tree for datastore {datastore_id}")
        return True
        
    except Exception as e:
        logger.error(f"Failed to reset directory tree for datastore {datastore_id}: {e}", exc_info=True)
        return False