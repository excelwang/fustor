# src/fustor_agent/services/schema_cache.py

import json
import os
import logging
from typing import Dict, Any, Optional

from .. import CONFIG_DIR

logger = logging.getLogger("fustor_agent")
SCHEMA_CACHE_DIR = os.path.join(CONFIG_DIR, "schemas")

def _get_source_schema_path(source_id: str) -> str:
    """Constructs the file path for a given source ID's schema cache."""
    return os.path.join(SCHEMA_CACHE_DIR, f"source_{source_id}.schema.json")

def _get_validation_marker_path(source_id: str) -> str:
    """Constructs the file path for a given source ID's validation marker."""
    return os.path.join(SCHEMA_CACHE_DIR, f"source_{source_id}.valid")

def save_source_schema(source_id: str, schema_data: Dict[str, Any]):
    """Saves the discovered schema for a source to a JSON file."""
    os.makedirs(SCHEMA_CACHE_DIR, exist_ok=True)
    
    file_path = _get_source_schema_path(source_id)
    try:
        with open(file_path, 'w', encoding='utf-8') as f:
            json.dump(schema_data, f, indent=2)
        logger.info(f"Successfully cached schema for source '{source_id}' at {file_path}")
    except IOError as e:
        logger.error(f"Failed to write schema cache file for source '{source_id}': {e}")
        raise

def load_source_schema(source_id: str) -> Optional[Dict[str, Any]]:
    """Loads the cached schema for a source from its JSON file."""
    file_path = _get_source_schema_path(source_id)
    if not os.path.exists(file_path):
        logger.warning(f"Schema cache file for source '{source_id}' not found at {file_path}")
        return None
    
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            return json.load(f)
    except (IOError, json.JSONDecodeError) as e:
        logger.error(f"Failed to read or parse schema cache file for source '{source_id}': {e}")
        return None

def is_schema_valid(source_id: str) -> bool:
    """
    Checks if the schema for a given source is present and validated.
    Validation is determined by the existence of the .valid marker file.
    """
    schema_path = _get_source_schema_path(source_id)
    marker_path = _get_validation_marker_path(source_id)
    return os.path.exists(schema_path) and os.path.exists(marker_path)

def validate_schema(source_id: str):
    """Creates a validation marker file to indicate the schema is valid."""
    os.makedirs(SCHEMA_CACHE_DIR, exist_ok=True)
    marker_path = _get_validation_marker_path(source_id)
    try:
        # Create an empty file
        with open(marker_path, 'w') as f:
            pass
        logger.info(f"Schema for source '{source_id}' marked as valid.")
    except IOError as e:
        logger.error(f"Failed to create validation marker for source '{source_id}': {e}")
        raise

def invalidate_schema(source_id: str):
    """
    Removes the validation marker file to invalidate the schema.
    This is done before a new discovery process starts.
    """
    marker_path = _get_validation_marker_path(source_id)
    if os.path.exists(marker_path):
        try:
            os.remove(marker_path)
            logger.info(f"Invalidated schema for source '{source_id}'.")
        except OSError as e:
            logger.error(f"Error removing validation marker for source '{source_id}': {e}")
            raise

def delete_schema(source_id: str):
    """Deletes all cache files (schema and marker) for a given source ID."""
    schema_path = _get_source_schema_path(source_id)
    marker_path = _get_validation_marker_path(source_id)
    
    for path in [schema_path, marker_path]:
        if os.path.exists(path):
            try:
                os.remove(path)
                logger.info(f"Removed cache file: {path}")
            except OSError as e:
                logger.error(f"Error removing cache file {path}: {e}")
                # Continue to try to remove the other file
