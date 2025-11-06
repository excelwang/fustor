"""
API endpoints for the parsers module.
Provides REST endpoints to access parsed data views.
"""
from fastapi import APIRouter, Query, Header, Depends, status, HTTPException
import logging
from typing import Dict, Any, Optional

from ..parsers.manager import get_directory_tree, search_files, get_directory_stats, reset_directory_tree
from ..auth.dependencies import get_datastore_id_from_api_key

logger = logging.getLogger(__name__)

parser_router = APIRouter(tags=["Parsers - Data Views"])


@parser_router.get("/fs/tree", summary="Get directory tree structure")
async def get_directory_tree_api(
    path: str = Query("/", description="Directory path to retrieve (default: '/')"),
    datastore_id: int = Depends(get_datastore_id_from_api_key)
) -> Optional[Dict[str, Any]]:
    """Get the directory structure tree starting from the specified path."""
    logger.info(f"API request for directory tree: path={path}, datastore_id={datastore_id}")
    result = await get_directory_tree(path, datastore_id=datastore_id)
    logger.info(f"Directory tree result for path '{path}': {result}")
    return result

@parser_router.get("/fs/search", summary="Search for files by pattern")
async def search_files_api(
    pattern: str = Query(..., description="Search pattern to match in file paths"),
    datastore_id: int = Depends(get_datastore_id_from_api_key)
) -> list:
    """Search for files matching the specified pattern."""
    logger.info(f"API request for file search: pattern={pattern}, datastore_id={datastore_id}")
    result = await search_files(pattern, datastore_id=datastore_id)
    logger.info(f"File search result for pattern '{pattern}': found {len(result)} files")
    return result


@parser_router.get("/fs/stats", summary="Get statistics about the directory structure")
async def get_directory_stats_api(
    datastore_id: int = Depends(get_datastore_id_from_api_key)
) -> Dict[str, Any]:
    """Get statistics about the current directory structure."""
    logger.info(f"API request for directory stats: datastore_id={datastore_id}")
    result = await get_directory_stats(datastore_id=datastore_id)
    logger.info(f"Directory stats result: {result}")
    return result


@parser_router.delete("/fs/reset", 
    summary="Reset directory tree structure",
    description="Clear all directory entries for a specific datastore",
    status_code=status.HTTP_204_NO_CONTENT
)
async def reset_directory_tree_api(
    datastore_id: int = Depends(get_datastore_id_from_api_key)
) -> None:
    """
    Reset the directory tree structure by clearing all entries for a specific datastore.
    """
    logger.info(f"API request to reset directory tree for datastore {datastore_id}")
    success = await reset_directory_tree(datastore_id)
    
    if not success:
        logger.error(f"Failed to reset directory tree for datastore {datastore_id}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to reset directory tree"
        )
    logger.info(f"Successfully reset directory tree for datastore {datastore_id}")