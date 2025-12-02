from fastapi import FastAPI, Request, HTTPException, status
from fastapi.responses import HTMLResponse, FileResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from typing import List, Dict, Any, Optional
import os
import logging
import asyncio

from fustor_event_model.models import EventBase, EventType # For type hinting if needed

from fustor_demo.store import demo_store
from fustor_demo.mock_agents import (
    mock_mysql_create_project,
    mock_nfs_hot_add_file,
    mock_nfs_cold_add_file,
    mock_oss_add_dataset_link,
    mock_es_add_publication
)

logger = logging.getLogger("fustor_demo")

# Get the directory where main.py is located
current_dir = os.path.dirname(os.path.abspath(__file__))
static_dir = os.path.join(current_dir, "static")

# Ensure static directory exists
if not os.path.exists(static_dir):
    os.makedirs(static_dir)

app = FastAPI(
    title="Fustor Bio-Fusion Demo API",
    description="Demonstrates unified directory service from 5 heterogeneous data sources.",
    version="0.1.0",
)

# Mount static files to serve the frontend UI
app.mount("/static", StaticFiles(directory=static_dir), name="static")

@app.get("/", response_class=HTMLResponse, include_in_schema=False)
async def serve_demo_ui():
    """Serve the main HTML page for the demo UI."""
    html_file_path = os.path.join(static_dir, "index.html")
    if not os.path.exists(html_file_path):
        raise HTTPException(status_code=404, detail="Demo UI (index.html) not found. Please ensure it's in the static directory.")
    return FileResponse(html_file_path)

@app.get("/api/unified_directory", response_model=List[Dict[str, Any]])
async def get_unified_directory(
    project_id: Optional[str] = None
):
    """
    Query the unified directory service.
    Returns all projects if project_id is None or "ALL".
    Returns items within a specific project if project_id is provided.
    """
    if project_id and project_id.upper() == "ALL":
        return demo_store.get_unified_directory("ALL")
    return demo_store.get_unified_directory(project_id)

@app.post("/api/mock/mysql_project")
async def trigger_mysql_project(project_name: str = "Project_Alpha"):
    """Trigger a mock MySQL event to create a new project."""
    event = mock_mysql_create_project(project_name)
    return {"status": "success", "message": f"Mock MySQL project '{project_name}' created.", "event": event}

@app.post("/api/mock/nfs_hot_file")
async def trigger_nfs_hot_file(project_id: str = "project_alpha", filename: str = "sample_data.fastq", size_mb: int = 100):
    """Trigger a mock NFS Hot event to add a file."""
    event = mock_nfs_hot_add_file(project_id, filename, size_mb)
    return {"status": "success", "message": f"Mock NFS hot file '{filename}' added to '{project_id}'.", "event": event}

@app.post("/api/mock/nfs_cold_file")
async def trigger_nfs_cold_file(project_id: str = "project_alpha", filename: str = "archive_data.bam", size_gb: int = 50):
    """Trigger a mock NFS Cold event to add an archived file."""
    event = mock_nfs_cold_add_file(project_id, filename, size_gb)
    return {"status": "success", "message": f"Mock NFS cold file '{filename}' added to '{project_id}'.", "event": event}

@app.post("/api/mock/oss_dataset")
async def trigger_oss_dataset(project_id: str = "project_alpha", dataset_name: str = "gsa_public_dataset.zip", public_url: str = "https://oss.example.com/data.zip"):
    """Trigger a mock OSS event to add a public dataset link."""
    event = mock_oss_add_dataset_link(project_id, dataset_name, public_url)
    return {"status": "success", "message": f"Mock OSS dataset '{dataset_name}' added to '{project_id}'.", "event": event}

@app.post("/api/mock/es_publication")
async def trigger_es_publication(project_id: str = "project_alpha", title: str = "Novel Gene Discovery", pubmed_id: str = "38000000"):
    """Trigger a mock Elasticsearch event to add a publication metadata."""
    event = mock_es_add_publication(project_id, title, pubmed_id)
    return {"status": "success", "message": f"Mock ES publication '{title}' added to '{project_id}'.", "event": event}

@app.post("/api/clear_store")
async def clear_store():
    """Clears all data in the demo store."""
    demo_store.clear()
    return {"status": "success", "message": "Demo store cleared."}

# Main entry point for uvicorn
if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000, log_level="info")