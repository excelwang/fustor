from typing import Dict, Any, Optional
from datetime import datetime, timedelta, timezone
import uuid
import random

from fustor_core.models.config import SourceConfig, PasswdCredential # Used for type hinting config in mock
from fustor_core.event import EventType
from fustor_demo.store import demo_store

# --- Helper to generate common event structure ---
def _generate_base_event(
    project_id: str,
    source_type: str,
    item_type: str, # "file", "directory", "link", "metadata"
    name: str,
    path: str,
    size: Optional[int] = None,
    url: Optional[str] = None,
    extra_metadata: Optional[Dict[str, Any]] = None
) -> Dict[str, Any]:
    """Generates a base event dictionary for the demo store."""
    unique_id = f"{source_type}-{project_id}-{uuid.uuid4().hex[:8]}"
    return {
        "id": unique_id,
        "name": name,
        "type": item_type,
        "source_type": source_type,
        "project_id": project_id,
        "path": path,
        "size": size,
        "last_modified": datetime.now(timezone.utc).isoformat(),
        "url": url,
        "extra_metadata": extra_metadata or {}
    }

# --- Mock Agent Functions ---

def mock_mysql_create_project(project_name: str) -> Dict[str, Any]:
    """Simulates a MySQL Agent creating a new project."""
    project_id = project_name.lower().replace(" ", "_")
    event = _generate_base_event(
        project_id=project_id,
        source_type="mysql",
        item_type="directory",
        name=project_name,
        path=f"/{project_name}",
        extra_metadata={
            "description": f"Research project {project_name}.",
            "pi": f"Dr. {random.choice(['Smith', 'Jones', 'Li', 'Chen'])}",
            "created_by": "System"
        }
    )
    demo_store.add_event(event)
    return event

def mock_nfs_hot_add_file(project_id: str, filename: str, size_mb: int) -> Dict[str, Any]:
    """Simulates an NFS Hot Agent detecting a new file."""
    event = _generate_base_event(
        project_id=project_id,
        source_type="nfs_hot",
        item_type="file",
        name=filename,
        path=f"/{project_id}/hot_data/{filename}",
        size=size_mb * 1024 * 1024,
        extra_metadata={
            "local_path": f"/mnt/nfs_hot/{project_id}/{filename}",
            "last_access": datetime.now(timezone.utc).isoformat()
        }
    )
    demo_store.add_event(event)
    return event

def mock_nfs_cold_add_file(project_id: str, filename: str, size_gb: int, age_days: int = 370) -> Dict[str, Any]:
    """Simulates an NFS Cold Agent detecting an archived file."""
    archived_time = datetime.now(timezone.utc) - timedelta(days=age_days)
    event = _generate_base_event(
        project_id=project_id,
        source_type="nfs_cold",
        item_type="file",
        name=filename,
        path=f"/{project_id}/cold_archive/{filename}",
        size=size_gb * 1024 * 1024 * 1024,
        extra_metadata={
            "local_path": f"/mnt/nfs_cold_archive/{project_id}/{filename}",
            "archived_on": archived_time.isoformat(),
            "retrieval_status": "offline"
        }
    )
    demo_store.add_event(event)
    return event

def mock_oss_add_dataset_link(project_id: str, dataset_name: str, public_url: str) -> Dict[str, Any]:
    """Simulates an OSS Agent detecting a new public dataset link."""
    event = _generate_base_event(
        project_id=project_id,
        source_type="oss",
        item_type="link",
        name=dataset_name,
        path=f"/{project_id}/public_datasets/{dataset_name}",
        url=public_url,
        extra_metadata={
            "provider": "GSA/OMIX Public Data",
            "version": "1.0",
            "access_level": "public"
        }
    )
    demo_store.add_event(event)
    return event

def mock_es_add_publication(project_id: str, title: str, pubmed_id: str) -> Dict[str, Any]:
    """Simulates an Elasticsearch Agent finding a new publication metadata."""
    event = _generate_base_event(
        project_id=project_id,
        source_type="es",
        item_type="metadata",
        name=title,
        path=f"/{project_id}/publications/{pubmed_id}",
        url=f"https://pubmed.ncbi.nlm.nih.gov/{pubmed_id}/",
        extra_metadata={
            "pubmed_id": pubmed_id,
            "journal": "Science",
            "authors": ["J. Doe", "A. Smith"],
            "abstract_snippet": "This study investigates...",
            "publication_date": (datetime.now(timezone.utc) - timedelta(days=random.randint(10, 300))).isoformat()
        }
    )
    demo_store.add_event(event)
    return event
