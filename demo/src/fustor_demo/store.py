from typing import Dict, Any, List, Optional
from datetime import datetime, timezone
import copy

class DemoStore:
    """
    A simple in-memory store for the demo.
    Aggregates events into a unified directory structure, mimicking Fusion's role.
    """
    def __init__(self):
        # Stores projects, indexed by project_id
        # Example: { "project_alpha": { "id": "project_alpha", "name": "Project Alpha", "files": [], "metadata": [] } }
        self._projects: Dict[str, Dict[str, Any]] = {}
        self._lock = {} # Placeholder for thread-safety in a real app, simplified for demo

    def add_event(self, event_data: Dict[str, Any]):
        """
        Adds an event to the store, aggregating it by project_id.
        Expected event_data structure:
        {
            "id": "unique_id_for_this_item",
            "name": "display_name",
            "type": "file" | "directory" | "link" | "metadata",
            "source_type": "mysql" | "nfs_hot" | "nfs_cold" | "oss" | "es",
            "project_id": "bio_project_id",
            "path": "/logical/path/to/item",
            "size": "1024" | None,
            "last_modified": "ISO_timestamp",
            "url": "http://download.link" | None,
            "extra_metadata": {}
        }
        """
        project_id = event_data.get("project_id")
        if not project_id:
            # For events without a project_id, put them under a "Unassigned" project
            project_id = "unassigned"
            event_data["project_id"] = project_id

        if project_id not in self._projects:
            # Determine project name: prefer explicit project_name if provided
            proj_name = event_data.get("project_name")
            if not proj_name:
                # Fallback to name if it's a project creation event (source_type=mysql)
                if event_data.get("source_type") == "mysql":
                    proj_name = event_data.get("name")
                else:
                    proj_name = project_id.replace("_", " ").title()

            self._projects[project_id] = {
                "id": project_id,
                "name": proj_name,
                "type": "directory",
                "source_type": "mysql" if project_id != "unassigned" else "system",
                "path": f"/{project_id}",
                "items": [],
                "last_modified": datetime.now(timezone.utc).isoformat()
            }
        
        # Add or update item in the project
        # Using item path as a logical unique identifier within a project if possible
        item_id = event_data.get("id")
        existing_items = self._projects[project_id]["items"]
        
        found = False
        for i, item in enumerate(existing_items):
            if item["id"] == item_id:
                # Update existing item
                existing_items[i] = copy.deepcopy(event_data)
                found = True
                break
        
        if not found:
            # Add new item
            existing_items.append(copy.deepcopy(event_data))
            
        # Sort items for consistent display: Projects first (if nested), then by path
        existing_items.sort(key=lambda x: (0 if x["type"] == "directory" else 1, x.get("path", x.get("name", ""))))
        
        # Update project last_modified time
        self._projects[project_id]["last_modified"] = datetime.now(timezone.utc).isoformat()


    def get_unified_directory(self, project_id: Optional[str] = None) -> List[Dict[str, Any]]:
        """
        Retrieves the unified directory structure.
        If project_id is "ALL", returns all projects and all items.
        If project_id is specified, returns items within that project.
        """
        if project_id == "ALL": 
            # Return everything: all projects and all nested items
            all_items = []
            for proj in self._projects.values():
                all_items.append(copy.deepcopy(proj))
                all_items.extend(copy.deepcopy(proj["items"]))
            return sorted(
                all_items,
                key=lambda x: (x.get("project_id", ""), x.get("path", x.get("name", "")))
            )
        elif project_id and project_id in self._projects:
            # Return items within a specific project
            return sorted(
                copy.deepcopy(self._projects[project_id]["items"]),
                key=lambda x: x.get("path", x.get("name", ""))
            )
        else:
            # Return all items if no project specified, or for invalid project_id
            all_items = []
            for proj in self._projects.values():
                all_items.append(copy.deepcopy(proj))
                all_items.extend(copy.deepcopy(proj["items"]))
            return sorted(
                all_items,
                key=lambda x: x.get("path", x.get("name", ""))
            )

    def clear(self):
        """Clears all stored data."""
        self._projects = {}

# Instantiate a global store for the demo server
demo_store = DemoStore()