# fusion/src/fustor_fusion/runtime/__init__.py
"""
Runtime components for Fustor Fusion.

This module provides the Pipeline-based architecture for Fusion:

FusionPipeline Architecture:
============================

┌─────────────────────────────────────────────────────────────┐
│                    FusionPipeline                           │
│  (receives events from Agents)                              │
└────────────────────────┬────────────────────────────────────┘
                         │
                         ▼
┌─────────────────────────────────────────────────────────────┐
│                  ViewHandler Registry                       │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐      │
│  │  FSViewHandler│  │  DBViewHandler│  │     ...      │      │
│  └──────────────┘  └──────────────┘  └──────────────┘      │
└─────────────────────────────────────────────────────────────┘

Example Usage:
--------------

    from fustor_fusion.runtime import FusionPipeline

    # Create pipeline
    pipeline = FusionPipeline(
        pipeline_id="view-1",
        config={"view_id": 1},
        view_handlers=[fs_view_handler]
    )

    # Start processing
    await pipeline.start()

    # Process incoming events from Agent
    await pipeline.process_events(events, session_id="sess-123")

    # Query views
    tree = pipeline.get_view("fs", path="/")
"""

from .fusion_pipeline import FusionPipeline

from .view_handler_adapter import (
    ViewDriverAdapter,
    ViewManagerAdapter,
    create_view_handler_from_driver,
    create_view_handler_from_manager,
)

from .session_bridge import (
    PipelineSessionBridge,
    create_session_bridge,
)



__all__ = [
    # Pipeline
    "FusionPipeline",
    
    # View Handler Adapters
    "ViewDriverAdapter",
    "ViewManagerAdapter",
    "create_view_handler_from_driver",
    "create_view_handler_from_manager",
    
    # Session Bridge
    "PipelineSessionBridge",
    "create_session_bridge",
    

]
