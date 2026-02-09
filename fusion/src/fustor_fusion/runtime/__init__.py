# fusion/src/fustor_fusion/runtime/__init__.py
"""
Runtime components for Fustor Fusion.

This module provides the Pipe-based architecture for Fusion:

FusionPipe Architecture:
============================

┌─────────────────────────────────────────────────────────────┐
│                    FusionPipe                           │
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

    from fustor_fusion.runtime import FusionPipe

    # Create pipe
    pipe = FusionPipe(
        pipe_id="view-1",
        config={"view_id": 1},
        view_handlers=[fs_view_handler]
    )

    # Start processing
    await pipe.start()

    # Process incoming events from Agent
    await pipe.process_events(events, session_id="sess-123")

    # Query views
    tree = pipe.get_view("fs", path="/")
"""

from .fusion_pipe import FusionPipe

from .view_handler_adapter import (
    ViewDriverAdapter,
    ViewManagerAdapter,
    create_view_handler_from_driver,
    create_view_handler_from_manager,
)

from .session_bridge import (
    PipeSessionBridge,
    create_session_bridge,
)



__all__ = [
    # Pipe
    "FusionPipe",
    
    # View Handler Adapters
    "ViewDriverAdapter",
    "ViewManagerAdapter",
    "create_view_handler_from_driver",
    "create_view_handler_from_manager",
    
    # Session Bridge
    "PipeSessionBridge",
    "create_session_bridge",
    

]
