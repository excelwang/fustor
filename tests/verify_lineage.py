import asyncio
from fustor_view_fs.nodes import DirectoryNode, FileNode
from fustor_view_fs.state import FSState
from fustor_view_fs.tree import TreeManager
from fustor_view_fs.arbitrator import FSArbitrator
from fustor_core.event import EventBase, EventType, MessageSource

async def test_lineage_persistence():
    # 1. Setup State and Components
    state = FSState(view_id="test_view", config={"limits": {"max_nodes": 100}})
    # Initialize root manually as TreeManager expects it or ensure_parent_chain needs it
    state.directory_path_map['/'] = DirectoryNode('root', '/')
    
    tree_manager = TreeManager(state)
    arbitrator = FSArbitrator(state, tree_manager, hot_file_threshold=5)
    
    # 2. Create Event with Metadata
    event = EventBase(
        event_type=EventType.INSERT,
        rows=[{
            'path': '/foo/bar.txt',
            'is_dir': False,
            'size': 1024,
            'modified_time': 1000.0,
            'created_time': 1000.0
        }],
        fields=['path', 'is_dir', 'size', 'modified_time', 'created_time'],
        event_schema='fs',
        table='files',
        message_source=MessageSource.REALTIME,
        metadata={
            "agent_id": "agent-007",
            "source_uri": "s3://bucket/data"
        }
    )
    
    # 3. Process Event
    await arbitrator.process_event(event)
    
    # 4. Verify Node Lineage
    node = state.get_node('/foo/bar.txt')
    assert node is not None
    assert isinstance(node, FileNode)
    assert node.last_agent_id == "agent-007"
    assert node.source_uri == "s3://bucket/data"
    
    # 5. Verify Parent Lineage (Auto-created parents usually don't get lineage from child event in current logic, 
    # but let's check if we want them to. Currently logic doesn't propagate to parents.)
    parent = state.get_node('/foo')
    assert parent is not None
    # Auto-created parent lineage check (optional based on implementation)
    # assert parent.last_agent_id is None 

    print("Lineage verification passed!")

if __name__ == "__main__":
    loop = asyncio.new_event_loop()
    loop.run_until_complete(test_lineage_persistence())
    loop.close()
