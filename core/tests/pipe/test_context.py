import pytest
from fustor_core.pipe.context import PipeContext

@pytest.mark.asyncio
async def test_pipe_context_registration():
    ctx = PipeContext(config={"global_key": "value"})
    
    assert ctx.get_config("global_key") == "value"
    assert ctx.get_config("missing", "default") == "default"
    
    pipe1 = object()
    pipe2 = object()
    
    await ctx.register_pipe("p1", pipe1)
    assert ctx.get_pipe("p1") is pipe1
    assert "p1" in ctx.list_pipes()
    
    # Test replacing
    await ctx.register_pipe("p1", pipe2)
    assert ctx.get_pipe("p1") is pipe2
    
    # Test unregister
    removed = await ctx.unregister_pipe("p1")
    assert removed is pipe2
    assert ctx.get_pipe("p1") is None
    
    # Unregister non-existent
    assert await ctx.unregister_pipe("missing") is None

def test_pipe_context_init_no_config():
    ctx = PipeContext()
    assert ctx.config == {}
