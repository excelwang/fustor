# FastAPI Dynamic Router Lifecycle & Testing Patterns

## 1. Dynamic Router Registration Pitfall

### The Problem
In **FastAPI**, calling `app.include_router(router)` copies the route definitions **by value** at the moment of the call. It does not store a reference to the router object for future updates.

If your application populates routers dynamically (e.g., loading plugins, reading configuration files) during startup:
*   **Anti-Pattern**: Registering the router `create_app()` *before* it is populated.
    ```python
    # BAD: pipe_router is empty here!
    app.include_router(pipe_router) 
    
    # ... later in lifespan ...
    setup_pipe_routers() # Routes added here are IGNORED by the running app
    ```
    **Result**: APIs return `404 Not Found` because the app holds a copy of the empty router.

### The Solution
Register dynamic routers **LATE**, inside the `lifespan` handler or after the initialization logic is guaranteed to be complete.

```python
# GOOD: Register after populating
async def lifespan(app):
    await initialize_plugins()
    setup_dynamic_routers() # Populates routers
    
    # Registering here ensures app assumes the fully populated routes
    app.include_router(dynamic_router)
    yield
```

---

## 2. Defensive Integration Testing

### The Problem
When testing distributed systems (like Fustor), resources (nodes, files) may be transiently missing due to race conditions or instability. Using bare assertions on `None` values leads to confusing crashes (`AttributeError`) that hide the root cause.

### Best Practice
Always check for existence before asserting properties, and dump state on failure.

**Anti-Pattern**:
```python
node = client.get_node(path)
# CRASHES if node is None, giving no context
assert node['suspect'] == True 
```

**Robust Pattern**:
```python
node = client.get_node(path)
if node is None:
    # Print the tree state to see what IS there
    logger.error(f"Tree Dump: {client.get_tree()}")
    pytest.fail(f"Node {path} vanished unexpectedly")

assert node.get('suspect') == True
```
