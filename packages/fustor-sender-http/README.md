# fustor-sender-http

HTTP Sender for Fustor Agent - implements the transport layer for Agent to Fusion communication.

## Overview

This package provides an HTTP-based implementation of the `Sender` transport abstraction. It uses the Fusion SDK client to communicate with Fusion's REST API.

## Installation

```bash
pip install fustor-sender-http
```

## Usage

```python
from fustor_sender_http import HTTPSender

sender = HTTPSender(
    sender_id="my-sender",
    endpoint="http://fusion.example.com:8000",
    credential={"api_key": "your-api-key"}
)

# Create session
await sender.connect()
session = await sender.create_session("my-task-id")

# Send events
await sender.send_events(events, source_type="message")

# Heartbeat
await sender.heartbeat()

# Cleanup
await sender.close()
```

## Entry Points

This package registers itself as:
- `fustor.senders:http` - New sender registry
- `fustor_agent.drivers.senders:fusion` - Legacy sender registry (backward compat)

## Migration from fustor-sender-fusion

The `fustor-sender-fusion` package is deprecated. To migrate:

1. Replace `from fustor_sender_fusion import FusionDriver` with `from fustor_sender_http import HTTPSender`
2. Update configuration to use `sender` instead of `sender` terminology
3. The `HTTPSender` class implements the new `Sender` interface but maintains API compatibility
