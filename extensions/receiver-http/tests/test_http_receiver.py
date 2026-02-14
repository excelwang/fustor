import pytest
from unittest.mock import MagicMock
from fustor_receiver_http import HTTPReceiver

class TestHTTPReceiver:
    def test_credential_validation_with_view_id(self):
        """Test that register_api_key and validate_credential work with pipe_id."""
        receiver = HTTPReceiver(
            receiver_id="test-recv",
            port=8888,
            config={}
        )
        
        # Register using new parameter name
        receiver.register_api_key("secret-key", pipe_id="my-view-123")
        
        # Validate
        loop = asyncio.new_event_loop()
        try:
             result = loop.run_until_complete(receiver.validate_credential({"api_key": "secret-key"}))
             assert result == "my-view-123"
        finally:
            loop.close()

    def test_register_api_key_positional(self):
         """Test positional arguments work (backward capability for callers)."""
         receiver = HTTPReceiver("test", 8889)
         receiver.register_api_key("key-2", "view-456")
         
         loop = asyncio.new_event_loop()
         try:
             result = loop.run_until_complete(receiver.validate_credential({"api_key": "key-2"}))
             assert result == "view-456"
         finally:
             loop.close()

import asyncio
