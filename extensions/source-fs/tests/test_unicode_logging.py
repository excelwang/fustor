"""
Test case to verify that the proxy character issue is resolved in logging
"""
import os
import tempfile
import logging
from io import StringIO
from unittest.mock import patch
import pytest
from fustor_core.models.config import PasswdCredential, SourceConfig
from fustor_source_fs import FSDriver


def test_safe_path_logging_with_surrogate_characters():
    """
    Test that paths with surrogate characters are handled safely in logging
    """
    with tempfile.TemporaryDirectory() as tmp_dir:
        # Create a directory with a problematic name containing potential surrogate chars
        # Since we can't directly create such paths on most systems, we'll test the function
        from fustor_source_fs.components import safe_path_handling
        
        # Test various problematic character sequences
        test_paths = [
            "/normal/path/without/issues",
            # Testing with surrogate pair that was in the error
            "/path/with/\udca3invalid\udca3chars",
            "/another/\udcfftest\udcfe",
        ]
        
        for test_path in test_paths:
            # This should not raise an exception
            safe_path = safe_path_handling(test_path)
            # The safe path should be encodable in UTF-8
            safe_path.encode('utf-8')


def test_pre_scan_logs_safe_paths():
    """
    Test that pre-scan phase logs paths safely without raising UnicodeEncodeError
    """
    with tempfile.TemporaryDirectory() as tmp_dir:
        # Create a more complex directory structure to trigger the logging
        nested_dir = os.path.join(tmp_dir, "nested")
        os.makedirs(nested_dir, exist_ok=True)
        
        # Create test files
        with open(os.path.join(nested_dir, "test.txt"), "w") as f:
            f.write("test content")
        
        fs_config = SourceConfig(
            driver="fs", 
            uri=tmp_dir, 
            credential=PasswdCredential(user="test")
        )
        
        driver = FSDriver('test-fs-id', fs_config)
        # This should not raise a UnicodeEncodeError when logging
        try:
            # Temporarily suppress actual logging output for this test
            import io
            log_capture_string = io.StringIO()
            ch = logging.StreamHandler(log_capture_string)
            logger = logging.getLogger("fustor_agent.driver.fs")
            logger.addHandler(ch)
            
            # Force the pre-scan to happen - this triggers the logging that was failing
            driver._perform_pre_scan_and_schedule()
            
            # Check that no UnicodeEncodeError was raised during the process
            assert driver._pre_scan_completed == True
        finally:
            logger = logging.getLogger("fustor_agent.driver.fs")
            logger.handlers.clear()