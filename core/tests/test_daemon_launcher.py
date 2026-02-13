import pytest
import sys
import os
from unittest.mock import patch, MagicMock
from fustor_core.common.daemon_launcher import main

def test_daemon_launcher_main_success(tmp_path):
    # Mock sys.argv
    test_args = [
        "daemon_launcher.py",
        "mock_module",
        "mock_app",
        "test.pid",
        "test.log",
        "Test Service",
        "8000",
        "127.0.0.1"
    ]
    
    # We MUST patch both importlib.import_module and uvicorn.run
    # Use patch.dict(sys.modules) to fake the existence of the module
    mock_app = MagicMock()
    mock_module = MagicMock()
    mock_module.mock_app = mock_app
    
    with patch.dict(sys.modules, {"mock_module": mock_module}):
        with patch("uvicorn.run") as mock_uvicorn:
            with patch("fustor_core.common.daemon_launcher.get_fustor_home_dir", return_value=str(tmp_path)):
                with patch("fustor_core.common.daemon_launcher.setup_logging"):
                    with patch.object(sys, 'argv', test_args):
                        main()
                        mock_uvicorn.assert_called_once()
                        assert not os.path.exists(tmp_path / "test.pid")

def test_daemon_launcher_import_error(tmp_path):
    test_args = [
        "daemon_launcher.py",
        "nonexistent_module",
        "app",
        "test.pid",
        "test.log",
        "Test Service",
        "8000"
    ]
    # Patch import_module to raise error only when called with "nonexistent_module"
    def mock_import(name):
        if name == "nonexistent_module":
            raise ImportError("No module")
        # Return something for other modules (like logging, etc) if needed, 
        # but usually it's better to patch the specific call site if possible.
        return MagicMock()

    with patch("importlib.import_module", side_effect=mock_import):
        with patch("fustor_core.common.daemon_launcher.get_fustor_home_dir", return_value=str(tmp_path)):
            with patch("fustor_core.common.daemon_launcher.setup_logging"):
                with patch.object(sys, 'argv', test_args):
                    main()
                    assert not os.path.exists(tmp_path / "test.pid")
