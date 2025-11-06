import pytest
import os
import logging
from datetime import datetime
from unittest.mock import patch, mock_open

from fustor_agent.services.log import LogService, LOG_FILE_PATH, LOG_PATTERN
from fustor_core.models.log import LogEntry

# Mock the LOG_FILE_PATH for testing purposes
@pytest.fixture(autouse=True)
def mock_log_file_path():
    with patch('fustor_agent.services.log.LOG_FILE_PATH', '/tmp/test_fustor_agent.log'):
        yield

@pytest.fixture
def log_service():
    return LogService()

@pytest.fixture
def sample_log_content():
    return [
        "2025-07-08 10:00:00,123 - fustor_agent.app - INFO - Application started.",
        "2025-07-08 10:00:01,456 - fustor_agent.api - DEBUG - API call received.",
        "2025-07-08 10:00:02,789 - fustor_agent.service - WARNING - Something unusual happened.",
        "2025-07-08 10:00:03,000 - fustor_agent.driver - ERROR - Driver failed to connect.",
        "INVALID LOG LINE",
        "2025-07-08 10:00:04,111 - fustor_agent.app - INFO - Application shutting down."
    ]

class TestLogService:
    def test_parse_valid_line(self, log_service):
        line = "2025-07-08 10:00:00,123 - fustor_agent.app - INFO - Application started."
        entry = log_service._parse_line(line, 1)
        assert entry is not None
        assert entry.timestamp == datetime(2025, 7, 8, 10, 0, 0, 123000)
        assert entry.level == "INFO"
        assert entry.component == "fustor_agent.app"
        assert entry.message == "Application started."
        assert entry.line_number == 1

    def test_parse_invalid_line(self, log_service):
        line = "THIS IS NOT A VALID LOG LINE"
        entry = log_service._parse_line(line, 1)
        assert entry is None

    @patch('os.path.exists', return_value=True)
    @patch('builtins.open', new_callable=mock_open)
    def test_get_logs_basic(self, mock_file_open, mock_exists, log_service, sample_log_content):
        mock_file_open.return_value.__enter__.return_value.readlines.return_value = sample_log_content
        
        logs = log_service.get_logs(limit=3)
        assert len(logs) == 3
        assert logs[0].message == "Application shutting down."
        assert logs[1].message == "Driver failed to connect."
        assert logs[2].message == "Something unusual happened."

    @patch('os.path.exists', return_value=False)
    def test_get_logs_file_not_found(self, mock_exists, log_service):
        logs = log_service.get_logs()
        assert len(logs) == 0

    @patch('os.path.exists', return_value=True)
    @patch('builtins.open', new_callable=mock_open)
    def test_get_logs_with_level_filter(self, mock_file_open, mock_exists, log_service, sample_log_content):
        mock_file_open.return_value.__enter__.return_value.readlines.return_value = sample_log_content
        
        logs = log_service.get_logs(level="ERROR")
        assert len(logs) == 1
        assert logs[0].level == "ERROR"
        assert logs[0].message == "Driver failed to connect."

    @patch('os.path.exists', return_value=True)
    @patch('builtins.open', new_callable=mock_open)
    def test_get_logs_with_component_filter(self, mock_file_open, mock_exists, log_service, sample_log_content):
        mock_file_open.return_value.__enter__.return_value.readlines.return_value = sample_log_content
        
        logs = log_service.get_logs(component="fustor_agent.app")
        assert len(logs) == 2
        assert logs[0].component == "fustor_agent.app"
        assert logs[1].component == "fustor_agent.app"
        assert logs[0].message == "Application shutting down."
        assert logs[1].message == "Application started."

    @patch('os.path.exists', return_value=True)
    @patch('builtins.open', new_callable=mock_open)
    def test_get_logs_with_before_line(self, mock_file_open, mock_exists, log_service, sample_log_content):
        mock_file_open.return_value.__enter__.return_value.readlines.return_value = sample_log_content
        
        # Request logs before line 4 (which is the ERROR line)
        logs = log_service.get_logs(limit=10, before_line=4)
        assert len(logs) == 3
        assert logs[0].message == "Something unusual happened."
        assert logs[1].message == "API call received."
        assert logs[2].message == "Application started."

    @patch('os.path.exists', return_value=True)
    @patch('builtins.open', new_callable=mock_open)
    def test_get_logs_with_all_filters(self, mock_file_open, mock_exists, log_service, sample_log_content):
        mock_file_open.return_value.__enter__.return_value.readlines.return_value = sample_log_content
        
        logs = log_service.get_logs(limit=1, level="INFO", component="fustor_agent.app", before_line=6)
        assert len(logs) == 1
        assert logs[0].message == "Application started."
        assert logs[0].level == "INFO"
        assert logs[0].component == "fustor_agent.app"

    @patch('os.path.exists', return_value=True)
    @patch('builtins.open', side_effect=IOError("Permission denied"))
    def test_get_logs_io_error(self, mock_file_open, mock_exists, log_service):
        logs = log_service.get_logs()
        assert len(logs) == 0
        # Check if error was logged (optional, requires capturing logs)
        # with self.assertLogs('fustor_agent', level='ERROR') as cm:
        #     log_service.get_logs()
        #     self.assertIn("Failed to read or parse log file", cm.output[0])
